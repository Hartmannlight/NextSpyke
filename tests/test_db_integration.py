import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import config, db, ingest


class EnvGuard:
    def __init__(self, **updates) -> None:
        self._updates = updates
        self._original = {}

    def __enter__(self):
        for key, value in self._updates.items():
            self._original[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return self

    def __exit__(self, exc_type, exc, tb):
        for key, value in self._original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return False


class TestDbIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not config.env_bool("RUN_DB_TESTS", False):
            raise unittest.SkipTest("Set RUN_DB_TESTS=true to run DB integration tests.")
        try:
            cls.conn = psycopg.connect(db.build_dsn(), connect_timeout=3)
        except Exception as exc:
            raise unittest.SkipTest(f"Database not reachable: {exc}") from exc
        schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
        with EnvGuard(SCHEMA_PATH=str(schema_path)):
            try:
                db.init_db(cls.conn)
            except Exception as exc:
                raise unittest.SkipTest(f"Schema init failed: {exc}") from exc

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "conn"):
            cls.conn.close()

    def test_schema_and_snapshot_insert(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO snapshot (fetched_at, domain, source, raw_json)
                VALUES (%s, %s, %s, %s)
                RETURNING snapshot_id
                """,
                (datetime.now(timezone.utc), "fg", "nextbike-live", None),
            )
            snapshot_id = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM snapshot WHERE snapshot_id = %s", (snapshot_id,))
            count = cur.fetchone()[0]
        self.conn.commit()
        self.assertEqual(count, 1)

    def test_upserts_basic(self):
        country = {
            "domain": "fg",
            "name": "KVV.nextbike",
            "country": "DE",
            "country_name": "Germany",
            "timezone": "Europe/Berlin",
        }
        city = {
            "uid": 21,
            "name": "Karlsruhe",
            "alias": "karlsruhe",
            "lat": 49.0,
            "lng": 8.4,
            "zoom": 12,
            "refresh_rate": "10000",
        }
        place = {
            "uid": 458,
            "name": "Erbprinzenstr.",
            "number": 5401,
            "spot": True,
            "terminal_type": "stele",
            "lat": 49.007499,
            "lng": 8.401762,
            "maintenance": True,
        }
        with self.conn.cursor() as cur:
            ingest.upsert_country(cur, country)
            ingest.upsert_cities(cur, "fg", [city])
            ingest.upsert_places(cur, 21, [place])
            cur.execute("SELECT name FROM country WHERE domain = %s", ("fg",))
            self.assertEqual(cur.fetchone()[0], "KVV.nextbike")
            cur.execute("SELECT name FROM city WHERE city_uid = %s", (21,))
            self.assertEqual(cur.fetchone()[0], "Karlsruhe")
            cur.execute("SELECT domain FROM city WHERE city_uid = %s", (21,))
            self.assertEqual(cur.fetchone()[0], "fg")
            cur.execute("SELECT name FROM place WHERE place_uid = %s", (458,))
            self.assertEqual(cur.fetchone()[0], "Erbprinzenstr.")
        self.conn.commit()

    def test_free_bike_coordinate_change_creates_movement(self):
        fetched_at = datetime.now(timezone.utc)
        next_fetched_at = fetched_at + timedelta(seconds=60)
        city_uid = -910001
        place_uid = -910001
        bike_number = "integration-free-bike"
        country = {"domain": "test-tracking", "name": "Tracking test"}
        city = {
            "uid": city_uid,
            "name": "Tracking city",
            "lat": 49.0,
            "lng": 8.4,
        }
        start_place = {
            "uid": place_uid,
            "name": f"BIKE {bike_number}",
            "spot": False,
            "bike": True,
            "lat": 49.0,
            "lng": 8.4,
        }
        end_place = {**start_place, "lat": 49.001, "lng": 8.4}
        try:
            with self.conn.cursor() as cur:
                db.ensure_partitions(cur, fetched_at)
                ingest.upsert_country(cur, country)
                ingest.upsert_cities(cur, country["domain"], [city])
                ingest.upsert_bikes(
                    cur,
                    [(bike_number, None, None, True, ["frame_lock"], fetched_at, fetched_at)],
                )
                first_snapshot_id = ingest.insert_snapshot(
                    cur,
                    fetched_at,
                    country["domain"],
                    None,
                )
                ingest.insert_bike_status(
                    cur,
                    first_snapshot_id,
                    [
                        (
                            first_snapshot_id,
                            fetched_at,
                            bike_number,
                            None,
                            True,
                            "ok",
                            None,
                            None,
                            None,
                            start_place["lng"],
                            start_place["lat"],
                        )
                    ],
                )
                ingest.update_bike_last_status(cur, first_snapshot_id, fetched_at)
                second_snapshot_id = ingest.insert_snapshot(
                    cur,
                    next_fetched_at,
                    country["domain"],
                    None,
                )
                ingest.insert_bike_status(
                    cur,
                    second_snapshot_id,
                    [
                        (
                            second_snapshot_id,
                            next_fetched_at,
                            bike_number,
                            None,
                            True,
                            "ok",
                            None,
                            None,
                            None,
                            end_place["lng"],
                            end_place["lat"],
                        )
                    ],
                )

                inserted = ingest.insert_bike_movements(
                    cur,
                    second_snapshot_id,
                    next_fetched_at,
                    10,
                )
                cur.execute(
                    """
                    SELECT movement_reason, distance_m
                    FROM bike_movement
                    WHERE bike_number = %s AND end_snapshot_id = %s
                    """,
                    (bike_number, second_snapshot_id),
                )
                movement = cur.fetchone()

            self.assertEqual(inserted, 1)
            self.assertEqual(movement[0], "coordinate_change")
            self.assertGreaterEqual(movement[1], 100)
        finally:
            self.conn.rollback()


if __name__ == "__main__":
    unittest.main()
