import os
import sys
import unittest
from datetime import datetime, timezone
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
            "domain": "fg",
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
            ingest.upsert_cities(cur, [city])
            ingest.upsert_places(cur, 21, [place])
            cur.execute("SELECT name FROM country WHERE domain = %s", ("fg",))
            self.assertEqual(cur.fetchone()[0], "KVV.nextbike")
            cur.execute("SELECT name FROM city WHERE city_uid = %s", (21,))
            self.assertEqual(cur.fetchone()[0], "Karlsruhe")
            cur.execute("SELECT name FROM place WHERE place_uid = %s", (458,))
            self.assertEqual(cur.fetchone()[0], "Erbprinzenstr.")
        self.conn.commit()


if __name__ == "__main__":
    unittest.main()
