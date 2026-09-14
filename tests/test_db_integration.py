import json
import os
import re
import sys
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import config, db, ingest
from scripts.compact_history import compact_day


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
            raise RuntimeError(f"Requested integration database not reachable: {exc}") from exc
        # A separate schema prevents repeated suites from leaving default-partition
        # fixtures behind and keeps integration data apart from existing tables.
        cls.test_schema = "test_nextspyke_" + uuid4().hex
        cls.conn.execute("CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public")
        cls.conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.test_schema)))
        cls.conn.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(cls.test_schema))
        )
        cls.conn.commit()
        schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
        with EnvGuard(SCHEMA_PATH=str(schema_path)):
            try:
                db.init_db(cls.conn)
            except Exception as exc:
                cls.tearDownClass()
                raise RuntimeError(f"Schema init failed: {exc}") from exc

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "conn"):
            cls.conn.rollback()
            cls.conn.execute("SET search_path TO public")
            cls.conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.test_schema))
            )
            cls.conn.commit()
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

    def test_compact_history_preserves_observations_and_movement_times(self):
        # Cross a month boundary, disappear, return unchanged, then move.
        start = datetime(2025, 1, 31, 23, 57, tzinfo=timezone.utc)
        times = [start + timedelta(minutes=i) for i in range(7)]
        name = "integration-compact-bike"
        expected = []
        try:
            with self.conn.cursor() as cur:
                ingest.upsert_bikes(cur, [(name, None, None, None, None, start, start)])
                for i, ts in enumerate(times):
                    db.ensure_partitions(cur, ts)
                    sid = ingest.insert_snapshot(cur, ts, "compact-test", None)
                    # i=2 is missing, i=3 crosses into February, i=5 changes battery.
                    battery = None if i < 5 else 50
                    lat = 49.0 if i < 6 else 49.002
                    rows = (
                        []
                        if i == 2
                        else [(sid, ts, name, None, True, "ok", battery, None, None, 8.4, lat)]
                    )
                    ingest.insert_bike_status(cur, sid, rows)
                    ingest.insert_bike_movements(cur, sid, ts, 60)
                    ingest.update_bike_last_status(cur, sid, ts)
                    if rows:
                        expected.append((ts, battery, lat))
                cur.execute("SELECT count(*) FROM bike_status WHERE bike_number = %s", (name,))
                self.assertEqual(cur.fetchone()[0], 4)
                cur.execute(
                    """SELECT fetched_at, pedelec_battery, ST_Y(geom)
                    FROM bike_status_samples(%s, %s) WHERE bike_number = %s
                    ORDER BY fetched_at""",
                    (times[0], times[-1], name),
                )
                self.assertEqual(cur.fetchall(), expected)
                # Start a query in the middle of an interval.
                cur.execute(
                    """SELECT fetched_at FROM bike_status_samples(%s, %s)
                    WHERE bike_number = %s ORDER BY fetched_at""",
                    (times[1], times[4], name),
                )
                self.assertEqual(cur.fetchall(), [(times[1],), (times[3],), (times[4],)])
                cur.execute(
                    """SELECT duration_seconds, start_fetched_at FROM bike_movement
                    WHERE bike_number = %s""",
                    (name,),
                )
                self.assertEqual(cur.fetchall(), [(60, times[5])])
                self.assertEqual(ingest.backfill_bike_movements(cur, 60), 0)
                cur.execute(
                    "SELECT fetched_at FROM bike_last_status WHERE bike_number = %s", (name,)
                )
                self.assertEqual(cur.fetchone()[0], times[-1])
        finally:
            self.conn.rollback()

    def test_all_domain_cities_are_imported_and_raw_is_disabled(self):
        ts = datetime(2025, 3, 1, tzinfo=timezone.utc)
        cfg = replace(
            config.load_config(),
            city_id=-930001,
            domain="filter-test",
            fetch_zones=False,
            fetch_gbfs=False,
            store_raw_json=False,
        )
        payload = {
            "countries": [
                {
                    "domain": "filter-test",
                    "cities": [
                        {"uid": -930001, "name": "Selected", "places": []},
                        {"uid": -930002, "name": "Additional city", "places": []},
                    ],
                }
            ]
        }
        try:
            # Outer transaction keeps this integration fixture rollback-only.
            self.conn.execute("SELECT 1")
            with (
                patch("nextspyke.ingest.utc_now", return_value=ts),
                patch("nextspyke.ingest.fetch_json", return_value=payload),
            ):
                result = ingest.ingest_once(self.conn, cfg)
            self.assertEqual(result["cities"], 2)
            self.assertEqual(
                self.conn.execute(
                    "SELECT city_uid FROM city WHERE domain = %s ORDER BY city_uid DESC",
                    (cfg.domain,),
                ).fetchall(),
                [(-930001,), (-930002,)],
            )
            self.assertIsNone(
                self.conn.execute(
                    "SELECT raw_json FROM snapshot WHERE domain = %s", (cfg.domain,)
                ).fetchone()[0]
            )
        finally:
            self.conn.rollback()

    def test_gap_and_return_to_previous_state_survive_temp_table_recreation(self):
        start = datetime(2025, 2, 5, 12, tzinfo=timezone.utc)
        name = "gap-return-bike"
        try:
            with self.conn.cursor() as cur:
                db.ensure_partitions(cur, start)
                ingest.upsert_bikes(cur, [(name, None, None, None, None, start, start)])
                expected = []
                for i, state in enumerate(["ok", None, "ok", "ok", "broken", "ok"]):
                    ts = start + timedelta(minutes=i)
                    sid = ingest.insert_snapshot(cur, ts, "gap-test", None)
                    if i == 3:
                        cur.execute("DROP TABLE pg_temp.current_bike_status")
                    rows = (
                        []
                        if state is None
                        else [(sid, ts, name, None, None, state, None, None, None, None, None)]
                    )
                    ingest.insert_bike_status(cur, sid, rows)
                    ingest.update_bike_last_status(cur, sid, ts)
                    if state is not None:
                        expected.append((ts, state))
                cur.execute("SELECT count(*) FROM bike_status WHERE bike_number = %s", (name,))
                self.assertEqual(cur.fetchone()[0], 4)
                cur.execute(
                    """SELECT fetched_at, state FROM bike_status_samples(%s, %s)
                    WHERE bike_number = %s ORDER BY fetched_at""",
                    (start, ts, name),
                )
                self.assertEqual(cur.fetchall(), expected)
        finally:
            self.conn.rollback()

    def test_dashboard_sql_parses_against_migrated_schema(self):
        def queries(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key == "rawSql":
                        yield child
                    else:
                        yield from queries(child)
            elif isinstance(value, list):
                for child in value:
                    yield from queries(child)

        try:
            for path in (ROOT / "observability/grafana/dashboards").glob("*.json"):
                for query in queries(json.loads(path.read_text(encoding="utf-8"))):
                    query = re.sub(
                        r"\$__timeGroup(Alias)?\(([^,]+),[^)]+\)",
                        lambda m: (
                            f"floor(extract(epoch from {m[2]}) / 3600) * 3600"
                            + (' AS "time"' if m[1] else "")
                        ),
                        query,
                    )
                    query = re.sub(
                        r"\$__timeFilter\(([^)]+)\)",
                        r"\1 BETWEEN '2025-01-01'::timestamptz AND '2025-02-01'::timestamptz",
                        query,
                    )
                    query = query.replace("$__timeFrom()", "'2025-01-01'::timestamptz")
                    query = query.replace("$__timeTo()", "'2025-02-01'::timestamptz")
                    with self.subTest(dashboard=path.name, query=query):
                        self.conn.execute("EXPLAIN " + query).fetchall()
        finally:
            self.conn.rollback()

    def test_legacy_compaction_dry_run_apply_and_repeat(self):
        # The cleanup command commits by design; isolate it in a disposable schema.
        schema = "test_storage_cleanup"
        self.conn.execute(f"CREATE SCHEMA {schema}")
        self.conn.execute(f"SET search_path TO {schema}, public")
        self.conn.commit()
        try:
            db.init_db(self.conn)
            day = datetime(2025, 4, 1, tzinfo=timezone.utc)
            with self.conn.cursor() as cur:
                ingest.upsert_bikes(cur, [("legacy", None, None, None, None, day, day)])
                for i in range(5):
                    ts = day + timedelta(minutes=i)
                    sid = ingest.insert_snapshot(cur, ts, "legacy-test", {"sample": i})
                    if i != 2:
                        cur.execute(
                            """INSERT INTO bike_status
                            (snapshot_id, fetched_at, bike_number, active, state, geom)
                            VALUES (%s, %s, 'legacy', true, 'ok', ST_SetSRID(ST_MakePoint(8.4,49),4326))""",
                            (sid, ts),
                        )
            self.conn.commit()
            result = compact_day(self.conn, day.date(), "legacy-test", purge_raw=True)
            self.assertEqual((result["before"], result["after"], result["applied"]), (4, 2, False))
            self.assertEqual(self.conn.execute("SELECT count(*) FROM bike_status").fetchone()[0], 4)
            self.conn.rollback()
            result = compact_day(self.conn, day.date(), "legacy-test", apply=True, purge_raw=True)
            self.assertEqual((result["before"], result["after"], result["raw_rows"]), (4, 2, 5))
            self.assertEqual(
                self.conn.execute(
                    "SELECT count(*) FROM snapshot WHERE raw_json IS NOT NULL"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                self.conn.execute(
                    "SELECT count(*) FROM bike_status_samples(%s,%s)",
                    (day, day + timedelta(days=1)),
                ).fetchone()[0],
                4,
            )
            self.conn.rollback()
            result = compact_day(self.conn, day.date(), "legacy-test", apply=True)
            self.assertEqual((result["before"], result["after"]), (2, 2))
        finally:
            self.conn.rollback()
            self.conn.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.test_schema))
            )
            self.conn.execute(f"DROP SCHEMA {schema} CASCADE")
            self.conn.commit()


if __name__ == "__main__":
    unittest.main()
