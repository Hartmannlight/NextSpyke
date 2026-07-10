import json
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import config, db, ingest


def sample_config() -> config.AppConfig:
    return config.AppConfig(
        service="nextspyke",
        env="test",
        version="0.1.0",
        commit="abc123",
        domain="fg",
        city_id=21,
        poll_interval=1,
        fetch_zones=True,
        fetch_gbfs=False,
        store_raw_json=True,
        movement_min_distance_m=10,
        refresh_mv_interval=0,
        refresh_mv_timeout=30,
        gbfs_system_id="nextbike_fg",
        metrics_enabled=False,
        metrics_port=8000,
        config_source="env",
        config_hash="sha256:test",
    )


class DummyTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def transaction(self):
        return DummyTransaction()

    def cursor(self):
        return DummyCursor()


class DummyResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


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


class TestApp(unittest.TestCase):
    def test_env_bool(self):
        self.assertTrue(config.env_bool("X", True))
        with EnvGuard(X="true"):
            self.assertTrue(config.env_bool("X", False))
        with EnvGuard(X="1"):
            self.assertTrue(config.env_bool("X", False))
        with EnvGuard(X="yes"):
            self.assertTrue(config.env_bool("X", False))
        with EnvGuard(X="off"):
            self.assertFalse(config.env_bool("X", True))

    def test_build_dsn_database_url(self):
        with EnvGuard(DATABASE_URL="postgresql://u:p@h:5432/db"):
            self.assertEqual(db.build_dsn(), "postgresql://u:p@h:5432/db")

    def test_build_dsn_parts(self):
        with EnvGuard(
            DATABASE_URL=None,
            PGHOST="db",
            PGPORT="5555",
            PGDATABASE="nextspyke",
            PGUSER="user",
            PGPASSWORD="pass",
        ):
            self.assertEqual(
                db.build_dsn(),
                "postgresql://user:pass@db:5555/nextspyke",
            )

    def test_load_config_materialized_view_timeout(self):
        with EnvGuard(
            MOVEMENT_MIN_DISTANCE_METERS="7.5",
            REFRESH_MV_TIMEOUT_SECONDS="12",
        ):
            loaded = config.load_config()
        self.assertEqual(loaded.refresh_mv_timeout, 12)
        self.assertEqual(loaded.movement_min_distance_m, 60)

    def test_fetch_json(self):
        payload = {"ok": True, "value": 3}
        response = DummyResponse(json.dumps(payload).encode("utf-8"))
        with patch("nextspyke.ingest.urlopen", return_value=response) as mocked:
            data = ingest.fetch_json("https://example.test/api", {"a": "b"})
        self.assertEqual(data, payload)
        mocked.assert_called_once()

    def test_load_schema_sql_from_env(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            tmp.write("select 1;")
            tmp_path = tmp.name
        try:
            with EnvGuard(SCHEMA_PATH=tmp_path):
                content = db.load_schema_sql()
            self.assertEqual(content, "select 1;")
        finally:
            os.remove(tmp_path)

    def test_ingest_once_persists_snapshot_when_optional_metadata_fails(self):
        live_payload = {
            "countries": [
                {
                    "domain": "fg",
                    "name": "KVV.nextbike",
                    "cities": [{"uid": 21, "places": []}],
                }
            ]
        }

        def fake_fetch_json(url, params=None):
            if url == ingest.LIVE_BASE_URL:
                self.assertEqual(params, {"domains": "fg"})
                return live_payload
            raise RuntimeError("optional endpoint failed")

        patched_helpers = (
            "ensure_partitions",
            "upsert_country",
            "upsert_cities",
            "insert_city_status",
            "upsert_places",
            "insert_place_status",
            "upsert_vehicle_types",
            "upsert_bikes",
            "insert_bike_status",
            "update_bike_last_status",
        )
        with ExitStack() as stack:
            for helper in patched_helpers:
                stack.enter_context(patch(f"nextspyke.ingest.{helper}"))
            stack.enter_context(patch("nextspyke.ingest.fetch_json", side_effect=fake_fetch_json))
            stack.enter_context(patch("nextspyke.ingest.record_snapshot_gap", return_value=None))
            stack.enter_context(patch("nextspyke.ingest.insert_snapshot", return_value=42))
            stack.enter_context(patch("nextspyke.ingest.insert_bike_movements", return_value=0))
            log_event = stack.enter_context(patch("nextspyke.ingest.log_event"))

            result = ingest.ingest_once(DummyConn(), sample_config())

        self.assertEqual(result["snapshot_id"], 42)
        self.assertEqual(result["cities"], 1)
        self.assertEqual(result["places"], 0)
        self.assertEqual(result["bikes"], 0)
        self.assertEqual(result["movements"], 0)
        self.assertEqual(log_event.call_count, 2)
        for call in log_event.call_args_list:
            self.assertEqual(call.kwargs["event"], "optional_ingest_failed")

    def test_movement_insert_includes_coordinate_changes(self):
        cur = Mock()
        cur.rowcount = 2
        fetched_at = datetime.now(timezone.utc)

        count = ingest.insert_bike_movements(cur, 42, fetched_at, 10)

        query, params = cur.execute.call_args.args
        self.assertIn("coordinate_change", query)
        self.assertIn("JOIN bike_last_status", query)
        self.assertIn("WHERE distance_m >= %s", query)
        self.assertEqual(params, (42, fetched_at, 10))
        self.assertEqual(count, 2)

    def test_movement_backfill_is_conflict_safe(self):
        cur = Mock()
        cur.rowcount = 3

        count = ingest.backfill_bike_movements(cur, 12.5)

        query, params = cur.execute.call_args.args
        self.assertIn("LAG(bs.geom)", query)
        self.assertIn("ON CONFLICT DO NOTHING", query)
        self.assertEqual(params, (12.5,))
        self.assertEqual(count, 3)

    def test_update_bike_last_status_is_monotonic(self):
        cur = Mock()
        fetched_at = datetime.now(timezone.utc)

        ingest.update_bike_last_status(cur, 42, fetched_at)

        query, params = cur.execute.call_args.args
        self.assertIn("ON CONFLICT (bike_number) DO UPDATE", query)
        self.assertIn("bike_last_status.fetched_at < EXCLUDED.fetched_at", query)
        self.assertEqual(params, (42, fetched_at))


if __name__ == "__main__":
    unittest.main()
