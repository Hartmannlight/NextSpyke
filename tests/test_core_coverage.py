import json
import runpy
import signal
import sys
import unittest
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import ANY, Mock, patch

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import app, config, db, health, ingest, metrics
from nextspyke import logging as app_logging
from nextspyke.config import AppConfig


def sample_config(
    *,
    fetch_zones: bool = True,
    fetch_gbfs: bool = True,
    store_raw_json: bool = True,
    city_id: int | None = 21,
    poll_interval: int = 60,
    metrics_enabled: bool = False,
) -> AppConfig:
    return AppConfig(
        service="nextspyke",
        env="test",
        version="0.1.0",
        commit="abc123",
        domain="fg",
        city_id=city_id,
        poll_interval=poll_interval,
        fetch_zones=fetch_zones,
        fetch_gbfs=fetch_gbfs,
        store_raw_json=store_raw_json,
        movement_min_distance_m=10,
        refresh_mv_interval=0,
        refresh_mv_timeout=30,
        gbfs_system_id="nextbike_fg",
        metrics_enabled=metrics_enabled,
        metrics_port=8001,
        config_source="env",
        config_hash="sha256:test",
    )


class DummyTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class CursorContext:
    def __init__(self, cursor):
        self.cursor_obj = cursor

    def __enter__(self):
        return self.cursor_obj

    def __exit__(self, exc_type, exc, tb):
        return False


class ConnectionWithCursor:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.closed = False
        self.broken = False
        self.rollback_calls = 0
        self.close_calls = 0

    def transaction(self):
        return DummyTransaction()

    def cursor(self):
        return CursorContext(self.cursor_obj)

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.closed = True
        self.close_calls += 1


class HealthCursor:
    def __init__(self, *, latest_snapshot=None, fail_snapshot_query=False):
        self.latest_snapshot = latest_snapshot
        self.fail_snapshot_query = fail_snapshot_query
        self.execute_calls = 0

    def execute(self, _query: str, *_params) -> None:
        self.execute_calls += 1
        if self.fail_snapshot_query and self.execute_calls == 2:
            raise RuntimeError("snapshot query failed")

    def fetchone(self):
        return (self.latest_snapshot,) if self.latest_snapshot else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class HealthConn:
    def __init__(self, cursor):
        self.cursor_obj = cursor

    def cursor(self):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestConfigCoverage(unittest.TestCase):
    def test_sanitize_config_redacts_secrets_and_hash_is_stable(self):
        payload = {
            "PGPASSWORD": "secret",
            "DATABASE_URL": "postgres://demo",
            "NEXTBIKE_DOMAIN": "fg",
            "EMPTY": None,
        }
        sanitized = config.sanitize_config(payload)
        self.assertEqual(sanitized["PGPASSWORD"], "***")
        self.assertEqual(sanitized["DATABASE_URL"], "***")
        self.assertEqual(sanitized["NEXTBIKE_DOMAIN"], "fg")
        self.assertIsNone(sanitized["EMPTY"])
        reversed_payload = dict(reversed(list(payload.items())))
        self.assertEqual(
            config.hash_config(sanitized),
            config.hash_config(config.sanitize_config(reversed_payload)),
        )


class TestDbCoverage(unittest.TestCase):
    def test_tracking_dashboard_uses_bounded_location_model(self):
        dashboard = json.loads(
            Path("observability/grafana/dashboards/nextspyke-overview.json").read_text()
        )
        panels = {panel["id"]: panel for panel in dashboard["panels"]}

        self.assertIn("$__timeGroupAlias", panels[11]["targets"][0]["rawSql"])
        self.assertEqual(panels[11]["targets"][0]["format"], "time_series")
        self.assertEqual(panels[13]["options"]["basemap"]["config"]["theme"], "light")
        self.assertEqual(
            panels[13]["options"]["layers"][0]["config"]["style"]["symbol"]["fixed"],
            "img/icons/unicons/map-marker.svg",
        )
        self.assertIn(
            "bls.fetched_at = (SELECT MAX(fetched_at) FROM snapshot)",
            panels[13]["targets"][0]["rawSql"],
        )
        self.assertGreaterEqual(
            panels[13]["gridPos"]["y"],
            panels[11]["gridPos"]["y"] + panels[11]["gridPos"]["h"],
        )
        self.assertEqual(panels[14]["options"]["layers"][0]["type"], "heatmap")
        self.assertEqual(
            panels[14]["options"]["layers"][0]["config"]["weight"]["field"],
            "parking_events",
        )
        self.assertIn("ST_SnapToGrid", panels[14]["targets"][0]["rawSql"])
        self.assertEqual(panels[15]["gridPos"]["w"], 24)
        self.assertIn("distance_m >= 60", panels[15]["targets"][0]["rawSql"])

        datasource_config = Path(
            "observability/grafana/provisioning/datasources/datasource.yml"
        ).read_text()
        self.assertIn("uid: nextspyke-postgres", datasource_config)
        self.assertIn("uid: PBFA97CFB590B2093", datasource_config)

    def test_load_schema_sql_prefers_repo_fallback(self):
        with patch("nextspyke.db.os.getenv", return_value=None):
            content = db.load_schema_sql()
        self.assertIn("CREATE TABLE", content)
        self.assertIn("CREATE TABLE IF NOT EXISTS bike_last_status", content)
        self.assertIn("DROP MATERIALIZED VIEW IF EXISTS mv_city_bikes_hourly", content)
        self.assertIn(
            'echo "tags<<EOF"',
            Path(".github/workflows/container-release.yml").read_text(),
        )

    def test_load_schema_sql_raises_when_no_candidate_exists(self):
        with patch("nextspyke.db.Path.is_file", return_value=False):
            with patch("nextspyke.db.os.getenv", return_value="missing.sql"):
                with self.assertRaises(FileNotFoundError):
                    db.load_schema_sql()

    def test_init_db_executes_schema_and_commits(self):
        cursor = Mock()
        cursor_ctx = CursorContext(cursor)
        conn = Mock()
        conn.cursor.return_value = cursor_ctx
        with patch("nextspyke.db.load_schema_sql", return_value="select 1;"):
            db.init_db(conn)
        cursor.execute.assert_called_once_with("select 1;")
        conn.commit.assert_called_once_with()

    def test_month_bounds_handles_both_month_paths(self):
        start, end = db.month_bounds(datetime(2026, 6, 15, tzinfo=timezone.utc))
        self.assertEqual((start.month, end.month), (6, 7))
        start_dec, end_dec = db.month_bounds(datetime(2026, 12, 15, tzinfo=timezone.utc))
        self.assertEqual((start_dec.year, end_dec.year, end_dec.month), (2026, 2027, 1))

    def test_ensure_month_partition_executes_sql(self):
        cur = Mock()
        db.ensure_month_partition(cur, "snapshot", datetime(2026, 6, 15, tzinfo=timezone.utc))
        self.assertEqual(cur.execute.call_count, 1)

    def test_ensure_partitions_calls_all_tables(self):
        cur = Mock()
        with patch("nextspyke.db.ensure_month_partition") as ensure_month_partition:
            db.ensure_partitions(cur, datetime(2026, 6, 15, tzinfo=timezone.utc))
        self.assertEqual(ensure_month_partition.call_count, 4)


class TestLoggingCoverage(unittest.TestCase):
    def test_json_default_stringifies_unknown_values(self):
        self.assertEqual(app_logging._json_default(object())[:8], "<object ")

    def test_log_event_with_error_and_lifecycle_metadata(self):
        output = StringIO()
        with patch("sys.stdout", output):
            try:
                raise RuntimeError("boom")
            except RuntimeError as exc:
                app_logging.log_event(
                    "error",
                    "app.lifecycle",
                    "Application crashed",
                    event="crashed",
                    config=sample_config(),
                    extra={"extra_value": 1},
                    exc=exc,
                )
        record = json.loads(output.getvalue())
        self.assertEqual(record["event"], "crashed")
        self.assertEqual(record["version"], "0.1.0")
        self.assertEqual(record["error"], "boom")
        self.assertEqual(record["exception_type"], "RuntimeError")
        self.assertIn("RuntimeError: boom", record["stack"])
        self.assertEqual(record["extra_value"], 1)

    def test_log_event_without_config_uses_defaults(self):
        output = StringIO()
        with patch("sys.stdout", output):
            app_logging.log_event("info", "custom", "plain")
        record = json.loads(output.getvalue())
        self.assertEqual(record["service"], "nextspyke")
        self.assertEqual(record["env"], "dev")
        self.assertNotIn("event", record)


class TestHealthCoverage(unittest.TestCase):
    def test_health_snapshot_query_failure_is_reported(self):
        buffer = StringIO()
        cursor = HealthCursor(latest_snapshot=datetime.now(timezone.utc), fail_snapshot_query=True)
        with patch("nextspyke.health.psycopg.connect", return_value=HealthConn(cursor)):
            with patch("sys.stdout", buffer):
                code = health.health_check(sample_config())
        payload = json.loads(buffer.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(payload["checks"][1]["status"], "ok")
        self.assertEqual(payload["checks"][2]["status"], "fail")
        self.assertIsNone(payload["checks"][2]["age_seconds"])


class TestMetricsCoverage(unittest.TestCase):
    def test_mark_shutdown_sets_gauge(self):
        with patch.object(metrics, "APP_UP") as app_up:
            metrics.mark_shutdown()
        app_up.set.assert_called_once_with(0)


class TestAppCoverage(unittest.TestCase):
    def setUp(self):
        app._shutdown_requested = False
        app._shutdown_reason = "signal"

    def test_connect_and_init_db_closes_connection_on_init_error(self):
        conn = ConnectionWithCursor(Mock())
        with patch("nextspyke.app.psycopg.connect", return_value=conn):
            with patch("nextspyke.app.init_db", side_effect=RuntimeError("bad schema")):
                with self.assertRaises(RuntimeError):
                    app._connect_and_init_db()
        self.assertTrue(conn.closed)

    def test_connection_closed_and_close_connection(self):
        conn = ConnectionWithCursor(Mock())
        self.assertTrue(app._connection_closed(None))
        self.assertFalse(app._connection_closed(conn))
        conn.closed = True
        self.assertTrue(app._connection_closed(conn))
        app._close_connection(conn)
        self.assertEqual(conn.close_calls, 0)
        conn.closed = False
        app._close_connection(conn)
        self.assertEqual(conn.close_calls, 1)

    def test_recover_connection_after_failure_variants(self):
        cfg = sample_config()

        self.assertIsNone(app._recover_connection_after_failure(None, cfg, RuntimeError("x")))

        rollback_broken = ConnectionWithCursor(Mock())

        def fail_rollback():
            raise RuntimeError("rollback-failed")

        rollback_broken.rollback = fail_rollback
        with patch("nextspyke.app.log_event") as log_event:
            recovered = app._recover_connection_after_failure(
                rollback_broken,
                cfg,
                RuntimeError("orig"),
            )
        self.assertIsNone(recovered)
        self.assertTrue(rollback_broken.closed)
        log_event.assert_called_once()

        op_conn = ConnectionWithCursor(Mock())
        recovered = app._recover_connection_after_failure(
            op_conn,
            cfg,
            psycopg.OperationalError("db down"),
        )
        self.assertIsNone(recovered)
        self.assertTrue(op_conn.closed)

        broken_conn = ConnectionWithCursor(Mock())
        broken_conn.broken = True
        recovered = app._recover_connection_after_failure(
            broken_conn,
            cfg,
            RuntimeError("boom"),
        )
        self.assertIsNone(recovered)
        self.assertTrue(broken_conn.closed)

        healthy_conn = ConnectionWithCursor(Mock())
        recovered = app._recover_connection_after_failure(
            healthy_conn,
            cfg,
            RuntimeError("boom"),
        )
        self.assertIs(recovered, healthy_conn)
        self.assertEqual(healthy_conn.rollback_calls, 1)

    def test_handle_signal_sets_named_and_unknown_reason(self):
        app._handle_signal(signal.SIGTERM, None)
        self.assertTrue(app._shutdown_requested)
        self.assertEqual(app._shutdown_reason, "SIGTERM")
        app._shutdown_requested = False
        app._handle_signal(999, None)
        self.assertEqual(app._shutdown_reason, "signal_999")

    def test_run_movement_backfill_logs_and_closes(self):
        cur = Mock()
        conn = ConnectionWithCursor(cur)
        with patch("nextspyke.app._connect_and_init_db", return_value=conn):
            with patch("nextspyke.app.backfill_bike_movements", return_value=7) as backfill:
                with patch("nextspyke.app.log_event") as log_event:
                    app._run_movement_backfill(sample_config())
        backfill.assert_called_once_with(cur, 10)
        self.assertTrue(conn.closed)
        self.assertEqual(log_event.call_args.kwargs["event"], "movement_backfill_complete")

    def test_main_health_branch_exits_with_health_status(self):
        with patch.object(sys, "argv", ["app", "health"]):
            with patch("nextspyke.app.load_config", return_value=sample_config()):
                with patch("nextspyke.app.health_check", return_value=3):
                    with self.assertRaises(SystemExit) as raised:
                        app.main()
        self.assertEqual(raised.exception.code, 3)

    def test_main_backfill_branch_runs_and_returns(self):
        with patch.object(sys, "argv", ["app", "backfill-movements"]):
            with patch("nextspyke.app.load_config", return_value=sample_config()):
                with patch("nextspyke.app._run_movement_backfill") as run_backfill:
                    app.main()
        run_backfill.assert_called_once()

    def test_main_logs_crash_for_exception_outside_iteration_handler(self):
        conn = ConnectionWithCursor(Mock())
        ingest_result = {
            "snapshot_id": 1,
            "fetched_at": app.utc_now(),
            "cities": 0,
            "places": 0,
            "bikes": 0,
            "movements": 0,
        }
        with patch.object(sys, "argv", ["app"]):
            with patch("nextspyke.app.env_bool", return_value=False):
                with patch("nextspyke.app.load_config", return_value=sample_config()):
                    with patch("nextspyke.app._connect_and_init_db", return_value=conn):
                        with patch("nextspyke.app.ingest_once", return_value=ingest_result):
                            with patch("nextspyke.app.log_event") as log_event:
                                with patch("nextspyke.app.init_metrics"):
                                    with patch("nextspyke.app.start_metrics_server"):
                                        with patch("nextspyke.app.mark_iteration_success"):
                                            with patch(
                                                "nextspyke.app.time.sleep",
                                                side_effect=RuntimeError("sleep boom"),
                                            ):
                                                with self.assertRaises(RuntimeError):
                                                    app.main()
        self.assertTrue(
            any(call.kwargs.get("event") == "crashed" for call in log_event.call_args_list)
        )

    def test_main_reopens_closed_connection_before_ingest(self):
        closed_conn = ConnectionWithCursor(Mock())
        closed_conn.closed = True
        open_conn = ConnectionWithCursor(Mock())
        ingest_result = {
            "snapshot_id": 1,
            "fetched_at": app.utc_now(),
            "cities": 0,
            "places": 0,
            "bikes": 0,
            "movements": 0,
        }
        with patch.object(sys, "argv", ["app"]):
            with patch("nextspyke.app.env_bool", return_value=True):
                with patch("nextspyke.app.load_config", return_value=sample_config()):
                    with patch(
                        "nextspyke.app._connect_and_init_db",
                        side_effect=[closed_conn, open_conn],
                    ) as connect_and_init:
                        with patch(
                            "nextspyke.app.ingest_once", return_value=ingest_result
                        ) as ingest_once:
                            with patch("nextspyke.app.log_event"):
                                with patch("nextspyke.app.init_metrics"):
                                    with patch("nextspyke.app.start_metrics_server"):
                                        with patch("nextspyke.app.mark_iteration_success"):
                                            with patch("nextspyke.app.mark_iteration_failure"):
                                                with patch("nextspyke.app.mark_shutdown"):
                                                    app.main()
        self.assertEqual(connect_and_init.call_count, 2)
        ingest_once.assert_called_once_with(open_conn, ANY)

    def test_module_main_guard_executes(self):
        ingest_result = {
            "snapshot_id": 1,
            "fetched_at": app.utc_now(),
            "cities": 0,
            "places": 0,
            "bikes": 0,
            "movements": 0,
        }
        conn = ConnectionWithCursor(Mock())
        with patch.object(sys, "argv", ["app"]):
            with patch("nextspyke.config.load_config", return_value=sample_config()):
                with patch("nextspyke.config.env_bool", return_value=True):
                    with patch("psycopg.connect", return_value=conn):
                        with patch("nextspyke.db.init_db"):
                            with patch("nextspyke.ingest.ingest_once", return_value=ingest_result):
                                with patch("nextspyke.metrics.init_metrics"):
                                    with patch("nextspyke.metrics.start_metrics_server"):
                                        with patch("nextspyke.metrics.mark_iteration_success"):
                                            with patch("nextspyke.metrics.mark_iteration_failure"):
                                                with patch("nextspyke.metrics.mark_shutdown"):
                                                    with patch("nextspyke.logging.log_event"):
                                                        with patch("signal.signal"):
                                                            runpy.run_module(
                                                                "nextspyke.app", run_name="__main__"
                                                            )


class TestIngestCoverage(unittest.TestCase):
    def test_fetch_json_without_params(self):
        response = DummyResponse(b'{"ok": true}')
        with patch("nextspyke.ingest.urlopen", return_value=response) as urlopen_mock:
            data = ingest.fetch_json("https://example.test/plain")
        self.assertEqual(data, {"ok": True})
        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, "https://example.test/plain")

    def test_log_optional_failure_and_connection_failure_classifier(self):
        with patch("nextspyke.ingest.log_event") as log_event:
            ingest.log_optional_failure(sample_config(), "zone-service", RuntimeError("boom"))
        self.assertEqual(log_event.call_args.kwargs["event"], "optional_ingest_failed")
        self.assertTrue(ingest.is_connection_failure(psycopg.InterfaceError("x")))
        self.assertFalse(ingest.is_connection_failure(RuntimeError("x")))

    def test_upsert_country_and_city_and_place_helpers(self):
        cur = Mock()
        ingest.upsert_country(cur, {"domain": "fg", "name": "Demo"})
        self.assertEqual(cur.execute.call_args.args[1][0], "fg")

        cur.reset_mock()
        ingest.upsert_cities(
            cur,
            "fg",
            [
                {
                    "uid": 1,
                    "name": "A",
                    "bounds": {
                        "south_west": {"lng": 1, "lat": 2},
                        "north_east": {"lng": 3, "lat": 4},
                    },
                    "refresh_rate": "60000",
                    "place_types": {"station": 1},
                },
                {
                    "uid": 2,
                    "name": "B",
                    "domain": "custom",
                },
            ],
        )
        self.assertEqual(cur.execute.call_count, 2)

        cur.reset_mock()
        ingest.upsert_places(
            cur,
            1,
            [
                {
                    "uid": 10,
                    "name": "Station",
                    "spot": True,
                    "lat": 49.0,
                    "lng": 8.4,
                    "place_type": 5,
                },
                {
                    "uid": 11,
                    "name": "Free",
                    "spot": False,
                    "lat": 49.1,
                    "lng": 8.5,
                    "place_type": None,
                },
            ],
        )
        self.assertEqual(cur.execute.call_count, 1)

    def test_upsert_vehicle_types_and_bikes_empty_and_non_empty(self):
        cur = Mock()
        ingest.upsert_vehicle_types(cur, set())
        cur.executemany.assert_not_called()
        ingest.upsert_vehicle_types(cur, {"1", "2"})
        self.assertEqual(cur.executemany.call_count, 1)

        cur.reset_mock()
        ingest.upsert_bikes(cur, [])
        cur.executemany.assert_not_called()
        ingest.upsert_bikes(
            cur,
            [
                (
                    "100",
                    "bc",
                    "1",
                    True,
                    ["ring"],
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                )
            ],
        )
        self.assertEqual(cur.executemany.call_count, 1)

    def test_insert_status_helpers_and_snapshot(self):
        cur = Mock()
        fetched_at = datetime.now(timezone.utc)
        ingest.insert_city_status(cur, 1, fetched_at, {"uid": 9, "bike_types": {"cargo": 2}})
        self.assertEqual(cur.execute.call_count, 1)

        cur.reset_mock()
        ingest.insert_place_status(cur, 1, fetched_at, [])
        cur.executemany.assert_not_called()
        ingest.insert_place_status(
            cur,
            1,
            fetched_at,
            [
                {"uid": 2, "spot": False},
                {"uid": 3, "spot": True, "bike_types": {"ebike": 1}},
            ],
        )
        self.assertEqual(cur.executemany.call_count, 1)
        self.assertEqual(len(cur.executemany.call_args.args[1]), 1)

        cur.reset_mock()
        ingest.insert_bike_status(cur, 1, [])
        cur.executemany.assert_not_called()
        ingest.insert_bike_status(
            cur, 1, [(1, fetched_at, "100", 3, True, "ok", 88, 77, 12.5, 8.4, 49.0)]
        )
        self.assertEqual(cur.executemany.call_count, 1)

        cur.reset_mock()
        cur.fetchone.return_value = [42]
        snapshot_id = ingest.insert_snapshot(cur, fetched_at, "fg", None)
        self.assertEqual(snapshot_id, 42)
        self.assertIsNone(cur.execute.call_args.args[1][3])

        cur.reset_mock()
        cur.fetchone.return_value = [43]
        ingest.insert_snapshot(cur, fetched_at, "fg", {"raw": True})
        self.assertIsNotNone(cur.execute.call_args.args[1][3])

    def test_record_snapshot_gap_all_branches(self):
        cur = Mock()
        fetched_at = datetime(2026, 6, 29, 12, 10, tzinfo=timezone.utc)

        cur.fetchone.return_value = None
        self.assertIsNone(ingest.record_snapshot_gap(cur, fetched_at, "fg", 60))

        cur.fetchone.return_value = (fetched_at - timedelta(seconds=120),)
        self.assertIsNone(ingest.record_snapshot_gap(cur, fetched_at, "fg", 0))

        cur.fetchone.return_value = (fetched_at - timedelta(seconds=80),)
        self.assertIsNone(ingest.record_snapshot_gap(cur, fetched_at, "fg", 60))

        cur.reset_mock()
        cur.fetchone.return_value = (fetched_at - timedelta(seconds=240),)
        gap = ingest.record_snapshot_gap(cur, fetched_at, "fg", 60)
        self.assertEqual(gap["missing_count"], 3)
        self.assertEqual(cur.execute.call_count, 2)

    def test_upsert_zone_features_and_gbfs_helpers(self):
        cur = Mock()
        ingest.upsert_zone_features(
            cur,
            21,
            "flexzone",
            {
                "features": [
                    {"properties": {}, "geometry": {"type": "Polygon"}},
                    {"id": "z1", "properties": {"type": "slow"}, "geometry": None},
                    {
                        "properties": {"flexzoneId": 9, "category": "flex", "name": "Zone"},
                        "geometry": {"type": "Polygon", "coordinates": []},
                    },
                ]
            },
        )
        self.assertEqual(cur.execute.call_count, 1)
        self.assertEqual(cur.execute.call_args.args[1][0], "9")

        with patch("nextspyke.ingest.fetch_json", return_value={"data": {"en": {"feeds": []}}}):
            self.assertEqual(ingest.fetch_gbfs_vehicle_types("demo"), [])

        with patch(
            "nextspyke.ingest.fetch_json",
            side_effect=[
                {
                    "data": {
                        "en": {
                            "feeds": [
                                {"name": "station_information", "url": "https://ignore"},
                                {"name": "vehicle_types", "url": "https://types"},
                            ]
                        }
                    }
                },
                {"data": {"vehicle_types": [{"vehicle_type_id": "1"}]}},
            ],
        ):
            self.assertEqual(ingest.fetch_gbfs_vehicle_types("demo"), [{"vehicle_type_id": "1"}])

    def test_upsert_vehicle_type_metadata_empty_and_non_empty(self):
        cur = Mock()
        ingest.upsert_vehicle_type_metadata(cur, [])
        cur.executemany.assert_not_called()
        ingest.upsert_vehicle_type_metadata(
            cur,
            [
                {
                    "vehicle_type_id": "1",
                    "name": "Bike",
                    "form_factor": "bicycle",
                    "propulsion_type": "human",
                    "max_range_meters": 1000,
                    "_description": "desc",
                    "rider_capacity": 1,
                    "vehicle_image": "img",
                }
            ],
        )
        self.assertEqual(cur.executemany.call_count, 1)

    def test_refresh_zone_metadata_branches(self):
        cur = Mock()
        conn = ConnectionWithCursor(cur)

        ingest.refresh_zone_metadata(conn, sample_config(fetch_zones=False))
        ingest.refresh_zone_metadata(conn, sample_config(city_id=None))
        self.assertEqual(cur.execute.call_count, 0)

        with patch("nextspyke.ingest.fetch_json", return_value={"features": []}) as fetch_json:
            with patch("nextspyke.ingest.upsert_zone_features") as upsert_zone_features:
                ingest.refresh_zone_metadata(conn, sample_config())
        self.assertEqual(fetch_json.call_count, 2)
        self.assertEqual(upsert_zone_features.call_count, 2)

        with patch(
            "nextspyke.ingest.fetch_json",
            side_effect=[RuntimeError("bad zone"), {"features": []}],
        ):
            with patch("nextspyke.ingest.log_optional_failure") as log_optional_failure:
                with patch("nextspyke.ingest.upsert_zone_features"):
                    ingest.refresh_zone_metadata(conn, sample_config())
        self.assertEqual(log_optional_failure.call_count, 1)

        with patch("nextspyke.ingest.fetch_json", side_effect=psycopg.InterfaceError("db")):
            with self.assertRaises(psycopg.InterfaceError):
                ingest.refresh_zone_metadata(conn, sample_config())

    def test_refresh_vehicle_type_metadata_branches(self):
        cur = Mock()
        conn = ConnectionWithCursor(cur)

        ingest.refresh_vehicle_type_metadata(conn, sample_config(fetch_gbfs=False))
        self.assertEqual(cur.execute.call_count, 0)

        with patch(
            "nextspyke.ingest.fetch_gbfs_vehicle_types", return_value=[{"vehicle_type_id": "1"}]
        ):
            with patch(
                "nextspyke.ingest.upsert_vehicle_type_metadata"
            ) as upsert_vehicle_type_metadata:
                ingest.refresh_vehicle_type_metadata(conn, sample_config())
        upsert_vehicle_type_metadata.assert_called_once()

        with patch(
            "nextspyke.ingest.fetch_gbfs_vehicle_types", side_effect=RuntimeError("bad gbfs")
        ):
            with patch("nextspyke.ingest.log_optional_failure") as log_optional_failure:
                ingest.refresh_vehicle_type_metadata(conn, sample_config())
        log_optional_failure.assert_called_once()

        with patch(
            "nextspyke.ingest.fetch_gbfs_vehicle_types", side_effect=psycopg.OperationalError("db")
        ):
            with self.assertRaises(psycopg.OperationalError):
                ingest.refresh_vehicle_type_metadata(conn, sample_config())

    def test_ingest_once_raises_without_country(self):
        conn = ConnectionWithCursor(Mock())
        with patch("nextspyke.ingest.fetch_json", return_value={"countries": []}):
            with self.assertRaisesRegex(RuntimeError, "No country data"):
                ingest.ingest_once(conn, sample_config())

    def test_ingest_once_full_success_path(self):
        fetched_at = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)
        cur = Mock()
        conn = ConnectionWithCursor(cur)
        live_data = {
            "countries": [
                {
                    "domain": "fg",
                    "cities": [
                        {
                            "uid": 21,
                            "places": [
                                {
                                    "uid": 7,
                                    "spot": True,
                                    "lat": 49.0,
                                    "lng": 8.4,
                                    "bike_list": [
                                        {
                                            "number": "100",
                                            "bike_type": 5,
                                            "boardcomputer": "bc",
                                            "electric_lock": True,
                                            "lock_types": ["ring"],
                                            "active": True,
                                            "state": "ok",
                                            "pedelec_battery": 88,
                                            "battery_pack": {
                                                "percentage": 77,
                                                "estimated_range_km": 12.5,
                                            },
                                        },
                                        {"bike_type": 6},
                                        {
                                            "number": "101",
                                            "active": True,
                                            "state": "ok",
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        with patch("nextspyke.ingest.utc_now", return_value=fetched_at):
            with patch("nextspyke.ingest.fetch_json", return_value=live_data):
                with patch("nextspyke.ingest.ensure_partitions") as ensure_partitions:
                    with patch("nextspyke.ingest.upsert_country") as upsert_country:
                        with patch("nextspyke.ingest.upsert_cities") as upsert_cities:
                            with patch(
                                "nextspyke.ingest.record_snapshot_gap",
                                return_value={"gap_start": fetched_at - timedelta(minutes=5)},
                            ):
                                with patch("nextspyke.ingest.log_event") as log_event:
                                    with patch("nextspyke.ingest.insert_snapshot", return_value=9):
                                        with patch(
                                            "nextspyke.ingest.insert_city_status"
                                        ) as insert_city_status:
                                            with patch(
                                                "nextspyke.ingest.upsert_places"
                                            ) as upsert_places:
                                                with patch(
                                                    "nextspyke.ingest.insert_place_status"
                                                ) as insert_place_status:
                                                    with patch(
                                                        "nextspyke.ingest.upsert_vehicle_types"
                                                    ) as upsert_vehicle_types:
                                                        with patch(
                                                            "nextspyke.ingest.upsert_bikes"
                                                        ) as upsert_bikes:
                                                            with patch(
                                                                "nextspyke.ingest.insert_bike_status"
                                                            ) as insert_bike_status:
                                                                with (
                                                                    patch(
                                                                        "nextspyke.ingest.insert_bike_movements",
                                                                        return_value=2,
                                                                    ),
                                                                    patch(
                                                                        "nextspyke.ingest.update_bike_last_status"
                                                                    ) as update_last_status,
                                                                ):
                                                                    with patch(
                                                                        "nextspyke.ingest.refresh_zone_metadata"
                                                                    ) as refresh_zone_metadata:
                                                                        with patch(
                                                                            "nextspyke.ingest.refresh_vehicle_type_metadata"
                                                                        ) as refresh_types:
                                                                            result = ingest.ingest_once(
                                                                                conn,
                                                                                sample_config(
                                                                                    store_raw_json=False
                                                                                ),
                                                                            )
        self.assertEqual(result["snapshot_id"], 9)
        self.assertEqual(result["places"], 1)
        self.assertEqual(result["bikes"], 2)
        ensure_partitions.assert_called_once_with(cur, fetched_at)
        upsert_country.assert_called_once()
        upsert_cities.assert_called_once_with(cur, "fg", live_data["countries"][0]["cities"])
        insert_city_status.assert_called_once()
        upsert_places.assert_called_once()
        insert_place_status.assert_called_once()
        upsert_vehicle_types.assert_called_once_with(cur, {"5"})
        upsert_bikes.assert_called_once()
        insert_bike_status.assert_called_once()
        update_last_status.assert_called_once_with(cur, 9, fetched_at)
        refresh_zone_metadata.assert_called_once()
        refresh_types.assert_called_once()
        self.assertTrue(
            any(call.kwargs.get("event") == "snapshot_gap" for call in log_event.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
