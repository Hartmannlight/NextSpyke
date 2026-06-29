import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import app
from nextspyke.config import AppConfig


def sample_config() -> AppConfig:
    return AppConfig(
        service="nextspyke",
        env="test",
        version="0.1.0",
        commit="abc123",
        domain="fg",
        city_id=21,
        poll_interval=1,
        fetch_zones=True,
        fetch_gbfs=True,
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


class DummyConn:
    def __init__(self):
        self.closed = False
        self.rollback_calls = 0

    def close(self):
        self.closed = True

    def rollback(self):
        self.rollback_calls += 1


class TestAppRuntime(unittest.TestCase):
    def setUp(self):
        app._shutdown_requested = False
        app._shutdown_reason = "signal"

    def test_main_run_once_success(self):
        dummy_conn = DummyConn()
        ingest_result = {
            "snapshot_id": 1,
            "fetched_at": app.utc_now(),
            "cities": 1,
            "places": 2,
            "bikes": 3,
            "movements": 4,
        }
        with patch.object(sys, "argv", ["app"]):
            with patch("nextspyke.app.env_bool", return_value=True):
                with patch("nextspyke.app.load_config", return_value=sample_config()):
                    with patch("nextspyke.app.psycopg.connect", return_value=dummy_conn):
                        with patch("nextspyke.app.init_db"):
                            with patch(
                                "nextspyke.app.ingest_once", return_value=ingest_result
                            ) as ingest_once:
                                with patch("nextspyke.app.log_event"):
                                    with patch("nextspyke.app.init_metrics"):
                                        with patch("nextspyke.app.start_metrics_server"):
                                            with patch(
                                                "nextspyke.app.mark_iteration_success"
                                            ) as mark_success:
                                                with patch(
                                                    "nextspyke.app.mark_iteration_failure"
                                                ) as mark_failure:
                                                    with patch(
                                                        "nextspyke.app.mark_shutdown"
                                                    ) as mark_shutdown:
                                                        with patch(
                                                            "nextspyke.app.time.sleep"
                                                        ) as sleep:
                                                            app.main()
        ingest_once.assert_called_once()
        mark_success.assert_called_once()
        mark_failure.assert_not_called()
        sleep.assert_not_called()
        self.assertTrue(dummy_conn.closed)
        self.assertTrue(mark_shutdown.called)

    def test_main_run_once_failure(self):
        dummy_conn = DummyConn()
        with patch.object(sys, "argv", ["app"]):
            with patch("nextspyke.app.env_bool", return_value=True):
                with patch("nextspyke.app.load_config", return_value=sample_config()):
                    with patch("nextspyke.app.psycopg.connect", return_value=dummy_conn):
                        with patch("nextspyke.app.init_db"):
                            with patch(
                                "nextspyke.app.ingest_once", side_effect=RuntimeError("boom")
                            ):
                                with patch("nextspyke.app.log_event"):
                                    with patch("nextspyke.app.init_metrics"):
                                        with patch("nextspyke.app.start_metrics_server"):
                                            with patch(
                                                "nextspyke.app.mark_iteration_success"
                                            ) as mark_success:
                                                with patch(
                                                    "nextspyke.app.mark_iteration_failure"
                                                ) as mark_failure:
                                                    with patch(
                                                        "nextspyke.app.mark_shutdown"
                                                    ) as mark_shutdown:
                                                        with patch("nextspyke.app.time.sleep"):
                                                            app.main()
        mark_success.assert_not_called()
        mark_failure.assert_called_once()
        self.assertEqual(dummy_conn.rollback_calls, 1)
        self.assertTrue(dummy_conn.closed)
        self.assertTrue(mark_shutdown.called)

    def test_main_recovers_after_failed_iteration(self):
        dummy_conn = DummyConn()
        ingest_result = {
            "snapshot_id": 2,
            "fetched_at": app.utc_now(),
            "cities": 1,
            "places": 2,
            "bikes": 3,
            "movements": 4,
        }
        sleep_calls = 0

        def stop_after_second_sleep(_seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls == 2:
                app._shutdown_requested = True

        with patch.object(sys, "argv", ["app"]):
            with patch("nextspyke.app.env_bool", return_value=False):
                with patch("nextspyke.app.load_config", return_value=sample_config()):
                    with patch("nextspyke.app.psycopg.connect", return_value=dummy_conn):
                        with patch("nextspyke.app.init_db"):
                            with patch(
                                "nextspyke.app.ingest_once",
                                side_effect=[RuntimeError("boom"), ingest_result],
                            ) as ingest_once:
                                with patch("nextspyke.app.log_event"):
                                    with patch("nextspyke.app.init_metrics"):
                                        with patch("nextspyke.app.start_metrics_server"):
                                            with patch(
                                                "nextspyke.app.mark_iteration_success"
                                            ) as mark_success:
                                                with patch(
                                                    "nextspyke.app.mark_iteration_failure"
                                                ) as mark_failure:
                                                    with patch("nextspyke.app.mark_shutdown"):
                                                        with patch(
                                                            "nextspyke.app.time.sleep",
                                                            side_effect=stop_after_second_sleep,
                                                        ):
                                                            app.main()
        self.assertEqual(ingest_once.call_count, 2)
        self.assertEqual(dummy_conn.rollback_calls, 1)
        mark_failure.assert_called_once()
        mark_success.assert_called_once()
        self.assertTrue(dummy_conn.closed)


if __name__ == "__main__":
    unittest.main()
