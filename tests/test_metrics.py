import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import URLError

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import metrics
from nextspyke.config import AppConfig


def sample_config(metrics_enabled: bool) -> AppConfig:
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
        refresh_mv_interval=0,
        gbfs_system_id="nextbike_fg",
        metrics_enabled=metrics_enabled,
        metrics_port=8001,
        config_source="env",
        config_hash="sha256:test",
    )


class DummyThread:
    def __init__(self, target, daemon):
        self._target = target
        self.daemon = daemon
        self.started = False

    def start(self):
        self.started = True
        self._target()


class TestMetrics(unittest.TestCase):
    def test_init_metrics_sets_build_info(self):
        labels_mock = Mock()
        labels_mock.set = Mock()
        with patch.object(metrics, "APP_BUILD_INFO") as build_info:
            with patch.object(metrics, "APP_UP") as app_up:
                build_info.labels.return_value = labels_mock
                metrics.init_metrics(sample_config(metrics_enabled=False))
        build_info.labels.assert_called_once_with(
            version="0.1.0",
            commit="abc123",
            env="test",
            service="nextspyke",
        )
        labels_mock.set.assert_called_once_with(1)
        app_up.set.assert_called_once_with(1)

    def test_start_metrics_server_enabled_only_once(self):
        metrics._metrics_started = False
        with patch("nextspyke.metrics.Thread", DummyThread):
            with patch("nextspyke.metrics.start_http_server") as start_server:
                metrics.start_metrics_server(sample_config(metrics_enabled=True))
                metrics.start_metrics_server(sample_config(metrics_enabled=True))
        start_server.assert_called_once_with(8001)

    def test_start_metrics_server_disabled(self):
        metrics._metrics_started = False
        with patch("nextspyke.metrics.start_http_server") as start_server:
            metrics.start_metrics_server(sample_config(metrics_enabled=False))
        start_server.assert_not_called()

    def test_mark_iteration_success(self):
        with patch.object(metrics, "APP_ITERATIONS_TOTAL") as iterations:
            with patch.object(metrics, "APP_ITERATION_DURATION") as duration:
                with patch.object(metrics, "APP_LAST_ITERATION_TS") as last_ts:
                    metrics.mark_iteration_success(1.5)
        iterations.inc.assert_called_once_with()
        duration.observe.assert_called_once_with(1.5)
        last_ts.set.assert_called_once()

    def test_mark_iteration_failure(self):
        labels_mock = Mock()
        labels_mock.inc = Mock()
        with patch.object(metrics, "APP_ITERATIONS_TOTAL") as iterations:
            with patch.object(metrics, "APP_ITERATION_FAILURES_TOTAL") as failures:
                with patch.object(metrics, "APP_ITERATION_FAILURE_REASONS_TOTAL") as reasons:
                    with patch.object(metrics, "APP_ITERATION_DURATION") as duration:
                        with patch.object(metrics, "APP_LAST_ITERATION_TS") as last_ts:
                            reasons.labels.return_value = labels_mock
                            metrics.mark_iteration_failure(2.0, "db")
        iterations.inc.assert_called_once_with()
        failures.inc.assert_called_once_with()
        reasons.labels.assert_called_once_with(reason="db")
        labels_mock.inc.assert_called_once_with()
        duration.observe.assert_called_once_with(2.0)
        last_ts.set.assert_called_once()

    def test_classify_failure_reason(self):
        self.assertEqual(metrics.classify_failure_reason(psycopg.Error("x")), "db")
        self.assertEqual(metrics.classify_failure_reason(URLError("x")), "http")
        self.assertEqual(metrics.classify_failure_reason(Exception("x")), "unknown")


if __name__ == "__main__":
    unittest.main()
