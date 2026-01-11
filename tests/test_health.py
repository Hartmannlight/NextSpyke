import json
import sys
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke.config import AppConfig
from nextspyke.health import health_check


class DummyCursor:
    def execute(self, _query: str) -> None:
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def cursor(self):
        return DummyCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


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
        refresh_mv_interval=0,
        gbfs_system_id="nextbike_fg",
        metrics_enabled=False,
        metrics_port=8000,
        config_source="env",
        config_hash="sha256:test",
    )


class TestHealth(unittest.TestCase):
    def test_health_ok(self):
        buffer = StringIO()
        with patch("nextspyke.health.psycopg.connect", return_value=DummyConn()):
            with patch("sys.stdout", buffer):
                code = health_check(sample_config())
        payload = json.loads(buffer.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "nextspyke")
        self.assertEqual(payload["checks"][1]["name"], "db")
        self.assertEqual(payload["checks"][1]["status"], "ok")

    def test_health_db_fail(self):
        buffer = StringIO()
        with patch("nextspyke.health.psycopg.connect", side_effect=Exception("fail")):
            with patch("sys.stdout", buffer):
                code = health_check(sample_config())
        payload = json.loads(buffer.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(payload["status"], "fail")
        self.assertEqual(payload["checks"][1]["status"], "fail")


if __name__ == "__main__":
    unittest.main()
