import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import config, db, ingest


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


if __name__ == "__main__":
    unittest.main()
