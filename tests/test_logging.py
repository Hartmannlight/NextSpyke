import json
import sys
import unittest
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke.logging import log_event


class TestLogging(unittest.TestCase):
    def test_log_event_serializes_datetime_extra(self):
        gap_start = datetime(2026, 6, 26, 20, 38, 5, tzinfo=timezone.utc)
        output = StringIO()

        with patch("sys.stdout", output):
            log_event(
                "warn",
                "ingest",
                "Snapshot gap detected",
                event="snapshot_gap",
                extra={"gap_start": gap_start},
            )

        record = json.loads(output.getvalue())
        self.assertEqual(record["gap_start"], "2026-06-26T20:38:05.000Z")


if __name__ == "__main__":
    unittest.main()
