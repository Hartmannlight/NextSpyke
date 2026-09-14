import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from scripts.compact_history import compact_day


class TestMaintenanceBounds(unittest.TestCase):
    def test_current_day_requires_offline_opt_in(self):
        today = datetime.now(timezone.utc).date()
        conn = Mock()
        with self.assertRaisesRegex(ValueError, "completed UTC days"):
            compact_day(conn, today, "fg")
        with self.assertRaisesRegex(ValueError, "closed for at least five minutes"):
            compact_day(conn, today, "fg", online=True, allow_current_day=True)
        conn.cursor.assert_not_called()

    def test_future_day_is_never_allowed(self):
        tomorrow = datetime.now(timezone.utc).date() + timedelta(days=1)
        conn = Mock()
        with self.assertRaisesRegex(ValueError, "completed UTC days"):
            compact_day(conn, tomorrow, "fg", allow_current_day=True)
        conn.cursor.assert_not_called()
