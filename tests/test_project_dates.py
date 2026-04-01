import unittest
from datetime import date, datetime, timezone

from project_dates import format_project_start_status, format_project_target_status


class ProjectDateStatusFormattingTest(unittest.TestCase):
    def test_target_status_uses_hours_when_less_than_day_left(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)

        days_left, status_text = format_project_target_status(
            date(2026, 4, 1),
            now=now,
        )

        self.assertEqual(days_left, 0)
        self.assertEqual(status_text, "12h left")

    def test_target_status_uses_hours_when_less_than_day_overdue(self):
        now = datetime(2026, 4, 2, 8, 0, tzinfo=timezone.utc)

        days_left, status_text = format_project_target_status(
            date(2026, 4, 1),
            now=now,
        )

        self.assertEqual(days_left, -1)
        self.assertEqual(status_text, "8h overdue")

    def test_start_status_uses_hours_when_less_than_day_away(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)

        starts_in, status_text = format_project_start_status(
            date(2026, 4, 2),
            now=now,
        )

        self.assertEqual(starts_in, 1)
        self.assertEqual(status_text, "starts in 12h")

    def test_target_status_keeps_days_for_longer_windows(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)

        days_left, status_text = format_project_target_status(
            date(2026, 4, 4),
            now=now,
        )

        self.assertEqual(days_left, 3)
        self.assertEqual(status_text, "3d left")


if __name__ == "__main__":
    unittest.main()
