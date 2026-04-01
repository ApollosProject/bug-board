import unittest
from datetime import datetime, timezone

from issue_timing import format_issue_sla_text


class IssueSlaFormattingTest(unittest.TestCase):
    def test_uses_hours_for_sub_day_remaining_sla(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
        issue = {"slaBreachesAt": "2026-04-01T19:00:00.000Z"}

        self.assertEqual(format_issue_sla_text(issue, now=now), "7h")

    def test_uses_hours_for_sub_day_overdue_sla(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
        issue = {"slaBreachesAt": "2026-04-01T09:00:00.000Z"}

        self.assertEqual(format_issue_sla_text(issue, now=now), "3h overdue")

    def test_uses_days_for_longer_remaining_sla(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
        issue = {"slaBreachesAt": "2026-04-03T12:00:00.000Z"}

        self.assertEqual(format_issue_sla_text(issue, now=now), "2d")

    def test_uses_elapsed_days_for_overdue_sla(self):
        now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
        issue = {"slaBreachesAt": "2026-03-31T11:00:00.000Z"}

        self.assertEqual(format_issue_sla_text(issue, now=now), "1d overdue")


if __name__ == "__main__":
    unittest.main()
