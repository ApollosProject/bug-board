import unittest
from datetime import date, datetime, timezone

from project_dates import (
    completed_project_weeks,
    format_project_start_status,
    format_project_target_status,
    get_project_planned_weeks,
    get_project_schedule_variance_days,
    is_completed_project,
    is_inactive_project,
    is_incomplete_project,
)


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


class ProjectPlannedWeeksTest(unittest.TestCase):
    def test_two_week_and_four_week_projects_use_calendar_span(self):
        two_week = {"startDate": "2026-03-02", "targetDate": "2026-03-15"}
        four_week = {"startDate": "2026-03-02", "targetDate": "2026-03-29"}

        self.assertEqual(get_project_planned_weeks(two_week), 2)
        self.assertEqual(get_project_planned_weeks(four_week), 4)
        self.assertEqual(
            get_project_planned_weeks(two_week) + get_project_planned_weeks(two_week),
            get_project_planned_weeks(four_week),
        )

    def test_missing_dates_count_as_one_week(self):
        self.assertEqual(get_project_planned_weeks({}), 1)
        self.assertEqual(get_project_planned_weeks({"startDate": "2026-03-02"}), 1)
        self.assertEqual(get_project_planned_weeks({"targetDate": "2026-03-15"}), 1)

    def test_inverted_dates_still_count_the_span(self):
        self.assertEqual(
            get_project_planned_weeks({"startDate": "2026-03-15", "targetDate": "2026-03-02"}),
            2,
        )


class ProjectStatusClassificationTest(unittest.TestCase):
    def test_released_project_is_inactive_and_completed(self):
        project = {"status": {"name": "Released"}}

        self.assertTrue(is_inactive_project(project))
        self.assertTrue(is_completed_project(project))
        self.assertFalse(is_incomplete_project(project))

    def test_completed_project_weeks_and_schedule_variance(self):
        early = {
            "status": {"name": "Completed"},
            "completedAt": "2026-03-10",
            "startDate": "2026-03-02",
            "targetDate": "2026-03-15",
        }
        self.assertEqual(completed_project_weeks([early]), 2)
        self.assertEqual(get_project_schedule_variance_days(early), -5)


if __name__ == "__main__":
    unittest.main()
