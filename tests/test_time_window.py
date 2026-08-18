import unittest
from datetime import date, datetime, timezone

from time_window import TimeWindow, parse_time_window


class TimeWindowTest(unittest.TestCase):
    def test_preset_days_keep_relative_linear_and_github_bounds(self):
        now = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
        window = TimeWindow.from_days(30, now=now)

        self.assertEqual(window.preset_days, 30)
        self.assertEqual(window.duration_days, 30)
        self.assertEqual(window.label, "30d")
        self.assertEqual(window.query_args(), {"days": 30})
        self.assertEqual(window.linear_after(), "-P30D")
        self.assertEqual(window.linear_before(), "2026-07-31T12:00:00.000Z")
        self.assertEqual(window.github_merged_qualifier(), "merged:>=2026-07-01")

    def test_date_range_is_inclusive_and_takes_over_days(self):
        window = TimeWindow.from_dates(date(2026, 1, 1), date(2026, 1, 31))

        self.assertIsNone(window.preset_days)
        self.assertEqual(window.duration_days, 31)
        self.assertEqual(window.label, "Jan 1 – Jan 31, 2026")
        self.assertEqual(
            window.query_args(),
            {"start": "2026-01-01", "end": "2026-01-31"},
        )
        self.assertEqual(window.linear_after(), "2026-01-01T00:00:00.000Z")
        self.assertEqual(window.linear_before(), "2026-02-01T00:00:00.000Z")
        self.assertEqual(
            window.github_merged_qualifier(),
            "merged:>=2026-01-01 merged:<=2026-01-31",
        )

    def test_parse_prefers_date_range_over_days_and_swaps_reversed_dates(self):
        now = datetime(2026, 8, 18, 15, tzinfo=timezone.utc)
        window = parse_time_window(
            {"days": "7", "start": "2026-02-10", "end": "2026-02-01"},
            now=now,
        )

        self.assertEqual(window.start.date().isoformat(), "2026-02-01")
        self.assertEqual(window.inclusive_end_date.isoformat(), "2026-02-10")
        self.assertIsNone(window.preset_days)

    def test_parse_falls_back_to_days_when_dates_are_invalid(self):
        window = parse_time_window({"days": "7", "start": "not-a-date"}, now=datetime.now())
        self.assertEqual(window.preset_days, 7)

    def test_open_ended_start_uses_today_as_the_end(self):
        now = datetime(2026, 8, 18, 15, tzinfo=timezone.utc)
        window = parse_time_window({"start": "2026-08-01"}, now=now)
        self.assertEqual(window.start.date().isoformat(), "2026-08-01")
        self.assertEqual(window.inclusive_end_date.isoformat(), "2026-08-18")


if __name__ == "__main__":
    unittest.main()
