import unittest
from datetime import date, datetime, timezone

from time_window import TimeWindow


class TimeWindowTest(unittest.TestCase):
    def test_preset_range_and_resolve_bounds(self):
        now = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
        preset = TimeWindow.from_days(30, now=now)
        custom = TimeWindow.from_dates(date(2026, 1, 1), date(2026, 1, 31))
        swapped = TimeWindow.resolve(days=7, start="2026-02-10", end="2026-02-01")

        self.assertEqual(preset.linear_bounds()["after"], "2026-07-01T12:00:00.000Z")
        self.assertEqual(preset.github_merged_qualifier(), "merged:>=2026-07-01")
        self.assertEqual(custom.linear_bounds()["before"], "2026-02-01T00:00:00.000Z")
        self.assertEqual(
            custom.github_merged_qualifier(),
            "merged:>=2026-01-01 merged:<=2026-01-31",
        )
        self.assertEqual(swapped.start.date().isoformat(), "2026-02-01")
        self.assertEqual(
            TimeWindow.resolve(days=7, start="bad", end="2026-02-01", now=now).preset_days,
            7,
        )
        self.assertEqual(
            preset.template_vars()["window_query"],
            {"days": 30},
        )
        self.assertEqual(preset.template_vars()["start"], "2026-07-01")
        self.assertEqual(preset.template_vars()["end"], "2026-07-31")
        self.assertEqual(preset.template_vars()["preset_days"], 30)
        self.assertEqual(
            custom.template_vars()["window_query"],
            {"start": "2026-01-01", "end": "2026-01-31"},
        )
        self.assertEqual(custom.template_vars()["start"], "2026-01-01")
        self.assertEqual(custom.template_vars()["end"], "2026-01-31")
        self.assertIsNone(custom.template_vars()["preset_days"])
