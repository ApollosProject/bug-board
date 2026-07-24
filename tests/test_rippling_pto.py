import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

import app as app_module
import rippling_pto
from rippling_pto import PTOCalendar, PTOEvent

CALENDAR_WITH_ALL_DAY_AND_TIMED_EVENTS = b"""BEGIN:VCALENDAR\r
VERSION:2.0\r
BEGIN:VEVENT\r
UID:all-day-1\r
SUMMARY:Bra\r
 ndon paid time off\r
DESCRIPTION:Synthetic private detail\r
DTSTART;VALUE=DATE:20260727\r
DTEND;VALUE=DATE:20260730\r
END:VEVENT\r
BEGIN:VEVENT\r
UID:timed-1\r
SUMMARY:Dylan partial time off\r
DTSTART;TZID=UTC;VALUE=DATE-TIME:20260731T010000Z\r
DTEND;TZID=UTC;VALUE=DATE-TIME:20260731T030000Z\r
END:VEVENT\r
BEGIN:VEVENT\r
UID:cancelled-1\r
SUMMARY:Brandon cancelled time off\r
DTSTART;VALUE=DATE:20260803\r
DTEND;VALUE=DATE:20260804\r
STATUS:CANCELLED\r
END:VEVENT\r
END:VCALENDAR\r
"""


class RipplingPTOCalendarParsingTest(unittest.TestCase):
    def test_parses_all_day_and_timed_events_without_descriptions(self):
        events = rippling_pto.parse_icalendar(CALENDAR_WITH_ALL_DAY_AND_TIMED_EVENTS)

        self.assertEqual(
            events,
            (
                PTOEvent(
                    summary="Brandon paid time off",
                    start=date(2026, 7, 27),
                    end=date(2026, 7, 29),
                    is_all_day=True,
                ),
                PTOEvent(
                    summary="Dylan partial time off",
                    start=date(2026, 7, 30),
                    end=date(2026, 7, 30),
                    is_all_day=False,
                ),
            ),
        )

    def test_rejects_non_calendar_responses(self):
        with self.assertRaisesRegex(ValueError, "not an iCalendar"):
            rippling_pto.parse_icalendar(b"<html>Sign in</html>")


class RipplingPTOCalendarFetchTest(unittest.TestCase):
    def test_missing_configuration_does_not_make_a_request(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(rippling_pto.requests, "get") as get_mock:
                calendar = rippling_pto.get_rippling_pto_calendar()

        self.assertEqual(calendar, PTOCalendar(configured=False, available=False))
        get_mock.assert_not_called()

    def test_fetches_webcal_subscription_over_https(self):
        calendar_url = (
            "webcal://app.rippling.com/api/feed/calendar/pto/all-reports/"
            "calendar-id/private-token/calendar.ics?company=company-id"
        )
        response = Mock(status_code=200, content=CALENDAR_WITH_ALL_DAY_AND_TIMED_EVENTS)

        with patch.dict(
            os.environ,
            {"RIPPLING_PTO_CALENDAR_URL": calendar_url},
            clear=True,
        ):
            with patch.object(rippling_pto.requests, "get", return_value=response) as get_mock:
                calendar = rippling_pto.get_rippling_pto_calendar()

        self.assertTrue(calendar.configured)
        self.assertTrue(calendar.available)
        self.assertEqual(len(calendar.events), 2)
        get_mock.assert_called_once_with(
            (
                "https://app.rippling.com/api/feed/calendar/pto/all-reports/"
                "calendar-id/private-token/calendar.ics?company=company-id"
            ),
            timeout=rippling_pto.RIPPLING_PTO_TIMEOUT_SECONDS,
            allow_redirects=False,
        )

    def test_rejects_non_rippling_calendar_urls(self):
        with patch.dict(
            os.environ,
            {"RIPPLING_PTO_CALENDAR_URL": "https://example.com/private/calendar.ics"},
            clear=True,
        ):
            with patch.object(rippling_pto.requests, "get") as get_mock:
                with self.assertLogs("rippling_pto", level="WARNING"):
                    calendar = rippling_pto.get_rippling_pto_calendar()

        self.assertEqual(calendar, PTOCalendar(configured=True, available=False))
        get_mock.assert_not_called()

    def test_failure_log_does_not_expose_calendar_credentials(self):
        calendar_url = (
            "webcal://app.rippling.com/api/feed/calendar/pto/all-reports/"
            "calendar-id/private-token/calendar.ics?company=private-company"
        )
        response = Mock(status_code=500, content=b"")

        with patch.dict(
            os.environ,
            {"RIPPLING_PTO_CALENDAR_URL": calendar_url},
            clear=True,
        ):
            with patch.object(rippling_pto.requests, "get", return_value=response):
                with self.assertLogs("rippling_pto", level="WARNING") as logs:
                    calendar = rippling_pto.get_rippling_pto_calendar()

        self.assertEqual(calendar, PTOCalendar(configured=True, available=False))
        combined_logs = "\n".join(logs.output)
        self.assertNotIn("private-token", combined_logs)
        self.assertNotIn("private-company", combined_logs)


class ProjectTimelineOOOTest(unittest.TestCase):
    def setUp(self):
        app_module._build_team_context.cache_clear()

    def tearDown(self):
        app_module._build_team_context.cache_clear()

    def test_ooo_shares_timeline_lanes_without_exposing_source_summary(self):
        config = {
            "people": {
                "brandon": {
                    "team": "engineering",
                    "linear_username": "brandon",
                }
            },
            "platforms": {},
        }
        active_project = {
            "id": "project-1",
            "name": "Project Alpha",
            "url": "https://linear.example/project/alpha",
            "health": "onTrack",
            "status": {"name": "Active"},
            "completedAt": None,
            "startDate": "2026-07-20",
            "targetDate": "2026-08-01",
            "lead": {"displayName": "Brandon"},
            "initiatives": {"nodes": [{"id": "initiative-1", "name": "Cycle"}]},
            "members": [],
        }
        calendar = PTOCalendar(
            configured=True,
            available=True,
            events=(
                PTOEvent(
                    summary="Brandon confidential source summary",
                    start=date(2026, 7, 27),
                    end=date(2026, 7, 29),
                    is_all_day=True,
                ),
                PTOEvent(
                    summary="Someone Else time off",
                    start=date(2026, 7, 27),
                    end=date(2026, 7, 27),
                    is_all_day=True,
                ),
            ),
        )

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)

        with (
            patch.object(app_module, "datetime", FixedDateTime),
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_projects", return_value=[active_project]),
            patch.object(app_module, "get_rippling_pto_calendar", return_value=calendar),
        ):
            context = app_module._build_team_context(1)

        timeline = context["project_timeline"]
        developer = timeline["rows"][0]
        self.assertTrue(timeline["ooo_available"])
        self.assertEqual(developer["lane_count"], 2)
        self.assertEqual(len(developer["ooo_events"]), 1)
        self.assertEqual(
            (
                developer["ooo_events"][0]["start_day"],
                developer["ooo_events"][0]["span_days"],
                developer["ooo_events"][0]["lane"],
            ),
            (8, 3, 2),
        )
        self.assertNotIn(
            "confidential source summary",
            repr(developer["ooo_events"]),
        )
        with app_module.app.test_request_context():
            rendered = app_module.render_template("partials/team_content.html", **context)
        self.assertIn("OOO from Rippling", rendered)
        self.assertIn("project-timeline-bar--ooo", rendered)
        self.assertNotIn("confidential source summary", rendered)

    def test_duplicate_first_names_require_a_full_name_match(self):
        config = {
            "people": {
                "john_alpha": {
                    "team": "engineering",
                    "linear_username": "john.alpha",
                },
                "john_beta": {
                    "team": "engineering",
                    "linear_username": "john.beta",
                },
            },
            "platforms": {},
        }
        calendar = PTOCalendar(
            configured=True,
            available=True,
            events=(
                PTOEvent(
                    summary="John time off",
                    start=date(2026, 7, 27),
                    end=date(2026, 7, 27),
                    is_all_day=True,
                ),
                PTOEvent(
                    summary="John Alpha time off",
                    start=date(2026, 7, 28),
                    end=date(2026, 7, 28),
                    is_all_day=True,
                ),
            ),
        )

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)

        with (
            patch.object(app_module, "datetime", FixedDateTime),
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_projects", return_value=[]),
            patch.object(app_module, "get_rippling_pto_calendar", return_value=calendar),
        ):
            context = app_module._build_team_context(2)

        rows_by_slug = {
            developer["slug"]: developer for developer in context["project_timeline"]["rows"]
        }
        self.assertEqual(len(rows_by_slug["john_alpha"]["ooo_events"]), 1)
        self.assertEqual(rows_by_slug["john_beta"]["ooo_events"], [])


if __name__ == "__main__":
    unittest.main()
