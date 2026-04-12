import sys
import types
import unittest
from datetime import datetime
from typing import Any, cast
from unittest.mock import patch


def _install_import_shims() -> None:
    dotenv_module = cast(Any, types.ModuleType("dotenv"))
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_module)

    requests_module = cast(Any, types.ModuleType("requests"))
    requests_module.post = lambda *args, **kwargs: None
    sys.modules.setdefault("requests", requests_module)

    schedule_module = cast(Any, types.ModuleType("schedule"))
    schedule_module.every = lambda *args, **kwargs: None
    schedule_module.run_pending = lambda: None
    sys.modules.setdefault("schedule", schedule_module)

    tenacity_module = cast(Any, types.ModuleType("tenacity"))
    tenacity_module.before_sleep_log = lambda *args, **kwargs: None
    tenacity_module.retry = lambda *args, **kwargs: (lambda func: func)
    tenacity_module.stop_after_attempt = lambda count: count
    tenacity_module.wait_fixed = lambda seconds: seconds
    sys.modules.setdefault("tenacity", tenacity_module)

    config_module = cast(Any, types.ModuleType("config"))
    config_module.load_config = lambda: {"people": {}}
    sys.modules.setdefault("config", config_module)

    constants_module = cast(Any, types.ModuleType("constants"))
    constants_module.ENGINEERING_TEAM_SLUG = "engineering"
    constants_module.PRIORITY_TO_SCORE = {}
    sys.modules.setdefault("constants", constants_module)

    fleet_health_module = cast(Any, types.ModuleType("fleet_health_cache"))
    fleet_health_module.refresh_fleet_health_cache = lambda: ({}, 200)
    fleet_health_module.should_use_redis_cache = lambda: False
    sys.modules.setdefault("fleet_health_cache", fleet_health_module)

    github_module = cast(Any, types.ModuleType("github"))
    github_module.get_pr_diff = lambda *args, **kwargs: ""
    github_module.get_prs_waiting_for_review_by_reviewer = lambda *args, **kwargs: {}
    github_module.get_prs_with_changes_requested_by_reviewer = (
        lambda *args, **kwargs: {}
    )
    github_module.merged_prs_by_author = lambda *args, **kwargs: {}
    github_module.merged_prs_by_reviewer = lambda *args, **kwargs: {}
    sys.modules.setdefault("github", github_module)

    leaderboard_module = cast(Any, types.ModuleType("leaderboard"))
    leaderboard_module.calculate_cycle_project_lead_points = lambda *args, **kwargs: 0
    leaderboard_module.calculate_cycle_project_member_points = lambda *args, **kwargs: 0
    sys.modules.setdefault("leaderboard", leaderboard_module)

    linear_package = cast(Any, types.ModuleType("linear"))
    linear_package.__path__ = []
    sys.modules.setdefault("linear", linear_package)

    linear_issues_module = cast(Any, types.ModuleType("linear.issues"))
    linear_issues_module.get_completed_issues = lambda *args, **kwargs: []
    linear_issues_module.get_completed_issues_for_person = lambda *args, **kwargs: []
    linear_issues_module.get_open_issues = lambda *args, **kwargs: []
    linear_issues_module.get_open_issues_in_projects = lambda *args, **kwargs: []
    linear_issues_module.get_stale_issues_by_assignee = lambda *args, **kwargs: {}
    sys.modules.setdefault("linear.issues", linear_issues_module)

    linear_projects_module = cast(Any, types.ModuleType("linear.projects"))
    linear_projects_module.get_projects = lambda *args, **kwargs: []
    sys.modules.setdefault("linear.projects", linear_projects_module)

    openai_module = cast(Any, types.ModuleType("openai_client"))
    openai_module.get_chat_function_call = lambda *args, **kwargs: {}
    sys.modules.setdefault("openai_client", openai_module)

    support_module = cast(Any, types.ModuleType("support"))
    support_module.get_support_slugs = lambda: []
    sys.modules.setdefault("support", support_module)


_install_import_shims()

import jobs as jobs_module  # noqa: E402

for module_name in [
    "config",
    "constants",
    "fleet_health_cache",
    "github",
    "leaderboard",
    "linear",
    "linear.issues",
    "linear.projects",
    "openai_client",
    "support",
]:
    sys.modules.pop(module_name, None)


class _FakeScheduledJob:
    def __init__(self, recorder, interval=None):
        self._recorder = recorder
        self.interval = interval
        self.unit = None
        self.at_time = None
        self.timezone = None

    @property
    def day(self):
        self.unit = "day"
        return self

    @property
    def days(self):
        self.unit = "day"
        return self

    @property
    def friday(self):
        self.unit = "friday"
        return self

    @property
    def monday(self):
        self.unit = "monday"
        return self

    @property
    def seconds(self):
        self.unit = "seconds"
        return self

    def at(self, at_time, timezone=None):
        self.at_time = at_time
        self.timezone = timezone
        return self

    def do(self, func):
        self._recorder.append(
            {
                "interval": self.interval,
                "unit": self.unit,
                "at_time": self.at_time,
                "timezone": self.timezone,
                "func": func,
            }
        )
        return self


class ConfigureScheduledJobsTest(unittest.TestCase):
    def test_registers_inactive_engineers_friday_job(self):
        recorded_jobs = []

        def fake_every(interval=None):
            return _FakeScheduledJob(recorded_jobs, interval=interval)

        with patch.object(jobs_module, "should_use_redis_cache", return_value=False):
            with patch.object(jobs_module.schedule, "every", side_effect=fake_every):
                jobs_module.configure_scheduled_jobs()

        self.assertIn(
            {
                "interval": None,
                "unit": "friday",
                "at_time": "13:00",
                "timezone": None,
                "func": jobs_module.post_inactive_engineers,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "friday",
                "at_time": "20:00",
                "timezone": None,
                "func": jobs_module.post_leaderboard,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "day",
                "at_time": "14:00",
                "timezone": None,
                "func": jobs_module.post_stale,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "day",
                "at_time": "14:00",
                "timezone": "America/New_York",
                "func": jobs_module.post_overdue_projects,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "friday",
                "at_time": "12:00",
                "timezone": None,
                "func": jobs_module.post_upcoming_projects,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "monday",
                "at_time": "12:00",
                "timezone": None,
                "func": jobs_module.post_friday_deadlines,
            },
            recorded_jobs,
        )


class RunDebugJobsTest(unittest.TestCase):
    def test_runs_leaderboard_overdue_projects_upcoming_projects_and_friday_deadlines(self):
        with patch.object(jobs_module, "should_use_redis_cache", return_value=False):
            with patch.object(jobs_module, "post_priority_bugs"):
                with patch.object(jobs_module, "post_leaderboard") as leaderboard:
                    with patch.object(jobs_module, "post_stale") as stale:
                        with patch.object(
                            jobs_module, "post_overdue_projects"
                        ) as overdue:
                            with patch.object(
                                jobs_module, "post_upcoming_projects"
                            ) as upcoming:
                                with patch.object(
                                    jobs_module, "post_friday_deadlines"
                                ) as friday_deadlines:
                                    jobs_module.run_debug_jobs()

        leaderboard.assert_called_once_with()
        stale.assert_called_once_with()
        overdue.assert_called_once_with()
        upcoming.assert_called_once_with()
        friday_deadlines.assert_called_once_with()


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return datetime(2026, 3, 15, 12, 0, 0)
        return datetime(2026, 3, 15, 12, 0, 0, tzinfo=tz)


class PlatformWhitelistMatchingTest(unittest.TestCase):
    def test_matches_when_platform_whitelist_and_bug_platform_normalize_to_same_value(self):
        person = {"platform_whitelist": ["  Mobile Web  ", "Roku"]}
        bugs = [
            {"platform": "mobile-web"},
            {"platform": "Admin"},
        ]

        self.assertTrue(
            jobs_module._person_matches_any_unassigned_platform(person, bugs)
        )

    def test_returns_false_when_platform_whitelist_has_no_valid_entries(self):
        person = {"platform_whitelist": ["   ", None]}
        bugs = [
            {"platform": "Web"},
            {"platform": None},
        ]

        self.assertFalse(
            jobs_module._person_matches_any_unassigned_platform(person, bugs)
        )


class PostPriorityBugsTest(unittest.TestCase):
    def test_uses_linear_sla_windows_for_at_risk_and_overdue(self):
        posted = []
        bugs = [
            {
                "id": "breached-bug",
                "title": "Breached bug",
                "assignee": {"displayName": "Alex"},
                "url": "https://linear.app/issue/breached-bug",
                "platform": "Mobile",
                "daysOpen": 1,
                "priority": 1,
                "slaMediumRiskAt": "2026-03-15T09:00:00.000Z",
                "slaHighRiskAt": "2026-03-15T10:00:00.000Z",
                "slaBreachesAt": "2026-03-15T11:00:00.000Z",
            },
            {
                "id": "risk-bug",
                "title": "Risk bug",
                "assignee": {"displayName": "Taylor"},
                "url": "https://linear.app/issue/risk-bug",
                "platform": "Web",
                "daysOpen": 2,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-15T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-15T11:00:00.000Z",
                "slaBreachesAt": "2026-03-16T12:00:00.000Z",
            },
            {
                "id": "old-bug",
                "title": "Old bug",
                "assignee": {"displayName": "Jordan"},
                "url": "https://linear.app/issue/old-bug",
                "platform": "API",
                "daysOpen": 30,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-16T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-16T10:00:00.000Z",
                "slaBreachesAt": "2026-03-17T12:00:00.000Z",
            },
            {
                "id": "no-sla-bug",
                "title": "No SLA bug",
                "assignee": None,
                "url": "https://linear.app/issue/no-sla-bug",
                "platform": "Admin",
                "daysOpen": 45,
                "priority": 1,
                "slaType": None,
                "slaStartedAt": None,
                "slaMediumRiskAt": None,
                "slaHighRiskAt": None,
                "slaBreachesAt": None,
            },
        ]

        with patch.object(
            jobs_module, "load_config", return_value={"people": {}, "platforms": {}}
        ):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(
                    jobs_module, "post_to_slack", side_effect=posted.append
                ):
                    with patch.object(jobs_module, "datetime", FixedDateTime):
                        jobs_module.post_priority_bugs()

        self.assertEqual(len(posted), 1)
        self.assertIn("*At Risk*", posted[0])
        self.assertIn("*Overdue*", posted[0])
        self.assertIn("Risk bug", posted[0])
        self.assertIn("Breached bug", posted[0])
        self.assertIn("(1d, Web, No Assignee)", posted[0])
        self.assertIn("(1h overdue, Mobile, No Assignee)", posted[0])
        self.assertIn("(1d, Web, No Assignee)\n\n*Overdue*", posted[0])
        self.assertNotIn("\n\n\n\n*Overdue*", posted[0])
        self.assertNotIn("Old bug", posted[0])
        self.assertNotIn("No SLA bug", posted[0])

    def test_uses_single_blank_line_between_priority_bug_sections(self):
        posted = []
        bugs = [
            {
                "id": "unassigned-bug",
                "title": "Unassigned bug",
                "assignee": None,
                "url": "https://linear.app/issue/unassigned-bug",
                "platform": "Mobile",
                "daysOpen": 14,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-16T10:00:00.000Z",
                "slaBreachesAt": "2026-03-17T12:00:00.000Z",
            },
            {
                "id": "risk-bug",
                "title": "Risk bug",
                "assignee": {"displayName": "Taylor"},
                "url": "https://linear.app/issue/risk-bug",
                "platform": "Web",
                "daysOpen": 1,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-15T11:00:00.000Z",
                "slaBreachesAt": "2026-03-16T12:00:00.000Z",
            },
            {
                "id": "overdue-bug",
                "title": "Overdue bug",
                "assignee": {"displayName": "Jordan"},
                "url": "https://linear.app/issue/overdue-bug",
                "platform": "API",
                "daysOpen": 0,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-15T09:00:00.000Z",
                "slaBreachesAt": "2026-03-15T11:00:00.000Z",
            },
        ]

        with patch.object(
            jobs_module, "load_config", return_value={"people": {}, "platforms": {}}
        ):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(
                    jobs_module, "post_to_slack", side_effect=posted.append
                ):
                    with patch.object(jobs_module, "datetime", FixedDateTime):
                        jobs_module.post_priority_bugs()

        self.assertEqual(len(posted), 1)
        self.assertIn("Unassigned bug", posted[0])
        self.assertIn("Risk bug", posted[0])
        self.assertIn("Overdue bug", posted[0])
        self.assertIn("Mobile)\n\n*At Risk*", posted[0])
        self.assertIn("No Assignee)\n\n*Overdue*", posted[0])
        self.assertNotIn("\n\n\n\n*At Risk*", posted[0])
        self.assertNotIn("\n\n\n\n*Overdue*", posted[0])

    def test_unassigned_priority_bug_notifies_all_support_people_without_platform_leads(self):
        posted = []
        bugs = [
            {
                "id": "unassigned-bug",
                "title": "Unassigned bug",
                "assignee": None,
                "url": "https://linear.app/issue/unassigned-bug",
                "platform": "Mobile",
                "daysOpen": 14,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-16T10:00:00.000Z",
                "slaBreachesAt": "2026-03-17T12:00:00.000Z",
            },
            {
                "id": "assigned-bug",
                "title": "Assigned bug",
                "assignee": {"displayName": "Alex"},
                "url": "https://linear.app/issue/assigned-bug",
                "platform": "Web",
                "daysOpen": 3,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-16T10:00:00.000Z",
                "slaBreachesAt": "2026-03-17T12:00:00.000Z",
            },
        ]
        config = {
            "people": {
                "alex": {"linear_username": "Alex", "slack_id": "U1"},
                "blair": {"linear_username": "Blair", "slack_id": "U2"},
                "casey": {"linear_username": "Casey", "slack_id": "U3"},
                "devon": {"linear_username": "Devon", "slack_id": "U4"},
            },
            "platforms": {
                "mobile": {"lead": "alex", "developers": ["blair", "casey"]},
                "web": {"lead": "casey", "developers": ["devon"]},
            },
        }

        with patch.object(jobs_module, "load_config", return_value=config):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(
                    jobs_module,
                    "get_support_slugs",
                    return_value={"alex", "blair", "casey", "devon"},
                ):
                    with patch.object(
                        jobs_module, "post_to_slack", side_effect=posted.append
                    ):
                        with patch.object(jobs_module, "datetime", FixedDateTime):
                            jobs_module.post_priority_bugs()

        self.assertEqual(len(posted), 1)
        self.assertIn("attn:\n\n<@U2>\n<@U3>\n<@U4>", posted[0])
        self.assertNotIn("<@U1>", posted[0])
        self.assertNotIn("Lead)", posted[0])

    def test_unassigned_priority_bug_only_notifies_matching_platform_whitelists(self):
        posted = []
        bugs = [
            {
                "id": "unassigned-bug",
                "title": "Unassigned mobile bug",
                "assignee": None,
                "url": "https://linear.app/issue/unassigned-bug",
                "platform": "Mobile",
                "daysOpen": 14,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-16T10:00:00.000Z",
                "slaBreachesAt": "2026-03-17T12:00:00.000Z",
            },
            {
                "id": "assigned-bug",
                "title": "Assigned bug",
                "assignee": {"displayName": "Alex"},
                "url": "https://linear.app/issue/assigned-bug",
                "platform": "Web",
                "daysOpen": 3,
                "priority": 2,
                "slaMediumRiskAt": "2026-03-14T08:00:00.000Z",
                "slaHighRiskAt": "2026-03-16T10:00:00.000Z",
                "slaBreachesAt": "2026-03-17T12:00:00.000Z",
            },
        ]
        config = {
            "people": {
                "alex": {"linear_username": "Alex", "slack_id": "U1"},
                "blair": {
                    "linear_username": "Blair",
                    "slack_id": "U2",
                    "platform_whitelist": ["mobile"],
                },
                "casey": {
                    "linear_username": "Casey",
                    "slack_id": "U3",
                    "platform_whitelist": ["web"],
                },
                "devon": {"linear_username": "Devon", "slack_id": "U4"},
            },
            "platforms": {},
        }

        with patch.object(jobs_module, "load_config", return_value=config):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(
                    jobs_module,
                    "get_support_slugs",
                    return_value={"alex", "blair", "casey", "devon"},
                ):
                    with patch.object(
                        jobs_module, "post_to_slack", side_effect=posted.append
                    ):
                        with patch.object(jobs_module, "datetime", FixedDateTime):
                            jobs_module.post_priority_bugs()

        self.assertEqual(len(posted), 1)
        self.assertIn("attn:\n\n<@U2>\n<@U4>", posted[0])
        self.assertNotIn("<@U1>", posted[0])
        self.assertNotIn("<@U3>", posted[0])


class PostOverdueProjectsTest(unittest.TestCase):
    def test_posts_only_engineering_led_active_projects_with_past_target_dates(self):
        posted = []
        projects = [
            {
                "name": "Late Alpha",
                "url": "https://linear.app/project/late-alpha",
                "targetDate": "2026-03-14",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            {
                "name": "Late Beta",
                "url": "https://linear.app/project/late-beta",
                "targetDate": "2026-03-11",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Pat"},
            },
            {
                "name": "Late Gamma",
                "url": "https://linear.app/project/late-gamma",
                "targetDate": "2026-03-13",
                "status": {"name": "Active"},
                "lead": None,
            },
            {
                "name": "Due Today",
                "url": "https://linear.app/project/due-today",
                "targetDate": "2026-03-15",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            {
                "name": "Completed Late",
                "url": "https://linear.app/project/completed-late",
                "targetDate": "2026-03-10",
                "status": {"name": "Completed"},
                "lead": {"displayName": "Alex"},
            },
            {
                "name": "Released Late",
                "url": "https://linear.app/project/released-late",
                "targetDate": "2026-03-09",
                "status": {"name": "Released"},
                "lead": {"displayName": "Alex"},
            },
        ]
        config = {
            "people": {
                "alex": {
                    "linear_username": "Alex",
                    "slack_id": "U1",
                    "team": "engineering",
                },
                "pat": {
                    "linear_username": "Pat",
                    "slack_id": "U2",
                    "team": "product",
                },
            }
        }

        with patch.object(jobs_module, "load_config", return_value=config):
            with patch.object(jobs_module, "get_projects", return_value=projects):
                with patch.object(
                    jobs_module, "post_to_slack", side_effect=posted.append
                ):
                    with patch.object(jobs_module, "datetime", FixedDateTime):
                        jobs_module.post_overdue_projects()

        self.assertEqual(len(posted), 1)
        self.assertIn("*Overdue Projects*", posted[0])
        self.assertIn("Late Alpha", posted[0])
        self.assertIn("12h overdue - Lead: <@U1>", posted[0])
        self.assertNotIn("Late Beta", posted[0])
        self.assertNotIn("Late Gamma", posted[0])
        self.assertNotIn("Due Today", posted[0])
        self.assertNotIn("Completed Late", posted[0])
        self.assertNotIn("Released Late", posted[0])


class PostUpcomingProjectsTest(unittest.TestCase):
    def test_posts_only_monday_projects_with_engineering_leads(self):
        posted = []
        projects = [
            {
                "name": "Monday Engineering",
                "url": "https://linear.app/project/monday-engineering",
                "startDate": "2026-03-16",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Alex"},
            },
            {
                "name": "Monday Product",
                "url": "https://linear.app/project/monday-product",
                "startDate": "2026-03-16",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Pat"},
            },
            {
                "name": "Monday No Lead",
                "url": "https://linear.app/project/monday-no-lead",
                "startDate": "2026-03-16",
                "status": {"name": "Planned"},
                "lead": None,
            },
            {
                "name": "Canceled Monday",
                "url": "https://linear.app/project/canceled-monday",
                "startDate": "2026-03-16",
                "status": {"name": "Canceled"},
                "lead": {"displayName": "Alex"},
            },
        ]
        config = {
            "people": {
                "alex": {
                    "linear_username": "Alex",
                    "slack_id": "U1",
                    "team": "engineering",
                },
                "pat": {
                    "linear_username": "Pat",
                    "slack_id": "U2",
                    "team": "product",
                },
            }
        }

        with patch.object(jobs_module, "load_config", return_value=config):
            with patch.object(jobs_module, "get_projects", return_value=projects):
                with patch.object(
                    jobs_module, "post_to_slack", side_effect=posted.append
                ):
                    with patch.object(jobs_module, "datetime", FixedDateTime):
                        jobs_module.post_upcoming_projects()

        self.assertEqual(len(posted), 1)
        self.assertIn("*Projects Starting Monday*", posted[0])
        self.assertIn("Monday Engineering", posted[0])
        self.assertIn("Lead: <@U1>", posted[0])
        self.assertNotIn("Monday Product", posted[0])
        self.assertNotIn("Monday No Lead", posted[0])
        self.assertNotIn("Canceled Monday", posted[0])


if __name__ == "__main__":
    unittest.main()
