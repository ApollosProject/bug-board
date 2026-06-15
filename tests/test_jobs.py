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
    requests_module.get = lambda *args, **kwargs: None
    sys.modules.setdefault("requests", requests_module)

    schedule_module = cast(Any, types.ModuleType("schedule"))
    schedule_module.every = lambda *args, **kwargs: None
    schedule_module.run_pending = lambda: None
    sys.modules.setdefault("schedule", schedule_module)

    tenacity_module = cast(Any, types.ModuleType("tenacity"))
    tenacity_module.before_sleep_log = lambda *args, **kwargs: None
    tenacity_module.retry = lambda *args, **kwargs: lambda func: func
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
    linear_issues_module.get_open_stale_issues = lambda *args, **kwargs: []
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
        self.assertNotIn(
            {
                "interval": None,
                "unit": "friday",
                "at_time": "16:00",
                "timezone": "America/New_York",
                "func": jobs_module.post_leaderboard,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "day",
                "at_time": "10:00",
                "timezone": "America/New_York",
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
                "func": jobs_module.post_project_updates,
            },
            recorded_jobs,
        )


class RunDebugJobsTest(unittest.TestCase):
    def test_runs_leaderboard_stale_and_project_updates(self):
        with patch.object(jobs_module, "should_use_redis_cache", return_value=False):
            with patch.object(jobs_module, "post_inactive_engineers"):
                with patch.object(jobs_module, "post_priority_bugs"):
                    with patch.object(jobs_module, "post_leaderboard") as leaderboard:
                        with patch.object(jobs_module, "post_stale") as stale:
                            with patch.object(
                                jobs_module, "post_project_updates"
                            ) as project_updates:
                                jobs_module.run_debug_jobs()

        leaderboard.assert_called_once_with()
        stale.assert_called_once_with()
        project_updates.assert_called_once_with()


class PostStaleTest(unittest.TestCase):
    def test_uses_open_stale_issues_without_label_or_priority_queries(self):
        open_issues = [{"id": "APO-7555"}]

        def fake_get_stale_issues(issues, days):
            self.assertIs(issues, open_issues)
            self.assertEqual(days, 7)
            return {
                "dylan": [
                    {
                        "title": "Regression in Apple Pay campus/fund confirmation flow",
                        "url": "https://linear.app/differential/issue/APO-7555",
                        "daysStale": 74,
                        "priority": 0,
                        "platform": None,
                    }
                ]
            }

        with patch.object(
            jobs_module,
            "get_team_members",
            return_value={"dylan": {"linear_username": "dylan", "slack_id": "U03LD9MJLNP"}},
        ):
            with patch.object(
                jobs_module, "get_prs_waiting_for_review_by_reviewer", return_value={}
            ):
                with patch.object(
                    jobs_module,
                    "get_open_issues",
                    side_effect=AssertionError("post_stale should use get_open_stale_issues"),
                ):
                    with patch.object(
                        jobs_module, "get_open_stale_issues", return_value=open_issues
                    ):
                        with patch.object(
                            jobs_module,
                            "get_stale_issues_by_assignee",
                            side_effect=fake_get_stale_issues,
                        ):
                            with patch.dict(
                                jobs_module.os.environ,
                                {"APP_URL": "https://bug-board.example"},
                                clear=False,
                            ):
                                with patch.object(jobs_module, "post_to_slack") as post:
                                    jobs_module.post_stale()

        post.assert_called_once()
        message = post.call_args.args[0]
        self.assertIn("*Stale Open Issues*", message)
        self.assertIn("APO-7555", message)
        self.assertIn("(74d)", message)


class AirflowFleetHeartbeatTest(unittest.TestCase):
    def setUp(self):
        jobs_module._airflow_fleet_unknown_heartbeat_failures = 0

    def test_reports_success_for_healthy_payload(self):
        with patch.dict(
            jobs_module.os.environ,
            {"AIRFLOW_FLEET_HEARTBEAT_URL": "https://uptime.betterstack.com/heartbeat/token"},
            clear=False,
        ):
            with patch.object(jobs_module.requests, "get") as get_mock:
                get_mock.return_value.status_code = 200
                jobs_module.report_airflow_fleet_health_heartbeat({"status": "healthy"}, 200)

        get_mock.assert_called_once_with(
            "https://uptime.betterstack.com/heartbeat/token",
            timeout=jobs_module.AIRFLOW_FLEET_HEARTBEAT_TIMEOUT_SECONDS,
        )

    def test_reports_failure_immediately_for_degraded_payload(self):
        with patch.dict(
            jobs_module.os.environ,
            {"AIRFLOW_FLEET_HEARTBEAT_URL": "https://uptime.betterstack.com/heartbeat/token/"},
            clear=False,
        ):
            with patch.object(jobs_module.requests, "get") as get_mock:
                get_mock.return_value.status_code = 200
                jobs_module.report_airflow_fleet_health_heartbeat({"status": "degraded"}, 503)

        get_mock.assert_called_once_with(
            "https://uptime.betterstack.com/heartbeat/token/fail",
            timeout=jobs_module.AIRFLOW_FLEET_HEARTBEAT_TIMEOUT_SECONDS,
        )

    def test_suppresses_transient_unknown_payloads_until_threshold(self):
        with patch.dict(
            jobs_module.os.environ,
            {"AIRFLOW_FLEET_HEARTBEAT_URL": "https://uptime.betterstack.com/heartbeat/token"},
            clear=False,
        ):
            with patch.object(jobs_module.requests, "get") as get_mock:
                get_mock.return_value.status_code = 200

                jobs_module.report_airflow_fleet_health_heartbeat({"status": "unknown"}, 503)
                jobs_module.report_airflow_fleet_health_heartbeat({"status": "unknown"}, 503)
                jobs_module.report_airflow_fleet_health_heartbeat({"status": "unknown"}, 503)

        self.assertEqual(
            [call.args[0] for call in get_mock.call_args_list],
            [
                "https://uptime.betterstack.com/heartbeat/token",
                "https://uptime.betterstack.com/heartbeat/token",
                "https://uptime.betterstack.com/heartbeat/token/fail",
            ],
        )

    def test_healthy_payload_resets_unknown_failure_count(self):
        jobs_module._airflow_fleet_unknown_heartbeat_failures = 2

        with patch.dict(
            jobs_module.os.environ,
            {"AIRFLOW_FLEET_HEARTBEAT_URL": "https://uptime.betterstack.com/heartbeat/token"},
            clear=False,
        ):
            with patch.object(jobs_module.requests, "get") as get_mock:
                get_mock.return_value.status_code = 200
                jobs_module.report_airflow_fleet_health_heartbeat({"status": "healthy"}, 200)

        self.assertEqual(jobs_module._airflow_fleet_unknown_heartbeat_failures, 0)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return datetime(2026, 3, 15, 12, 0, 0)
        return datetime(2026, 3, 15, 12, 0, 0, tzinfo=tz)


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

        with patch.object(jobs_module, "load_config", return_value={"people": {}, "platforms": {}}):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(jobs_module, "post_to_slack", side_effect=posted.append):
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

        with patch.object(jobs_module, "load_config", return_value={"people": {}, "platforms": {}}):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(jobs_module, "post_to_slack", side_effect=posted.append):
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
                    with patch.object(jobs_module, "post_to_slack", side_effect=posted.append):
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
                    with patch.object(jobs_module, "post_to_slack", side_effect=posted.append):
                        with patch.object(jobs_module, "datetime", FixedDateTime):
                            jobs_module.post_priority_bugs()

        self.assertEqual(len(posted), 1)
        self.assertIn("attn:\n\n<@U2>\n<@U4>", posted[0])
        self.assertNotIn("<@U1>", posted[0])
        self.assertNotIn("<@U3>", posted[0])


class UnassignedPlatformWhitelistMatchingTest(unittest.TestCase):
    def test_matches_when_whitelist_is_missing(self):
        person = {"linear_username": "Alex"}
        bugs = [{"platform": "Mobile"}]

        self.assertTrue(jobs_module._person_matches_any_unassigned_platform(person, bugs))

    def test_matches_when_whitelist_has_no_valid_platforms(self):
        person = {"platform_whitelist": ["", "   ", None]}
        bugs = [{"platform": "Mobile"}]

        with patch.object(jobs_module.logging, "warning") as warning:
            self.assertTrue(jobs_module._person_matches_any_unassigned_platform(person, bugs))

        warning.assert_called_once()

    def test_matches_after_normalizing_whitelist_and_bug_platform_values(self):
        person = {"platform_whitelist": ["Mobile App", "Api"]}
        bugs = [{"platform": " mobile-app "}, {"platform": "Web"}]

        self.assertTrue(jobs_module._person_matches_any_unassigned_platform(person, bugs))

    def test_does_not_match_when_bug_platforms_are_missing(self):
        person = {"platform_whitelist": ["Web"]}
        bugs = [{"platform": None}, {"platform": "  "}]

        self.assertFalse(jobs_module._person_matches_any_unassigned_platform(person, bugs))

    def test_warns_and_matches_when_whitelist_is_an_empty_list(self):
        person = {"linear_username": "Alex", "platform_whitelist": []}
        bugs = [{"platform": "Web"}]

        with patch.object(jobs_module.logging, "warning") as warning:
            self.assertTrue(jobs_module._person_matches_any_unassigned_platform(person, bugs))

        warning.assert_called_once()


class PostPriorityBugsInvalidWhitelistFallbackTest(unittest.TestCase):
    def test_unassigned_priority_bug_notifies_people_with_invalid_platform_whitelists(self):
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
            }
        ]
        config = {
            "people": {
                "alex": {"linear_username": "Alex", "slack_id": "U1"},
                "blair": {
                    "linear_username": "Blair",
                    "slack_id": "U2",
                    "platform_whitelist": ["", "   ", None],
                },
                "casey": {
                    "linear_username": "Casey",
                    "slack_id": "U3",
                    "platform_whitelist": ["web"],
                },
            },
            "platforms": {},
        }

        with patch.object(jobs_module, "load_config", return_value=config):
            with patch.object(jobs_module, "get_open_issues", return_value=bugs):
                with patch.object(
                    jobs_module,
                    "get_support_slugs",
                    return_value={"alex", "blair", "casey"},
                ):
                    with patch.object(jobs_module, "post_to_slack", side_effect=posted.append):
                        with patch.object(jobs_module, "datetime", FixedDateTime):
                            with patch.object(jobs_module.logging, "warning") as warning:
                                jobs_module.post_priority_bugs()

        self.assertEqual(len(posted), 1)
        self.assertIn("attn:\n\n<@U1>\n<@U2>", posted[0])
        self.assertNotIn("<@U3>", posted[0])
        warning.assert_called_once()


class PostProjectUpdatesTest(unittest.TestCase):
    def _run(self, projects, config):
        posted = []
        with patch.object(jobs_module, "load_config", return_value=config):
            with patch.object(jobs_module, "get_projects", return_value=projects):
                with patch.object(jobs_module, "post_to_slack", side_effect=posted.append):
                    with patch.object(jobs_module, "datetime", FixedDateTime):
                        jobs_module.post_project_updates()
        return posted

    def test_groups_overdue_ending_soon_and_starting_soon_in_order(self):
        # FixedDateTime -> 2026-03-15 (Sunday)
        projects = [
            # Overdue
            {
                "name": "Late Alpha",
                "url": "https://linear.app/project/late-alpha",
                "targetDate": "2026-03-14",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            # Ending today should use a relative label.
            {
                "name": "Ending Today",
                "url": "https://linear.app/project/ending-today",
                "targetDate": "2026-03-15",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            # Ending soon (within 3 days)
            {
                "name": "Ending Tue",
                "url": "https://linear.app/project/ending-tue",
                "targetDate": "2026-03-17",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            # Ending soon boundary (exactly 3 days out)
            {
                "name": "Ending Wed",
                "url": "https://linear.app/project/ending-wed",
                "targetDate": "2026-03-18",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            # Outside the 3-day ending window
            {
                "name": "Ending Far",
                "url": "https://linear.app/project/ending-far",
                "targetDate": "2026-03-19",
                "status": {"name": "Active"},
                "lead": {"displayName": "Alex"},
            },
            # Starting today should not be in starting soon.
            {
                "name": "Starting Today",
                "url": "https://linear.app/project/starting-today",
                "startDate": "2026-03-15",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Alex"},
            },
            # Starting soon (within 3 days)
            {
                "name": "Starting Tue",
                "url": "https://linear.app/project/starting-tue",
                "startDate": "2026-03-17",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Alex"},
            },
            # Starting soon boundary (exactly 3 days out)
            {
                "name": "Starting Wed",
                "url": "https://linear.app/project/starting-wed",
                "startDate": "2026-03-18",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Alex"},
            },
            # Outside the 3-day starting window
            {
                "name": "Starting Far",
                "url": "https://linear.app/project/starting-far",
                "startDate": "2026-03-19",
                "status": {"name": "Planned"},
                "lead": {"displayName": "Alex"},
            },
            # Canceled starting project should be skipped
            {
                "name": "Canceled Start",
                "url": "https://linear.app/project/canceled-start",
                "startDate": "2026-03-17",
                "status": {"name": "Canceled"},
                "lead": {"displayName": "Alex"},
            },
            # Completed starting project should be skipped
            {
                "name": "Completed Start",
                "url": "https://linear.app/project/completed-start",
                "startDate": "2026-03-17",
                "status": {"name": "Completed"},
                "lead": {"displayName": "Alex"},
            },
            # Released starting project should be skipped
            {
                "name": "Released Start",
                "url": "https://linear.app/project/released-start",
                "startDate": "2026-03-17",
                "status": {"name": "Released"},
                "lead": {"displayName": "Alex"},
            },
            # Non-engineering lead should be skipped
            {
                "name": "Product Overdue",
                "url": "https://linear.app/project/product-overdue",
                "targetDate": "2026-03-10",
                "status": {"name": "Active"},
                "lead": {"displayName": "Pat"},
            },
            # Inactive project - overdue target should be skipped
            {
                "name": "Completed Late",
                "url": "https://linear.app/project/completed-late",
                "targetDate": "2026-03-10",
                "status": {"name": "Completed"},
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

        posted = self._run(projects, config)

        self.assertEqual(len(posted), 1)
        message = posted[0]

        # All three section headers present
        self.assertIn("*Overdue Projects*", message)
        self.assertIn("*Projects Ending Soon*", message)
        self.assertIn("*Projects Starting Soon*", message)

        # Sections are ordered Overdue -> Ending Soon -> Starting Soon
        overdue_idx = message.index("*Overdue Projects*")
        ending_idx = message.index("*Projects Ending Soon*")
        starting_idx = message.index("*Projects Starting Soon*")
        self.assertLess(overdue_idx, ending_idx)
        self.assertLess(ending_idx, starting_idx)

        # Correct projects present
        self.assertIn("Late Alpha", message)
        self.assertIn("Ending Today", message)
        self.assertIn("Ending Tue", message)
        self.assertIn("Ending Wed", message)
        self.assertIn("Starting Tue", message)
        self.assertIn("Starting Wed", message)
        self.assertIn("Lead: <@U1>", message)
        self.assertIn(
            "- <https://linear.app/project/ending-today|Ending Today> - Today - Lead: <@U1>",
            message,
        )
        self.assertIn(
            "- <https://linear.app/project/ending-tue|Ending Tue> - Tue - Lead: <@U1>", message
        )
        self.assertIn(
            "- <https://linear.app/project/ending-wed|Ending Wed> - Wed - Lead: <@U1>", message
        )
        self.assertIn(
            "- <https://linear.app/project/starting-tue|Starting Tue> - Tue - Lead: <@U1>", message
        )
        self.assertIn(
            "- <https://linear.app/project/starting-wed|Starting Wed> - Wed - Lead: <@U1>", message
        )
        self.assertNotIn("d left", message)
        self.assertNotIn("h left", message)

        # Correct projects filtered out
        self.assertNotIn("Ending Far", message)
        self.assertNotIn("Starting Today", message)
        self.assertNotIn("Starting Far", message)
        self.assertNotIn("Canceled Start", message)
        self.assertNotIn("Completed Start", message)
        self.assertNotIn("Released Start", message)
        self.assertNotIn("Product Overdue", message)
        self.assertNotIn("Completed Late", message)

    def test_skips_post_when_no_projects_match(self):
        projects = [
            {
                "name": "Far Future",
                "url": "https://linear.app/project/far-future",
                "startDate": "2026-04-30",
                "targetDate": "2026-05-30",
                "status": {"name": "Planned"},
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
            }
        }

        posted = self._run(projects, config)
        self.assertEqual(posted, [])


if __name__ == "__main__":
    unittest.main()
