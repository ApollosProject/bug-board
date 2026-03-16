import sys
import types
import unittest
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
    github_module.get_prs_waiting_for_review_by_reviewer = (
        lambda *args, **kwargs: {}
    )
    github_module.get_prs_with_changes_requested_by_reviewer = (
        lambda *args, **kwargs: {}
    )
    github_module.merged_prs_by_author = lambda *args, **kwargs: {}
    github_module.merged_prs_by_reviewer = lambda *args, **kwargs: {}
    sys.modules.setdefault("github", github_module)

    leaderboard_module = cast(Any, types.ModuleType("leaderboard"))
    leaderboard_module.calculate_cycle_project_lead_points = (
        lambda *args, **kwargs: 0
    )
    leaderboard_module.calculate_cycle_project_member_points = (
        lambda *args, **kwargs: 0
    )
    sys.modules.setdefault("leaderboard", leaderboard_module)

    linear_package = cast(Any, types.ModuleType("linear"))
    linear_package.__path__ = []
    sys.modules.setdefault("linear", linear_package)

    linear_issues_module = cast(Any, types.ModuleType("linear.issues"))
    linear_issues_module.get_completed_issues = lambda *args, **kwargs: []
    linear_issues_module.get_completed_issues_for_person = lambda *args, **kwargs: []
    linear_issues_module.get_open_issues = lambda *args, **kwargs: []
    linear_issues_module.get_open_issues_in_projects = lambda *args, **kwargs: []
    linear_issues_module.get_recently_resolved_parent_issues_in_project = (
        lambda *args, **kwargs: []
    )
    linear_issues_module.get_stale_issues_by_assignee = lambda *args, **kwargs: {}
    sys.modules.setdefault("linear.issues", linear_issues_module)

    linear_projects_module = cast(Any, types.ModuleType("linear.projects"))
    linear_projects_module.get_project_by_name = lambda *args, **kwargs: None
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


class _FakeScheduledJob:
    def __init__(self, recorder, interval=None):
        self._recorder = recorder
        self.interval = interval
        self.unit = None
        self.at_time = None

    @property
    def day(self):
        self.unit = "day"
        return self

    @property
    def friday(self):
        self.unit = "friday"
        return self

    @property
    def seconds(self):
        self.unit = "seconds"
        return self

    def at(self, at_time):
        self.at_time = at_time
        return self

    def do(self, func):
        self._recorder.append(
            {
                "interval": self.interval,
                "unit": self.unit,
                "at_time": self.at_time,
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
                "func": jobs_module.post_inactive_engineers,
            },
            recorded_jobs,
        )
        self.assertIn(
            {
                "interval": None,
                "unit": "day",
                "at_time": "14:00",
                "func": jobs_module.post_recon_issues,
            },
            recorded_jobs,
        )


if __name__ == "__main__":
    unittest.main()
