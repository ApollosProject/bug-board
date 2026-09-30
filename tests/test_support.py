import unittest
from datetime import datetime
from unittest.mock import patch

import support


class FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls):
        return cls(2026, 6, 15)


def _project(**overrides):
    project = {
        "startDate": "2026-06-01",
        "status": {"name": "In Progress"},
        "lead": {"displayName": "Ada Lovelace"},
        "members": ["Grace Hopper"],
    }
    project.update(overrides)
    return project


CONFIG = {
    "people": {
        "ada": {"linear_username": "ada.lovelace", "team": "engineering"},
        "grace": {"linear_username": "grace.hopper", "team": "engineering"},
        "ops": {"linear_username": "ops.person", "team": "support"},
    }
}


class IsActiveTodayTest(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(support, "datetime", FrozenDateTime)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_started_projects_stay_active_regardless_of_target(self):
        for target in (None, "2026-06-10", "2026-07-01", "not-a-date", ""):
            with self.subTest(target=target):
                project = _project(startDate="2026-06-15")
                if target is None:
                    project.pop("targetDate", None)
                else:
                    project["targetDate"] = target
                self.assertTrue(support._is_active_today(project))

    def test_inactive_or_not_yet_started_projects_are_excluded(self):
        cases = [
            _project(completedAt="2026-06-02T00:00:00Z"),
            _project(status={"name": "Completed"}),
            _project(status={"name": "Canceled"}),
            _project(status={"name": "cancelled"}),
            _project(status={"name": "Incomplete"}),
            _project(status={"name": "Released"}),
            _project(startDate=None),
            _project(startDate=""),
            _project(startDate="nope"),
            _project(startDate="2026-06-16", targetDate="2026-06-01"),
        ]
        for project in cases:
            with self.subTest(project=project):
                self.assertFalse(support._is_active_today(project))


class GetSupportSlugsTest(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(support, "datetime", FrozenDateTime)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_overdue_active_project_keeps_assignees_off_support(self):
        slugs = support.get_support_slugs(
            config=CONFIG,
            projects=[_project(targetDate="2026-06-01")],
        )
        self.assertEqual(slugs, set())

    def test_future_or_finished_projects_leave_engineers_on_support(self):
        projects = [
            _project(startDate="2026-06-20", lead={"displayName": "Ada Lovelace"}, members=[]),
            _project(
                completedAt="2026-06-02",
                lead={"displayName": "Grace Hopper"},
                members=[],
                targetDate="2026-07-01",
            ),
        ]
        slugs = support.get_support_slugs(config=CONFIG, projects=projects)
        self.assertEqual(slugs, {"ada", "grace"})
