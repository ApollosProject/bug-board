import importlib
import sys
import types
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch


def _import_leaderboard_with_stub():
    linear_package = types.ModuleType("linear")
    linear_package.__path__ = []

    linear_projects_module = types.ModuleType("linear.projects")

    def _get_projects():
        return []

    def _get_completed_project_issue_assignees(_project_id):
        return []

    linear_projects_module.get_projects = _get_projects
    linear_projects_module.get_completed_project_issue_assignees = (
        _get_completed_project_issue_assignees
    )

    original_leaderboard = sys.modules.pop("leaderboard", None)
    try:
        with patch.dict(
            sys.modules,
            {"linear": linear_package, "linear.projects": linear_projects_module},
        ):
            import leaderboard as leaderboard_module

            return importlib.reload(leaderboard_module)
    finally:
        sys.modules.pop("leaderboard", None)
        if original_leaderboard is not None:
            sys.modules["leaderboard"] = original_leaderboard


class CycleProjectPointsTest(TestCase):
    def test_released_project_counts_toward_leaderboard(self):
        leaderboard_module = _import_leaderboard_with_stub()
        projects = [
            {
                "id": "project-1",
                "name": "Google Pay",
                "status": {"name": "Released", "type": "completed"},
                "completedAt": "2026-04-03T00:00:00.000Z",
                "startDate": "2026-02-09",
                "targetDate": "2026-03-30",
                "lead": {"displayName": "nick"},
                "members": ["Austin", "Member Only"],
            }
        ]
        now = datetime(2026, 4, 7, tzinfo=timezone.utc)

        with patch.object(leaderboard_module, "get_projects", return_value=projects):
            with patch.object(
                leaderboard_module,
                "get_completed_project_issue_assignees",
                return_value=["Austin"],
            ) as assignees_mock:
                lead_points = leaderboard_module.calculate_cycle_project_lead_points(30, now)
                member_points = leaderboard_module.calculate_cycle_project_member_points(30, now)

        assignees_mock.assert_called_with("project-1")
        self.assertEqual(lead_points, {"nick": 120})
        self.assertEqual(member_points, {"Austin": 60})

    def test_canceled_project_does_not_count_even_with_completed_at(self):
        leaderboard_module = _import_leaderboard_with_stub()
        projects = [
            {
                "id": "project-1",
                "name": "Canceled Project",
                "status": {"name": "Canceled", "type": "canceled"},
                "completedAt": "2026-04-03T00:00:00.000Z",
                "startDate": "2026-02-09",
                "targetDate": "2026-03-30",
                "lead": {"displayName": "nick"},
                "members": ["Austin"],
            }
        ]
        now = datetime(2026, 4, 7, tzinfo=timezone.utc)

        with patch.object(leaderboard_module, "get_projects", return_value=projects):
            lead_points = leaderboard_module.calculate_cycle_project_lead_points(30, now)
            member_points = leaderboard_module.calculate_cycle_project_member_points(30, now)

        self.assertEqual(lead_points, {})
        self.assertEqual(member_points, {})

    def test_project_members_without_completed_issues_do_not_get_points(self):
        leaderboard_module = _import_leaderboard_with_stub()
        projects = [
            {
                "id": "project-1",
                "name": "Member Only Project",
                "status": {"name": "Released", "type": "completed"},
                "completedAt": "2026-04-03T00:00:00.000Z",
                "startDate": "2026-03-24",
                "targetDate": "2026-03-30",
                "lead": {"displayName": "nick"},
                "members": ["Member Only"],
            }
        ]
        now = datetime(2026, 4, 7, tzinfo=timezone.utc)

        with patch.object(leaderboard_module, "get_projects", return_value=projects):
            with patch.object(
                leaderboard_module,
                "get_completed_project_issue_assignees",
                return_value=["Contributor"],
            ):
                member_points = leaderboard_module.calculate_cycle_project_member_points(30, now)

        self.assertEqual(member_points, {"Contributor": 15})

    def test_assignees_not_fetched_for_projects_outside_window(self):
        leaderboard_module = _import_leaderboard_with_stub()
        projects = [
            {
                "id": "old-project",
                "name": "Old Released Project",
                "status": {"name": "Released", "type": "completed"},
                "completedAt": "2025-01-01T00:00:00.000Z",
                "startDate": "2024-12-01",
                "targetDate": "2024-12-15",
                "lead": {"displayName": "nick"},
                "members": ["Austin"],
            }
        ]
        now = datetime(2026, 4, 7, tzinfo=timezone.utc)

        with patch.object(leaderboard_module, "get_projects", return_value=projects):
            with patch.object(
                leaderboard_module,
                "get_completed_project_issue_assignees",
                return_value=["Austin"],
            ) as assignees_mock:
                lead_points = leaderboard_module.calculate_cycle_project_lead_points(30, now)
                member_points = leaderboard_module.calculate_cycle_project_member_points(30, now)

        assignees_mock.assert_not_called()
        self.assertEqual(lead_points, {})
        self.assertEqual(member_points, {})
