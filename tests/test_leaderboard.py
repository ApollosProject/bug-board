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

    linear_projects_module.get_projects = _get_projects

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
                "name": "Google Pay",
                "status": {"name": "Released"},
                "completedAt": "2026-04-03T00:00:00.000Z",
                "startDate": "2026-02-09",
                "targetDate": "2026-03-30",
                "lead": {"displayName": "nick"},
                "members": ["Austin"],
            }
        ]
        now = datetime(2026, 4, 7, tzinfo=timezone.utc)

        with patch.object(leaderboard_module, "get_projects", return_value=projects):
            lead_points = leaderboard_module.calculate_cycle_project_lead_points(
                30, now
            )
            member_points = leaderboard_module.calculate_cycle_project_member_points(
                30, now
            )

        self.assertEqual(lead_points, {"nick": 120})
        self.assertEqual(member_points, {"Austin": 60})
