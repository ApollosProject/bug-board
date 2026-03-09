import sys
import types
import unittest
from typing import Any, cast
from unittest.mock import patch


def _install_import_shims() -> None:
    dotenv_module = cast(Any, types.ModuleType("dotenv"))
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_module)

    gql_module = cast(Any, types.ModuleType("gql"))

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def execute(self, query, variable_values=None):
            return {}

    gql_module.Client = DummyClient
    gql_module.gql = lambda query: query
    sys.modules.setdefault("gql", gql_module)

    transport_module = cast(Any, types.ModuleType("gql.transport"))
    sys.modules.setdefault("gql.transport", transport_module)

    aiohttp_module = cast(Any, types.ModuleType("gql.transport.aiohttp"))

    class DummyAIOHTTPTransport:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp_module.AIOHTTPTransport = DummyAIOHTTPTransport
    sys.modules.setdefault("gql.transport.aiohttp", aiohttp_module)


_install_import_shims()

import app as app_module  # noqa: E402


class PersonContextTest(unittest.TestCase):
    def test_counts_support_and_customer_priority_work_across_completed_items(self):
        open_items = [
            {
                "title": "Open support issue",
                "url": "https://linear.app/differential/issue/SUP-999",
                "updatedAt": "2026-03-08T12:00:00.000Z",
                "createdAt": "2026-03-07T12:00:00.000Z",
                "project": None,
                "labels": {"nodes": []},
                "platform": None,
            }
        ]
        completed_items = [
            {
                "title": "Support incident",
                "url": "https://linear.app/differential/issue/SUP-616",
                "completedAt": "2026-03-06T15:52:14.784Z",
                "project": None,
                "team": {"key": "SUP", "name": "Support"},
                "labels": {"nodes": []},
                "priority": 2,
                "platform": None,
                "assignee_time_to_fix": 2,
            },
            {
                "title": "Customer success incident",
                "url": "https://linear.app/differential/issue/CUS-25",
                "completedAt": "2026-03-05T15:52:14.784Z",
                "project": None,
                "team": {"key": "CUS", "name": "Customer Success"},
                "labels": {"nodes": []},
                "priority": 2,
                "platform": None,
                "assignee_time_to_fix": 4,
            },
            {
                "title": "APO bug",
                "url": "https://linear.app/differential/issue/APO-22",
                "completedAt": "2026-03-04T15:52:14.784Z",
                "project": None,
                "team": {"key": "APO", "name": "Product + Engineering"},
                "labels": {"nodes": [{"name": "Bug"}]},
                "priority": 1,
                "platform": None,
                "assignee_time_to_fix": 6,
            },
            {
                "title": "APO non-bug priority issue",
                "url": "https://linear.app/differential/issue/APO-23",
                "completedAt": "2026-03-03T15:52:14.784Z",
                "project": None,
                "team": {"key": "APO", "name": "Product + Engineering"},
                "labels": {"nodes": []},
                "priority": 1,
                "platform": None,
                "assignee_time_to_fix": 8,
            },
            {
                "title": "Symphony maintenance task",
                "url": "https://linear.app/differential/issue/SYM-3",
                "completedAt": "2026-03-02T15:52:14.784Z",
                "project": None,
                "team": {"key": "SYM", "name": "Symphony"},
                "labels": {"nodes": [{"name": "bug-board"}]},
                "priority": 2,
                "platform": None,
                "assignee_time_to_fix": 1,
            },
        ]
        config = {
            "people": {
                "michael": {
                    "linear_username": "michael.neeley",
                }
            }
        }

        app_module._build_person_context.cache_clear()
        with patch.object(app_module, "load_config", return_value=config):
            with patch.object(
                app_module,
                "get_open_issues_for_person",
                return_value=open_items,
            ):
                with patch.object(
                    app_module,
                    "get_completed_issues_for_person_all_teams",
                    return_value=completed_items,
                ):
                    with patch.object(app_module, "get_projects", return_value=[]):
                        with patch.object(app_module, "get_support_slugs", return_value=set()):
                            context = app_module._build_person_context(
                                "michael", 30, 1
                            )
        app_module._build_person_context.cache_clear()

        self.assertEqual(context["all_work_done"], 5)
        self.assertEqual(context["priority_bugs_fixed"], 3)
        self.assertEqual(context["priority_bug_avg_time_to_fix"], 4)

    def test_ignores_support_work_without_a_real_priority(self):
        config = {
            "people": {
                "michael": {
                    "linear_username": "michael.neeley",
                }
            }
        }
        completed_items = [
            {
                "title": "Support follow-up",
                "url": "https://linear.app/differential/issue/SUP-700",
                "completedAt": "2026-03-06T15:52:14.784Z",
                "project": None,
                "team": {"key": "SUP", "name": "Support"},
                "labels": {"nodes": []},
                "priority": 0,
                "platform": None,
                "assignee_time_to_fix": 3,
            }
        ]

        app_module._build_person_context.cache_clear()
        with patch.object(app_module, "load_config", return_value=config):
            with patch.object(
                app_module,
                "get_open_issues_for_person",
                return_value=[],
            ):
                with patch.object(
                    app_module,
                    "get_completed_issues_for_person_all_teams",
                    return_value=completed_items,
                ):
                    with patch.object(app_module, "get_projects", return_value=[]):
                        with patch.object(app_module, "get_support_slugs", return_value=set()):
                            context = app_module._build_person_context(
                                "michael", 30, 2
                            )
        app_module._build_person_context.cache_clear()

        self.assertEqual(context["all_work_done"], 1)
        self.assertEqual(context["priority_bugs_fixed"], 0)
        self.assertIsNone(context["priority_bug_avg_time_to_fix"])


if __name__ == "__main__":
    unittest.main()
