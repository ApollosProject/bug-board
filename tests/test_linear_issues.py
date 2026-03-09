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

from linear import issues as issues_module  # noqa: E402


class FixedDateTime(datetime):
    @classmethod
    def utcnow(cls):
        return datetime(2026, 3, 9, 0, 0, 0)


class CompletedIssuesAllTeamsTest(unittest.TestCase):
    def test_uses_completed_state_type_for_all_team_query(self):
        responses = [
            {
                "issues": {
                    "nodes": [
                        {
                            "title": "Support incident",
                            "completedAt": "2026-03-08T00:00:00.000Z",
                            "labels": {"nodes": [{"name": "iOS"}]},
                            "history": {"edges": []},
                        }
                    ],
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "cursor-1",
                    },
                }
            },
            {
                "issues": {
                    "nodes": [
                        {
                            "title": "Customer follow-up",
                            "completedAt": "2026-03-07T00:00:00.000Z",
                            "labels": {"nodes": [{"name": "Other"}]},
                            "history": {"edges": []},
                        }
                    ],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                }
            },
        ]

        execute_calls = []

        def fake_execute(query, variable_values=None):
            execute_calls.append((query, variable_values))
            return responses[len(execute_calls) - 1]

        with patch.object(issues_module, "_execute", side_effect=fake_execute):
            with patch.object(issues_module, "get_platforms", return_value={"ios"}):
                with patch.object(
                    issues_module,
                    "_compute_assignee_time_to_fix",
                    side_effect=[2, 4],
                ):
                    with patch.object(issues_module, "datetime", FixedDateTime):
                        issues = issues_module.get_completed_issues_for_person_all_teams(
                            "michael.neeley", 30
                        )

        self.assertEqual(len(execute_calls), 2)
        query_text = execute_calls[0][0]
        self.assertIn('state: { type: { in: ["completed"] } }', query_text)
        self.assertNotIn('state: { name: { in: ["Done"] } }', query_text)
        self.assertEqual(execute_calls[0][1]["cursor"], None)
        self.assertEqual(execute_calls[1][1]["cursor"], "cursor-1")

        self.assertEqual(len(issues), 2)
        self.assertEqual(issues[0]["platform"], "iOS")
        self.assertIsNone(issues[1]["platform"])
        self.assertEqual(issues[0]["daysCompleted"], 1)
        self.assertEqual(issues[1]["daysCompleted"], 2)
        self.assertEqual(issues[0]["assignee_time_to_fix"], 2)
        self.assertEqual(issues[1]["assignee_time_to_fix"], 4)


if __name__ == "__main__":
    unittest.main()
