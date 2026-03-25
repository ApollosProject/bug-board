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

        def execute(self, query, **kwargs):
            return {}

    class DummyGraphQLRequest:
        def __init__(self, request, *, variable_values=None, operation_name=None):
            self.request = request
            self.variable_values = variable_values
            self.operation_name = operation_name

    gql_module.Client = DummyClient
    gql_module.GraphQLRequest = DummyGraphQLRequest
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


class LinearIssueStateFiltersTest(unittest.TestCase):
    def test_get_open_issues_uses_state_type_for_open_filter(self):
        execute_calls = []
        response = {
            "issues": {
                "nodes": [
                    {
                        "title": "Priority bug",
                        "createdAt": "2026-03-08T00:00:00.000Z",
                        "updatedAt": "2026-03-08T12:00:00.000Z",
                        "labels": {"nodes": [{"name": "Bug"}, {"name": "Mobile"}]},
                        "priority": 2,
                    }
                ]
            }
        }

        def fake_execute(query, variable_values=None):
            execute_calls.append((query, variable_values))
            return response

        with patch.object(issues_module, "_execute", side_effect=fake_execute):
            with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
                with patch.object(
                    issues_module, "get_platforms", return_value={"mobile"}
                ):
                    with patch.object(issues_module, "datetime", FixedDateTime):
                        issues = issues_module.get_open_issues(2, "Bug")

        query_text = execute_calls[0][0]
        self.assertIn('state: { type: { nin: ["completed", "canceled"] } }', query_text)
        self.assertNotIn(
            'state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }',
            query_text,
        )
        self.assertIn("slaMediumRiskAt", query_text)
        self.assertIn("slaHighRiskAt", query_text)
        self.assertIn("slaBreachesAt", query_text)
        self.assertEqual(issues[0]["platform"], "Mobile")
        self.assertEqual(issues[0]["daysOpen"], 1)

    def test_get_completed_issues_summary_uses_completed_state_type(self):
        execute_calls = []
        response = {
            "issues": {
                "nodes": [
                    {
                        "title": "Released bug",
                        "project": {"name": "No Project"},
                        "priority": 2,
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }

        def fake_execute(query, variable_values=None):
            execute_calls.append((query, variable_values))
            return response

        with patch.object(issues_module, "_execute", side_effect=fake_execute):
            with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
                issues_module.get_completed_issues_summary(2, "Bug", 30)

        query_text = execute_calls[0][0]
        self.assertIn('state: { type: { in: ["completed"] } }', query_text)
        self.assertNotIn('state: { name: { in: ["Done"] } }', query_text)
        self.assertEqual(execute_calls[0][1]["days"], "-P30D")

    def test_get_open_issues_for_person_uses_state_type_for_open_filter(self):
        execute_calls = []
        response = {
            "issues": {
                "nodes": [
                    {
                        "title": "Current issue",
                        "createdAt": "2026-03-07T00:00:00.000Z",
                        "updatedAt": "2026-03-08T00:00:00.000Z",
                        "labels": {"nodes": [{"name": "Mobile"}]},
                        "project": {"name": "No Project"},
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }

        def fake_execute(query, variable_values=None):
            execute_calls.append((query, variable_values))
            return response

        with patch.object(issues_module, "_execute", side_effect=fake_execute):
            with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
                with patch.object(
                    issues_module, "get_platforms", return_value={"mobile"}
                ):
                    with patch.object(issues_module, "datetime", FixedDateTime):
                        issues = issues_module.get_open_issues_for_person(
                            "michael.neeley"
                        )

        query_text = execute_calls[0][0]
        self.assertIn('state: { type: { nin: ["completed", "canceled"] } }', query_text)
        self.assertNotIn(
            'state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }',
            query_text,
        )
        self.assertEqual(issues[0]["platform"], "Mobile")
        self.assertEqual(issues[0]["daysOpen"], 2)
        self.assertEqual(issues[0]["daysUpdated"], 1)

    def test_get_completed_issues_for_person_uses_completed_state_type(self):
        responses = [
            {
                "issues": {
                    "nodes": [
                        {
                            "title": "Released bug",
                            "completedAt": "2026-03-08T00:00:00.000Z",
                            "labels": {"nodes": [{"name": "iOS"}]},
                            "history": {"edges": []},
                        }
                    ],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                }
            },
            {
                "issues": {
                    "nodes": [
                        {
                            "title": "Released follow-up",
                            "completedAt": "2026-03-07T00:00:00.000Z",
                            "labels": {"nodes": [{"name": "Other"}]},
                            "history": {"edges": []},
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            },
        ]
        execute_calls = []

        def fake_execute(query, variable_values=None):
            execute_calls.append((query, variable_values))
            return responses[len(execute_calls) - 1]

        with patch.object(issues_module, "_execute", side_effect=fake_execute):
            with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
                with patch.object(issues_module, "get_platforms", return_value={"ios"}):
                    with patch.object(
                        issues_module,
                        "_compute_assignee_time_to_fix",
                        side_effect=[2, 4],
                    ):
                        with patch.object(issues_module, "datetime", FixedDateTime):
                            issues = issues_module.get_completed_issues_for_person(
                                "michael.neeley", 30
                            )

        self.assertEqual(len(execute_calls), 2)
        query_text = execute_calls[0][0]
        self.assertIn('state: { type: { in: ["completed"] } }', query_text)
        self.assertNotIn('state: { name: { in: ["Done"] } }', query_text)
        self.assertEqual(execute_calls[0][1]["cursor"], None)
        self.assertEqual(execute_calls[1][1]["cursor"], "cursor-1")
        self.assertEqual(issues[0]["platform"], "iOS")
        self.assertIsNone(issues[1]["platform"])
        self.assertEqual(issues[0]["daysCompleted"], 1)
        self.assertEqual(issues[1]["daysCompleted"], 2)
        self.assertEqual(issues[0]["assignee_time_to_fix"], 2)
        self.assertEqual(issues[1]["assignee_time_to_fix"], 4)


if __name__ == "__main__":
    unittest.main()
