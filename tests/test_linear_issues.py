import unittest
from unittest.mock import patch

from graphql import print_ast

import linear.issues as issues_module


class GetCompletedIssuesForPersonTest(unittest.TestCase):
    def test_filters_completed_items_by_state_type(self):
        captured = {}

        def fake_execute(query, variable_values=None):
            query_document = query.document if hasattr(query, "document") else query
            captured["query"] = (
                print_ast(query_document) if not isinstance(query_document, str) else query_document
            )
            captured["variables"] = variable_values
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": "issue-1",
                            "title": "Released issue",
                            "url": "https://linear.app/example/issue-1",
                            "completedAt": "2026-03-04T03:04:33.635Z",
                            "project": None,
                            "labels": {"nodes": [{"name": "Shovel"}]},
                            "priority": 2,
                            "history": {"edges": []},
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }

        with patch.object(issues_module, "get_linear_team_keys", return_value=["APO", "SUP"]):
            with patch.object(issues_module, "_execute", side_effect=fake_execute):
                with patch.object(
                    issues_module,
                    "_compute_assignee_time_to_fix",
                    return_value=0,
                ):
                    issues = issues_module.get_completed_issues_for_person("michael.neeley", 7)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["platform"], "Shovel")
        self.assertEqual(captured["variables"]["login"], "michael.neeley")
        self.assertEqual(captured["variables"]["team_keys"], ["APO", "SUP"])
        self.assertNotIn("team_key", captured["variables"])
        self.assertIn("after", captured["variables"])
        self.assertIn("before", captured["variables"])
        self.assertNotIn("days", captured["variables"])
        normalized_query = "".join(captured["query"].split())
        self.assertIn('state:{type:{in:["completed"]}}', normalized_query)
        self.assertIn("team:{key:{in:$team_keys}}", normalized_query)
        self.assertIn("$team_keys:[String!]!", normalized_query)
        self.assertIn("completedAt:{gte:$after,lt:$before}", normalized_query)
        self.assertIn("identifier", normalized_query)
        self.assertNotIn('state:{name:{in:["Done"]}}', normalized_query)


class GetOpenIssuesForPersonTest(unittest.TestCase):
    def test_queries_every_configured_team(self):
        captured = {}

        def fake_execute(query, variable_values=None):
            query_document = query.document if hasattr(query, "document") else query
            captured["query"] = (
                print_ast(query_document) if not isinstance(query_document, str) else query_document
            )
            captured["variables"] = variable_values
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": "sup-1",
                            "title": "Support issue",
                            "url": "https://linear.app/example/sup-1",
                            "updatedAt": "2026-03-04T03:04:33.635Z",
                            "createdAt": "2026-03-01T03:04:33.635Z",
                            "project": None,
                            "labels": {"nodes": [{"name": "Issue"}]},
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }

        with patch.object(issues_module, "get_linear_team_keys", return_value=["APO", "SUP"]):
            with patch.object(issues_module, "get_platforms", return_value=set()):
                with patch.object(issues_module, "_execute", side_effect=fake_execute):
                    issues = issues_module.get_open_issues_for_person("brandon")

        self.assertEqual([issue["id"] for issue in issues], ["sup-1"])
        self.assertEqual(captured["variables"]["team_keys"], ["APO", "SUP"])
        normalized_query = "".join(captured["query"].split())
        self.assertIn("team:{key:{in:$team_keys}}", normalized_query)
        self.assertIn("$team_keys:[String!]!", normalized_query)


if __name__ == "__main__":
    unittest.main()
