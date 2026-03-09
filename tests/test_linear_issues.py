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
                print_ast(query_document)
                if not isinstance(query_document, str)
                else query_document
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

        with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
            with patch.object(issues_module, "_execute", side_effect=fake_execute):
                with patch.object(
                    issues_module,
                    "_compute_assignee_time_to_fix",
                    return_value=0,
                ):
                    issues = issues_module.get_completed_issues_for_person(
                        "michael.neeley", 7
                    )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["platform"], "Shovel")
        self.assertEqual(captured["variables"]["login"], "michael.neeley")
        self.assertEqual(captured["variables"]["team_key"], "APO")
        self.assertEqual(captured["variables"]["days"], "-P7D")
        self.assertIn('state: {type: {in: ["completed"]}}', captured["query"])
        self.assertNotIn('state: {name: {in: ["Done"]}}', captured["query"])


if __name__ == "__main__":
    unittest.main()
