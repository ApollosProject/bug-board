import unittest
from datetime import date
from unittest.mock import patch

from graphql import print_ast

import linear.issues as issues_module
from time_window import TimeWindow


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

        with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
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
        self.assertEqual(captured["variables"]["team_key"], "APO")
        self.assertEqual(captured["variables"]["after"], "-P7D")
        normalized_query = " ".join(captured["query"].split())
        self.assertIn('state: { type: { in: ["completed"] } }', normalized_query)
        self.assertNotIn('state: { name: { in: ["Done"] } }', normalized_query)

    def test_date_range_uses_inclusive_completed_at_bounds(self):
        captured = {}

        def fake_execute(query, variable_values=None):
            captured["query"] = query
            captured["variables"] = variable_values
            return {
                "issues": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }

        window = TimeWindow.from_dates(date(2026, 1, 1), date(2026, 1, 31))
        with patch.object(issues_module, "get_linear_team_key", return_value="APO"):
            with patch.object(issues_module, "_execute", side_effect=fake_execute):
                with patch.object(
                    issues_module,
                    "_compute_assignee_time_to_fix",
                    return_value=0,
                ):
                    issues_module.get_completed_issues_for_person("michael.neeley", window=window)

        self.assertEqual(captured["variables"]["after"], "2026-01-01T00:00:00.000Z")
        self.assertEqual(captured["variables"]["before"], "2026-02-01T00:00:00.000Z")
        query_document = captured["query"]
        query_text = (
            print_ast(query_document.document)
            if hasattr(query_document, "document")
            else query_document
        )
        self.assertIn(
            "completedAt: { gte: $after, lt: $before }",
            " ".join(str(query_text).split()),
        )


if __name__ == "__main__":
    unittest.main()
