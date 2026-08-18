import unittest
from unittest.mock import patch

from linear import projects as project_module


class GetProjectsTest(unittest.TestCase):
    def test_get_projects_paginates_and_normalizes_members(self):
        responses = [
            {
                "teams": {
                    "nodes": [
                        {
                            "projects": {
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "cursor-1",
                                },
                                "nodes": [
                                    {
                                        "id": "project-1",
                                        "name": "Web Giving",
                                        "status": {"type": "started"},
                                        "members": {"nodes": [{"displayName": "Nathan Lewis"}]},
                                    }
                                ],
                            }
                        }
                    ]
                }
            },
            {
                "teams": {
                    "nodes": [
                        {
                            "projects": {
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": None,
                                },
                                "nodes": [
                                    {
                                        "id": "project-2",
                                        "name": "Giving History + Recurring Management",
                                        "status": {"type": "completed"},
                                        "members": {"nodes": [{"displayName": "Austin Witherow"}]},
                                    }
                                ],
                            }
                        }
                    ]
                }
            },
        ]
        calls = []
        queries = []

        def fake_execute(_query, variable_values=None):
            queries.append(str(_query))
            calls.append(variable_values)
            return responses[len(calls) - 1]

        with patch.object(project_module, "_execute", side_effect=fake_execute):
            with patch.object(project_module, "get_linear_team_key", return_value="APO"):
                projects = project_module.get_projects()

        self.assertEqual(
            calls,
            [
                {"team_key": "APO", "after": None},
                {"team_key": "APO", "after": "cursor-1"},
            ],
        )
        self.assertEqual(
            [project["name"] for project in projects],
            [
                "Giving History + Recurring Management",
                "Web Giving",
            ],
        )
        self.assertEqual(projects[0]["members"], ["Austin Witherow"])
        self.assertEqual(projects[1]["members"], ["Nathan Lewis"])
        self.assertIn("lastUpdate", queries[0])


class GetCompletedProjectIssueAssigneesTest(unittest.TestCase):
    def test_paginates_and_returns_sorted_unique_assignees(self):
        responses = [
            {
                "issues": {
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "issue-cursor-1",
                    },
                    "nodes": [
                        {
                            "project": {"id": "project-2"},
                            "assignee": {"displayName": "Austin Witherow"},
                        },
                        {"project": {"id": "project-2"}, "assignee": None},
                    ],
                }
            },
            {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [
                        {
                            "project": {"id": "project-2"},
                            "assignee": {"displayName": "Later Page Contributor"},
                        },
                        {
                            "project": {"id": "project-2"},
                            "assignee": {"displayName": "Austin Witherow"},
                        },
                    ],
                }
            },
        ]
        calls = []
        queries = []

        def fake_execute(_query, variable_values=None):
            queries.append(str(_query))
            calls.append(variable_values)
            return responses[len(calls) - 1]

        with patch.object(project_module, "_execute", side_effect=fake_execute):
            assignees = project_module.get_completed_issue_assignees_by_project(["project-2"])

        self.assertEqual(
            calls,
            [
                {"project_ids": ["project-2"], "after": None},
                {"project_ids": ["project-2"], "after": "issue-cursor-1"},
            ],
        )
        self.assertIn("$project_ids: [ID!]", queries[0])
        self.assertEqual(assignees["project-2"], ["Austin Witherow", "Later Page Contributor"])

    def test_batches_assignees_for_multiple_projects_in_one_query(self):
        response = {
            "issues": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {
                        "project": {"id": "project-1"},
                        "assignee": {"displayName": "Austin"},
                    },
                    {
                        "project": {"id": "project-2"},
                        "assignee": {"displayName": "Nick"},
                    },
                    {
                        "project": {"id": "project-1"},
                        "assignee": {"displayName": "Contributor"},
                    },
                ],
            }
        }
        calls = []

        def fake_execute(_query, variable_values=None):
            calls.append(variable_values)
            return response

        with patch.object(project_module, "_execute", side_effect=fake_execute):
            assignees = project_module.get_completed_issue_assignees_by_project(
                ["project-1", "project-2"]
            )

        self.assertEqual(calls, [{"project_ids": ["project-1", "project-2"], "after": None}])
        self.assertEqual(assignees["project-1"], ["Austin", "Contributor"])
        self.assertEqual(assignees["project-2"], ["Nick"])

    def test_returns_empty_mapping_without_query_when_no_project_ids(self):
        with patch.object(project_module, "_execute") as execute:
            assignees = project_module.get_completed_issue_assignees_by_project([])

        execute.assert_not_called()
        self.assertEqual(assignees, {})


if __name__ == "__main__":
    unittest.main()
