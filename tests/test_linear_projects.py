import unittest
from unittest.mock import patch

from linear import projects as project_module


def _project_pages():
    return [
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


class GetProjectsTest(unittest.TestCase):
    def test_get_projects_paginates_and_normalizes_members_without_assignee_fetches(self):
        responses = _project_pages()
        calls = []

        def fake_execute(_query, variable_values=None):
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
        self.assertNotIn("completedIssueAssignees", projects[0])
        self.assertEqual(projects[1]["members"], ["Nathan Lewis"])
        self.assertNotIn("completedIssueAssignees", projects[1])

    def test_get_projects_can_fetch_completed_issue_assignees_for_completed_projects(self):
        responses = [
            *_project_pages(),
            {
                "issues": {
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "issue-cursor-1",
                    },
                    "nodes": [
                        {"assignee": {"displayName": "Austin Witherow"}},
                        {"assignee": None},
                    ],
                }
            },
            {
                "issues": {
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                    "nodes": [
                        {"assignee": {"displayName": "Later Page Contributor"}},
                        {"assignee": {"displayName": "Austin Witherow"}},
                    ],
                }
            },
        ]
        calls = []

        def fake_execute(_query, variable_values=None):
            calls.append(variable_values)
            return responses[len(calls) - 1]

        with patch.object(project_module, "_execute", side_effect=fake_execute):
            with patch.object(project_module, "get_linear_team_key", return_value="APO"):
                projects = project_module.get_projects(include_completed_issue_assignees=True)

        self.assertEqual(
            calls,
            [
                {"team_key": "APO", "after": None},
                {"team_key": "APO", "after": "cursor-1"},
                {"project_id": "project-2", "after": None},
                {"project_id": "project-2", "after": "issue-cursor-1"},
            ],
        )
        self.assertEqual(
            projects[0]["completedIssueAssignees"],
            ["Austin Witherow", "Later Page Contributor"],
        )
        self.assertEqual(projects[1]["completedIssueAssignees"], [])


if __name__ == "__main__":
    unittest.main()
