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
                                        "members": {"nodes": [{"displayName": "Austin Witherow"}]},
                                    }
                                ],
                            }
                        }
                    ]
                }
            },
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
            {
                "issues": {
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                    "nodes": [
                        {"assignee": {"displayName": "Nathan Lewis"}},
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
                projects = project_module.get_projects()

        self.assertEqual(
            calls,
            [
                {"team_key": "APO", "after": None},
                {"team_key": "APO", "after": "cursor-1"},
                {"project_id": "project-2", "after": None},
                {"project_id": "project-2", "after": "issue-cursor-1"},
                {"project_id": "project-1", "after": None},
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
        self.assertEqual(
            projects[0]["completedIssueAssignees"],
            ["Austin Witherow", "Later Page Contributor"],
        )
        self.assertEqual(projects[1]["completedIssueAssignees"], ["Nathan Lewis"])


if __name__ == "__main__":
    unittest.main()
