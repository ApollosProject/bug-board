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
                                        "name": "Giving History + Recurring Management",
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


if __name__ == "__main__":
    unittest.main()
