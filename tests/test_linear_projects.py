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

    gql_module.gql = lambda query: query
    gql_module.Client = DummyClient
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

from linear import projects as projects_module  # noqa: E402


class GetProjectsTest(unittest.TestCase):
    def test_get_projects_paginates_and_flattens_member_names(self):
        responses = [
            {
                "teams": {
                    "nodes": [
                        {
                            "projects": {
                                "nodes": [
                                    {
                                        "id": "1",
                                        "name": "Zeta",
                                        "url": "https://example.com/zeta",
                                        "members": {
                                            "nodes": [{"displayName": "nathan"}]
                                        },
                                    }
                                ],
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "cursor-1",
                                },
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
                                "nodes": [
                                    {
                                        "id": "2",
                                        "name": "Alpha",
                                        "url": "https://example.com/alpha",
                                        "members": {
                                            "nodes": [
                                                {"displayName": "austin"},
                                                {"displayName": None},
                                            ]
                                        },
                                    }
                                ],
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": None,
                                },
                            }
                        }
                    ]
                }
            },
        ]

        execute_calls: list[dict[str, str | None]] = []

        def fake_execute(query, variable_values=None):
            execute_calls.append(variable_values or {})
            return responses.pop(0)

        with patch.object(projects_module, "_execute", side_effect=fake_execute):
            with patch.object(
                projects_module, "get_linear_team_key", return_value="APO"
            ):
                projects = projects_module.get_projects()

        self.assertEqual(
            execute_calls,
            [
                {"team_key": "APO", "cursor": None},
                {"team_key": "APO", "cursor": "cursor-1"},
            ],
        )
        self.assertEqual([project["name"] for project in projects], ["Alpha", "Zeta"])
        self.assertEqual(projects[0]["members"], ["austin"])
        self.assertEqual(projects[1]["members"], ["nathan"])


if __name__ == "__main__":
    unittest.main()
