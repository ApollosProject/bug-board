import unittest
from datetime import date
from unittest.mock import patch

from gql import GraphQLRequest, gql

import github
from linear import client as linear_client
from time_window import TimeWindow


class _RecordingClient:
    def __init__(self):
        self.calls = []

    def execute(self, request, **kwargs):
        self.calls.append((request, kwargs))
        return {"ok": True}


class GraphQLClientRequestTests(unittest.TestCase):
    def test_person_pr_counts_use_scoped_searches_and_only_count_approvals(self):
        github._get_cursor_authored_merged_prs_cached.cache_clear()
        response = {
            "authored": {"issueCount": 60},
            "reviewed": {
                "nodes": [
                    {"reviews": {"nodes": [{"author": {"login": "Bkraeling"}}]}},
                    {"reviews": {"nodes": [{"author": {"login": "someone-else"}}]}},
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        }

        with (
            patch.object(github, "token", "token"),
            patch.object(github, "get_github_orgs", return_value=["apollosproject"]),
            patch.object(github, "_execute", return_value=response) as execute,
            patch.object(github.time, "monotonic", return_value=120),
        ):
            counts = github.get_merged_pr_counts_for_user("bkraeling", 30)
            self.assertEqual(github.get_merged_pr_counts_for_user("bkraeling", 30), counts)

        self.assertEqual(counts, (60, 1))
        variables = execute.call_args_list[0].kwargs["variable_values"]
        self.assertIn("author:bkraeling", variables["authored"])
        self.assertIn("reviewed-by:bkraeling", variables["reviewed"])
        delegated_variables = execute.call_args_list[1].kwargs["variable_values"]
        self.assertIn("author:app/cursor", delegated_variables["query"])
        self.assertEqual(execute.call_count, 3)

    def test_person_pr_counts_include_cursor_coauthored_prs_once_each(self):
        response = {
            "authored": {"issueCount": 3},
            "reviewed": {
                "nodes": [],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        }
        cursor_prs = [
            self._cursor_pr("alice", "Alice"),
            self._cursor_pr("ALICE"),
            self._cursor_pr("bob"),
        ]

        with (
            patch.object(github, "token", "token"),
            patch.object(github, "get_github_orgs", return_value=["apollosproject"]),
            patch.object(github, "_execute", return_value=response),
            patch.object(github, "_get_cursor_authored_merged_prs", return_value=cursor_prs),
        ):
            counts = github.get_merged_pr_counts_for_user("Alice", 30)

        self.assertEqual(counts, (5, 0))

    def test_merged_pr_activity_fetches_once_and_groups_authors_and_reviewers(self):
        prs = [
            {
                "author": {"login": "alice"},
                "reviews": {
                    "nodes": [
                        {"author": {"login": "bob"}, "state": "APPROVED"},
                        {"author": {"login": "BOB"}, "state": "APPROVED"},
                    ]
                },
            },
            {
                "author": {"login": "bob"},
                "reviews": {"nodes": [{"author": {"login": "alice"}, "state": "APPROVED"}]},
            },
        ]

        cursor_prs = [self._cursor_pr("ALICE"), self._cursor_pr("CARA"), self._cursor_pr("Cara")]
        with (
            patch.object(github, "_get_merged_prs", return_value=prs) as fetch,
            patch.object(github, "_get_cursor_authored_merged_prs", return_value=cursor_prs),
        ):
            authored, reviewed = github.get_merged_pr_activity(30)

        fetch.assert_called_once_with(30, None)
        self.assertEqual(list(authored), ["alice", "bob", "CARA"])
        self.assertEqual(list(reviewed), ["bob", "alice"])
        self.assertEqual((len(authored["alice"]), len(authored["CARA"])), (2, 2))
        self.assertEqual(len(reviewed["bob"]), 1)

    def test_merged_pr_search_splits_date_ranges_above_github_limit(self):
        window = TimeWindow.from_dates(date(2026, 1, 1), date(2026, 1, 4))
        searches = []

        def execute(_query, variable_values):
            search = variable_values["query"]
            searches.append(search)
            if "merged:2026-01-01..2026-01-04" in search:
                return {
                    "search": {
                        "issueCount": 1500,
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            author = "alice" if "merged:2026-01-01..2026-01-02" in search else "bob"
            return {
                "search": {
                    "issueCount": 1,
                    "nodes": [{"author": {"login": author}, "reviews": {"nodes": []}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }

        with (
            patch.object(github, "token", "token"),
            patch.object(github, "get_github_orgs", return_value=["apollosproject"]),
            patch.object(github, "_execute", side_effect=execute),
        ):
            prs = github._get_merged_prs(window=window)

        self.assertEqual([pr["author"]["login"] for pr in prs], ["alice", "bob"])
        self.assertEqual(len(searches), 3)
        self.assertTrue(any("merged:2026-01-01..2026-01-02" in q for q in searches))
        self.assertTrue(any("merged:2026-01-03..2026-01-04" in q for q in searches))

    def test_complete_merged_pr_search_fails_closed_on_api_error(self):
        with patch.object(github, "_execute", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(github.GitHubDataError, "boom"):
                github._search_prs(None, "query", require_complete=True)

    @staticmethod
    def _cursor_pr(*coauthors):
        return {
            "author": {"login": "cursor"},
            "commits": {
                "nodes": [
                    {
                        "commit": {
                            "author": {"user": {"login": "cursoragent"}},
                            "authors": {
                                "nodes": [
                                    {"user": {"login": login}}
                                    for login in ("cursoragent", *coauthors)
                                ]
                            },
                        }
                    }
                ]
            },
        }

    def test_cursor_coauthors_require_cursor_app_and_generated_first_commit(self):
        human_pr = self._cursor_pr("alice")
        human_pr["author"] = {"login": "bob"}
        human_first_commit = self._cursor_pr("alice")
        human_first_commit["commits"]["nodes"][0]["commit"]["author"] = {"user": {"login": "bob"}}
        later_cursor_commit = self._cursor_pr()
        later_cursor_commit["commits"]["nodes"].append(
            self._cursor_pr("alice")["commits"]["nodes"][0]
        )

        self.assertEqual(github._cursor_coauthors(human_pr), [])
        self.assertEqual(github._cursor_coauthors(human_first_commit), [])
        self.assertEqual(github._cursor_coauthors(later_cursor_commit), [])

    def test_github_client_allows_slow_repository_queries(self):
        previous_client = getattr(github._thread_local, "client", None)
        if hasattr(github._thread_local, "client"):
            del github._thread_local.client
        try:
            with patch.object(github, "AIOHTTPTransport") as transport:
                with patch.object(github, "Client") as client:
                    github._get_client()

            client.assert_called_once_with(
                transport=transport.return_value,
                fetch_schema_from_transport=False,
                execute_timeout=github.GITHUB_GRAPHQL_EXECUTE_TIMEOUT_SECONDS,
            )
        finally:
            if previous_client is not None:
                github._thread_local.client = previous_client
            elif hasattr(github._thread_local, "client"):
                del github._thread_local.client

    def test_github_execute_embeds_variables_in_graphql_request(self):
        client = _RecordingClient()
        query = gql("query RepoId($owner: String!) { __typename }")

        with patch.object(github, "_get_client", return_value=client):
            response = github._execute(query, {"owner": "apollosproject"})

        self.assertEqual(response, {"ok": True})
        self.assertEqual(len(client.calls), 1)
        request, kwargs = client.calls[0]
        self.assertIsInstance(request, GraphQLRequest)
        self.assertEqual(request.variable_values, {"owner": "apollosproject"})
        self.assertEqual(kwargs, {})

    def test_linear_execute_embeds_variables_in_graphql_request(self):
        client = _RecordingClient()
        query = gql("query Team($team: String!) { __typename }")

        with patch.object(linear_client, "_get_client", return_value=client):
            response = linear_client._execute(query, {"team": "APO"})

        self.assertEqual(response, {"ok": True})
        self.assertEqual(len(client.calls), 1)
        request, kwargs = client.calls[0]
        self.assertIsInstance(request, GraphQLRequest)
        self.assertEqual(request.variable_values, {"team": "APO"})
        self.assertEqual(kwargs, {})

    def test_execute_without_variables_uses_original_request(self):
        client = _RecordingClient()
        query = gql("query Example { __typename }")

        with patch.object(github, "_get_client", return_value=client):
            response = github._execute(query)

        self.assertEqual(response, {"ok": True})
        self.assertEqual(len(client.calls), 1)
        request, kwargs = client.calls[0]
        self.assertIs(request, query)
        self.assertEqual(kwargs, {})


if __name__ == "__main__":
    unittest.main()
