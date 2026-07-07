import unittest
from datetime import datetime
from unittest.mock import patch

from gql import GraphQLRequest, gql

import github
from linear import client as linear_client


class _RecordingClient:
    def __init__(self):
        self.calls = []

    def execute(self, request, **kwargs):
        self.calls.append((request, kwargs))
        return {"ok": True}


class GraphQLClientRequestTests(unittest.TestCase):
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

    def test_repo_id_lookup_raises_instead_of_caching_partial_tracking_set(self):
        def fake_execute(query, variable_values=None):
            if variable_values["name"] == "apollos-cluster":
                raise RuntimeError("missing access")
            return {"repository": {"id": variable_values["name"]}}

        github.get_repo_ids_by_name.cache_clear()
        try:
            with patch.object(github, "token", "token"):
                with patch.object(github, "_execute", side_effect=fake_execute):
                    with patch.object(github.logging, "exception"):
                        with self.assertRaisesRegex(
                            github.GitHubDataError,
                            "apollosproject/apollos-cluster",
                        ):
                            github.get_repo_ids_by_name()

                    with patch.object(
                        github,
                        "_execute",
                        return_value={"repository": {"id": "ok"}},
                    ):
                        repos = github.get_repo_ids_by_name()

            self.assertEqual(set(repos), set(github.TRACKED_REPOSITORIES))
        finally:
            github.get_repo_ids_by_name.cache_clear()

    def test_repo_id_lookup_raises_when_github_omits_tracked_repository(self):
        github.get_repo_ids_by_name.cache_clear()
        try:
            with patch.object(github, "token", "token"):
                with patch.object(github, "_execute", return_value={"repository": None}):
                    with self.assertRaisesRegex(
                        github.GitHubDataError,
                        "repository was not returned",
                    ):
                        github.get_repo_ids_by_name()
        finally:
            github.get_repo_ids_by_name.cache_clear()

    def test_get_prs_raises_when_repo_fetch_fails(self):
        with patch.object(github, "token", "token"):
            with patch.object(github, "_execute", side_effect=RuntimeError("rate limited")):
                with self.assertRaisesRegex(
                    github.GitHubDataError,
                    "apollosproject/apollos-cluster",
                ):
                    github.get_prs("repo-id", ["OPEN"], "apollosproject/apollos-cluster")

    def test_get_all_prs_raises_when_any_repo_fetch_fails(self):
        def fake_get_prs(repo_id, pr_states, repo_name=None):
            if repo_name == "apollosproject/apollos-cluster":
                raise RuntimeError("rate limited")
            return [{"number": 1, "repo": repo_name}]

        repo_ids = {
            "apollosproject/apollos-platforms": "platforms-id",
            "apollosproject/apollos-cluster": "cluster-id",
        }
        with patch.object(github, "get_repo_ids_by_name", return_value=repo_ids):
            with patch.object(github, "get_prs", side_effect=fake_get_prs):
                with patch.object(github.logging, "warning") as warning:
                    with self.assertRaisesRegex(
                        github.GitHubDataError,
                        "apollosproject/apollos-cluster",
                    ):
                        github._get_all_prs(["OPEN"])
        warning.assert_called_once_with(
            "Failed to fetch GitHub PRs for %s: %s",
            "apollosproject/apollos-cluster",
            "rate limited",
        )

    def test_waiting_for_review_uses_utc_timestamps(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "number": 6050,
            "additions": 1,
            "mergeable": "MERGEABLE",
            "reviewDecision": "REVIEW_REQUIRED",
            "reviewRequests": {
                "nodes": [
                    {"requestedReviewer": {"login": "darrylyip"}},
                    {"requestedReviewer": {"login": "vitlelis"}},
                ]
            },
            "reviews": {"nodes": []},
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "darrylyip"},
                    },
                    {
                        "createdAt": "2026-03-24T13:51:13Z",
                        "requestedReviewer": {"login": "vitlelis"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting["darrylyip"], [pr])
        self.assertEqual(waiting["vitlelis"], [pr])

    def test_waiting_for_review_allows_unknown_mergeability(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "number": 6050,
            "additions": 1,
            "mergeable": "UNKNOWN",
            "reviewDecision": "REVIEW_REQUIRED",
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "darrylyip"}}]},
            "reviews": {"nodes": []},
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "darrylyip"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting["darrylyip"], [pr])

    def test_waiting_for_review_skips_known_merge_conflicts(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "number": 6050,
            "additions": 1,
            "mergeable": "CONFLICTING",
            "reviewDecision": "REVIEW_REQUIRED",
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "darrylyip"}}]},
            "reviews": {"nodes": []},
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "darrylyip"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting, {})

    def test_waiting_for_review_skips_approved_prs_with_open_requests(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "number": 1479,
            "additions": 3,
            "mergeable": "MERGEABLE",
            "reviewDecision": "APPROVED",
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "redreceipt"}}]},
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "solideo-gloria"},
                        "state": "APPROVED",
                        "submittedAt": "2026-03-24T13:52:12Z",
                    }
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "redreceipt"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting, {})

    def test_waiting_for_review_only_notifies_active_change_request_reviewer(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": "CHANGES_REQUESTED",
            "reviewRequests": {
                "nodes": [
                    {"requestedReviewer": {"login": "dylan-manchester"}},
                    {"requestedReviewer": {"login": "michael"}},
                    {"requestedReviewer": {}},
                ]
            },
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "michael"},
                        "state": "APPROVED",
                        "submittedAt": "2026-03-24T13:51:30Z",
                    },
                    {
                        "author": {"login": "dylan-manchester"},
                        "state": "CHANGES_REQUESTED",
                        "submittedAt": "2026-03-24T13:52:12Z",
                    },
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                    {
                        "createdAt": "2026-03-24T13:51:13Z",
                        "requestedReviewer": {"login": "michael"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting["dylan-manchester"], [pr])
        self.assertNotIn("michael", waiting)

    def test_waiting_for_review_allows_cleared_change_requests(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": "REVIEW_REQUIRED",
            "reviewRequests": {
                "nodes": [
                    {"requestedReviewer": {"login": "michael"}},
                    {"requestedReviewer": {"login": "dylan-manchester"}},
                ]
            },
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "michael"},
                        "state": "APPROVED",
                        "submittedAt": "2026-03-24T13:50:12Z",
                    },
                    {
                        "author": {"login": "dylan-manchester"},
                        "state": "CHANGES_REQUESTED",
                        "submittedAt": "2026-03-24T13:52:12Z",
                    },
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "michael"},
                    },
                    {
                        "createdAt": "2026-03-24T13:51:13Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting["michael"], [pr])
        self.assertEqual(waiting["dylan-manchester"], [pr])

    def test_waiting_for_review_skips_change_request_without_open_review_request(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": "CHANGES_REQUESTED",
            "reviewRequests": {"nodes": []},
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "dylan-manchester"},
                        "state": "CHANGES_REQUESTED",
                        "submittedAt": "2026-03-24T13:52:12Z",
                    }
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting, {})

    def test_waiting_for_review_uses_latest_review_request_time(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": "REVIEW_REQUIRED",
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "dylan-manchester"}}]},
            "reviews": {"nodes": []},
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-23T13:51:12Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                    {
                        "createdAt": "2026-03-25T13:00:00Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting, {})

    def test_waiting_for_review_falls_back_to_current_review_nodes_for_missing_decision(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": None,
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "dylan-manchester"}}]},
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "dylan-manchester"},
                        "state": "CHANGES_REQUESTED",
                        "submittedAt": "2026-03-24T13:52:12Z",
                    }
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting["dylan-manchester"], [pr])

    def test_waiting_for_review_ignores_historical_review_nodes_for_missing_decision(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": None,
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "dylan-manchester"}}]},
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "dylan-manchester"},
                        "state": "CHANGES_REQUESTED",
                        "submittedAt": "2026-03-24T12:51:12Z",
                    }
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "dylan-manchester"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting["dylan-manchester"], [pr])

    def test_waiting_for_review_ignores_unrelated_review_requests_for_missing_decision(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 25, 14, 0, 0)
                if tz is None:
                    return base
                return base.replace(tzinfo=tz)

        pr = {
            "additions": 55,
            "mergeable": "MERGEABLE",
            "reviewDecision": None,
            "reviewRequests": {"nodes": [{"requestedReviewer": {"login": "michael"}}]},
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "dylan-manchester"},
                        "state": "CHANGES_REQUESTED",
                        "submittedAt": "2026-03-24T12:51:12Z",
                    }
                ]
            },
            "timelineItems": {
                "nodes": [
                    {
                        "createdAt": "2026-03-24T13:51:12Z",
                        "requestedReviewer": {"login": "michael"},
                    },
                ]
            },
            "statusCheckRollup": {"state": "SUCCESS"},
        }

        with patch.object(github, "_get_all_prs", return_value=[pr]):
            with patch.object(github, "datetime", FixedDateTime):
                waiting = github.get_prs_waiting_for_review_by_reviewer()

        self.assertEqual(waiting, {})


if __name__ == "__main__":
    unittest.main()
