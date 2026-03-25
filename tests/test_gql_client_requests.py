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

    def test_github_execute_falls_back_for_gql3_style_clients(self):
        class _FallbackClient:
            def __init__(self):
                self.calls = []

            def execute(self, request, **kwargs):
                self.calls.append((request, kwargs))
                if isinstance(request, GraphQLRequest):
                    raise TypeError("Not an AST Node: <GraphQLRequest instance>.")
                return {"ok": True}

        client = _FallbackClient()
        query = gql("query RepoId($owner: String!) { __typename }")

        with patch.object(github, "_get_client", return_value=client):
            response = github._execute(query, {"owner": "apollosproject"})

        self.assertEqual(response, {"ok": True})
        self.assertEqual(len(client.calls), 2)
        request, kwargs = client.calls[0]
        self.assertIsInstance(request, GraphQLRequest)
        self.assertEqual(request.variable_values, {"owner": "apollosproject"})
        request, kwargs = client.calls[1]
        self.assertIs(request, query)
        self.assertEqual(kwargs, {"variable_values": {"owner": "apollosproject"}})

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


if __name__ == "__main__":
    unittest.main()
