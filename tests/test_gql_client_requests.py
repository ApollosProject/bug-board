import unittest
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


if __name__ == "__main__":
    unittest.main()
