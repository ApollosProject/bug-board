import os

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


headers = {"Authorization": f"bearer {os.getenv('GITHUB_TOKEN')}"}
transport = AIOHTTPTransport(url="https://api.github.com/graphql", headers=headers)
client = Client(transport=transport, fetch_schema_from_transport=True)

import pprint


def get_repo_ids():
    repos = ["apollos-platforms"]
    ids = []
    for repo in repos:
        params = {"name": repo}
        query = gql(
            """
            query RepoId ($name: String!) {
                repository(owner: "apollosproject", name: $name) {
                    id
                }
            }
        """
        )
        data = client.execute(query, variable_values=params)
        ids.append(data["repository"]["id"])
    return ids


def get_prs(repo_id):
    params = {"repo_id": repo_id}
    query = gql(
        """
        query PRs ($repo_id: ID!) {
            node(id: $repo_id) {
                ... on Repository {
                    pullRequests(first: 100, states: [MERGED], orderBy: {field: UPDATED_AT, direction: DESC}) {
                        nodes {
                            title
                            closedAt
                            reviews(first: 10, states: [APPROVED]) {
                                nodes {
                                    author {
                                        login
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """
    )
    data = client.execute(query, variable_values=params)
    return data["node"]["pullRequests"]["nodes"]


def prs_by_approver():
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id)
        all_prs.extend(prs)
    prs_by_approver = {}
    for pr in all_prs:
        for review in pr["reviews"]["nodes"]:
            if review["author"]:
                approver = review["author"]["login"]
                if approver not in prs_by_approver:
                    prs_by_approver[approver] = []
                prs_by_approver[approver].append(pr)
    return prs_by_approver


pprint.pprint(prs_by_approver())
