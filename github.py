import os
from datetime import datetime, timedelta

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


def get_prs(repo_id, pr_states):
    params = {"repo_id": repo_id, "pr_states": pr_states}
    query = gql(
        """
        query PRs ($repo_id: ID!, $pr_states: [PullRequestState!]) {
            node(id: $repo_id) {
                ... on Repository {
                    pullRequests(first: 100, states: $pr_states, orderBy: {field: UPDATED_AT, direction: DESC}) {
                        nodes {
                            title
                            url
                            closedAt
                            isDraft
                            reviews(first: 10, states: [APPROVED]) {
                                nodes {
                                    author {
                                        login
                                    }
                                }
                            }
                            timelineItems(first: 50, itemTypes: [REVIEW_REQUESTED_EVENT]) {
                              nodes {
                                ... on ReviewRequestedEvent {
                                  createdAt
                                  requestedReviewer {
                                    ... on User {
                                      login
                                    }
                                  }
                                }
                              }
                            }
                            reviewRequests(first: 10) {
                                nodes {
                                    requestedReviewer {
                                        ... on User {
                                            login
                                        }
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
    prs = data["node"]["pullRequests"]["nodes"]
    non_draft_prs = [pr for pr in prs if not pr.get("isDraft", False)]
    return non_draft_prs


def prs_by_approver():
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id, pr_states=["MERGED"])
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


def get_prs_waiting_for_review_by_reviewer():
    """Returns dictonary of PRs waiting on review, grouped by reviewer, if they have been sitting for 24 hours, and there's no other approvals"""
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id, pr_states=["OPEN"])
        all_prs.extend(prs)
    stuck_prs = {}
    for pr in all_prs:
        if pr["reviews"]["nodes"] or not pr["reviewRequests"]["nodes"]:
            continue
        for review in pr["timelineItems"]["nodes"]:
            if review["createdAt"] < (datetime.now() - timedelta(hours=12)).isoformat():
                reviewer = review["requestedReviewer"]["login"]
                open_review_requests = [
                    req["requestedReviewer"]["login"]
                    for req in pr["reviewRequests"]["nodes"]
                ]
                if reviewer not in open_review_requests:
                    continue
                if reviewer not in stuck_prs:
                    stuck_prs[reviewer] = []
                stuck_prs[reviewer].append(pr)
    return stuck_prs
