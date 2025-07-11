import os
from datetime import datetime, timedelta
from functools import lru_cache

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


token = os.getenv("GITHUB_TOKEN")
headers = {"Authorization": f"bearer {token}"}
transport = AIOHTTPTransport(
    url="https://api.github.com/graphql",
    headers=headers,
)
client = Client(transport=transport, fetch_schema_from_transport=True)


@lru_cache(maxsize=1)
def get_repo_ids():
    repos = [
        "apollos-platforms",
        "apollos-cluster",
        "apollos-admin",
        "admin-transcriptions",
        "apollos-shovel",
        "apollos-embeds",
    ]
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
                    pullRequests(
                        first: 100,
                        states: $pr_states,
                        orderBy: {field: UPDATED_AT, direction: DESC}
                    ) {
                        nodes {
                            author {
                                login
                            }
                            title
                            url
                            closedAt
                            isDraft
                            reviews(
                                first: 10,
                                states: [APPROVED, CHANGES_REQUESTED]
                            ) {
                                nodes {
                                    author {
                                        login
                                    }
                                    state
                                }
                            }
                            timelineItems(
                                first: 50,
                                itemTypes: [REVIEW_REQUESTED_EVENT],
                            ) {
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
                            number
                            mergeable
                            statusCheckRollup {
                                state
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


def has_failing_required_checks(pr):
    """Return True if the PR has any failing required checks."""

    rollup = pr.get("statusCheckRollup") or {}
    return rollup.get("state") != "SUCCESS"


def prs_by_approver():
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id, pr_states=["MERGED"])
        all_prs.extend(prs)
    prs_by_approver = {}
    for pr in all_prs:
        for review in pr["reviews"]["nodes"]:
            if review.get("author") and review.get("state") == "APPROVED":
                approver = review["author"]["login"]
                prs_by_approver.setdefault(approver, []).append(pr)
    return prs_by_approver


@lru_cache(maxsize=4)
def _get_merged_prs(days: int = 30):
    """Return merged PRs across all repos within the last ``days`` days."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id, pr_states=["MERGED"])
        all_prs.extend(prs)

    filtered = []
    for pr in all_prs:
        closed = pr.get("closedAt")
        if not closed:
            continue
        if closed.endswith("Z"):
            closed = closed[:-1]
        if datetime.fromisoformat(closed) < cutoff:
            continue
        filtered.append(pr)

    return filtered


def merged_prs_by_author(days: int = 30):
    """Return merged PRs grouped by author within the given timeframe."""
    prs = _get_merged_prs(days)
    prs_by_author = {}
    for pr in prs:
        author = pr.get("author", {}).get("login")
        if not author:
            continue
        prs_by_author.setdefault(author, []).append(pr)
    return prs_by_author


def merged_prs_by_reviewer(days: int = 30):
    """Return merged PRs grouped by reviewer within the given timeframe."""
    prs = _get_merged_prs(days)
    prs_by_reviewer = {}
    for pr in prs:
        for review in pr.get("reviews", {}).get("nodes", []):
            if review.get("author") and review.get("state") == "APPROVED":
                reviewer = review["author"]["login"]
                prs_by_reviewer.setdefault(reviewer, []).append(pr)
    return prs_by_reviewer


def get_prs_waiting_for_review_by_reviewer():
    """Return PRs waiting on review, grouped by reviewer.

    Includes pull requests with an open review request that was made more
    than 12 hours ago, even if the PR has previously been reviewed.
    """
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id, pr_states=["OPEN"])
        all_prs.extend(prs)
    stuck_prs = {}
    for pr in all_prs:
        # only consider pull requests that are mergeable
        if pr.get("mergeable") != "MERGEABLE":
            continue
        if not pr["reviewRequests"]["nodes"]:
            continue
        if any(r.get("state") == "APPROVED" for r in pr["reviews"]["nodes"]):
            continue
        if has_failing_required_checks(pr):
            # waiting on author to fix checks
            continue
        for review in pr["timelineItems"]["nodes"]:
            if (
                review["requestedReviewer"]
                and review["createdAt"]
                < (datetime.now() - timedelta(hours=12)).isoformat()
            ):
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


def get_prs_with_changes_requested_by_reviewer():
    """Return open PRs with change requests, grouped by the reviewer who requested changes."""
    repo_ids = get_repo_ids()
    all_prs = []
    for repo_id in repo_ids:
        prs = get_prs(repo_id, pr_states=["OPEN"])
        all_prs.extend(prs)
    cr_prs = {}
    for pr in all_prs:
        for review in pr.get("reviews", {}).get("nodes", []):
            if review.get("author") and review.get("state") == "CHANGES_REQUESTED":
                reviewer = review["author"]["login"]
                cr_prs.setdefault(reviewer, []).append(pr)
    return cr_prs
