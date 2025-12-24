import os
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List
import requests
import threading
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


token = os.getenv("GITHUB_TOKEN")
headers = {"Authorization": f"bearer {token}"}


_thread_local = threading.local()


def _get_client():
    client = getattr(_thread_local, "client", None)
    if client is None:
        transport = AIOHTTPTransport(
            url="https://api.github.com/graphql",
            headers=headers,
        )
        client = Client(transport=transport, fetch_schema_from_transport=False)
        _thread_local.client = client
    return client


# headers used for REST API requests
rest_headers = {
    "Authorization": f"bearer {token}",
    "Accept": "application/vnd.github.v3.diff",
    # Use the latest stable REST API version
    "X-GitHub-Api-Version": "2022-11-28",
}


@lru_cache(maxsize=1)
def get_repo_ids():
    # List of repositories to track in the format "owner/name".
    repos = [
        "apollosproject/apollos-platforms",
        "apollosproject/apollos-cluster",
        "apollosproject/apollos-admin",
        "apollosproject/admin-transcriptions",
        "apollosproject/apollos-shovel",
        "apollosproject/apollos-embeds",
        "differential/crossroads-anywhere",
    ]
    ids = []
    # GraphQL query for fetching a repository ID by owner and name.
    repo_id_query = gql(
        """
        query RepoId($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                id
            }
        }
        """
    )
    for full_name in repos:
        try:
            owner, name = full_name.split("/", 1)
        except ValueError:
            # Skip invalid entries.
            continue
        params = {"owner": owner, "name": name}
        data = _get_client().execute(repo_id_query, variable_values=params)
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
                            additions
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
    data = _get_client().execute(query, variable_values=params)
    prs = data["node"]["pullRequests"]["nodes"]
    non_draft_prs = [pr for pr in prs if not pr.get("isDraft", False)]
    return non_draft_prs


def has_failing_required_checks(pr):
    """Return True if the PR has any failing required checks."""

    rollup = pr.get("statusCheckRollup") or {}
    return rollup.get("state") != "SUCCESS"


def _get_all_prs(pr_states: List[str]) -> List[Dict[str, Any]]:
    """Fetch PRs for all tracked repositories concurrently."""
    repo_ids = get_repo_ids()
    with ThreadPoolExecutor(max_workers=len(repo_ids)) as executor:
        futures = [
            executor.submit(get_prs, repo_id, pr_states) for repo_id in repo_ids
        ]
        all_prs: List[Dict[str, Any]] = []
        for future in futures:
            all_prs.extend(future.result())
    return all_prs


def prs_by_approver():
    all_prs = _get_all_prs(["MERGED"])
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
    all_prs = _get_all_prs(["MERGED"])

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


def merged_prs_by_author(days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
    """Return merged PRs grouped by author within the given timeframe."""
    prs = _get_merged_prs(days)
    prs_by_author: Dict[str, List[Dict[str, Any]]] = {}
    for pr in prs:
        author = pr.get("author", {}).get("login")
        if not author:
            continue
        prs_by_author.setdefault(author, []).append(pr)
    return prs_by_author


def merged_prs_by_reviewer(days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
    """Return merged PRs grouped by reviewer within the given timeframe."""
    prs = _get_merged_prs(days)
    prs_by_reviewer: Dict[str, List[Dict[str, Any]]] = {}
    for pr in prs:
        for review in pr.get("reviews", {}).get("nodes", []):
            if review.get("author") and review.get("state") == "APPROVED":
                reviewer = review["author"]["login"]
                prs_by_reviewer.setdefault(reviewer, []).append(pr)
    return prs_by_reviewer


def get_prs_waiting_for_review_by_reviewer():
    """Return PRs waiting on review, grouped by reviewer.

    Includes pull requests with an open review request that was made more
    than 12 hours ago, even if the PR has previously been reviewed. Only
    includes PRs with fewer than 200 lines added.
    """
    all_prs = _get_all_prs(["OPEN"])
    stuck_prs = {}
    for pr in all_prs:
        additions = pr.get("additions")
        if additions is None or additions >= 200:
            continue
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
    all_prs = _get_all_prs(["OPEN"])
    cr_prs = {}
    for pr in all_prs:
        for review in pr.get("reviews", {}).get("nodes", []):
            if review.get("author") and review.get("state") == "CHANGES_REQUESTED":
                reviewer = review["author"]["login"]
                cr_prs.setdefault(reviewer, []).append(pr)
    return cr_prs


def get_pr_diff(owner: str, repo: str, number: int) -> str:
    """Return the diff for a pull request."""
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}"
    resp = requests.get(url, headers=rest_headers)
    resp.raise_for_status()
    return resp.text
