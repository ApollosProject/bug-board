import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from gql import Client, GraphQLRequest, gql
from gql.transport.aiohttp import AIOHTTPTransport

from config import get_github_orgs

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


def _execute(query, variable_values=None):
    client = _get_client()
    if variable_values is None:
        return client.execute(query)
    request = GraphQLRequest(query, variable_values=variable_values)
    return client.execute(request)


# headers used for REST API requests
rest_headers = {
    "Authorization": f"bearer {token}",
    "Accept": "application/vnd.github.v3.diff",
    # Use the latest stable REST API version
    "X-GitHub-Api-Version": "2022-11-28",
}


@lru_cache(maxsize=1)
def get_repo_ids():
    if not token:
        return []
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
        try:
            data = _execute(repo_id_query, variable_values=params)
        except Exception:
            continue
        ids.append(data["repository"]["id"])
    return ids


def get_prs(repo_id, pr_states):
    if not token:
        return []
    query = gql(
        """
        query PRs ($repo_id: ID!, $pr_states: [PullRequestState!], $cursor: String) {
            node(id: $repo_id) {
                ... on Repository {
                    pullRequests(
                        first: 100,
                        after: $cursor,
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
                                    submittedAt
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
                            reviewDecision
                            statusCheckRollup {
                                state
                            }
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                    }
                }
            }
        }
    """
    )
    all_prs = []
    cursor = None
    while True:
        params = {"repo_id": repo_id, "pr_states": pr_states, "cursor": cursor}
        try:
            data = _execute(query, variable_values=params)
        except Exception:
            return []
        payload = data["node"]["pullRequests"]
        all_prs.extend(payload["nodes"])
        page_info = payload["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    non_draft_prs = [pr for pr in all_prs if not pr.get("isDraft", False)]
    return non_draft_prs


def has_failing_required_checks(pr):
    """Return True if the PR has any failing required checks."""

    rollup = pr.get("statusCheckRollup") or {}
    return rollup.get("state") != "SUCCESS"


def has_known_merge_conflicts(pr):
    """Return True only when GitHub has confirmed the PR cannot merge cleanly."""

    return pr.get("mergeable") == "CONFLICTING"


def _parse_github_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def get_active_change_request_reviewers(pr):
    """Return reviewers with currently active requested-changes reviews."""

    review_decision = pr.get("reviewDecision")
    if review_decision and review_decision != "CHANGES_REQUESTED":
        return set()

    review_request_times_by_reviewer: dict[str, list[datetime]] = {}
    for review_request in pr.get("timelineItems", {}).get("nodes", []):
        requested_reviewer = review_request.get("requestedReviewer") or {}
        reviewer = requested_reviewer.get("login")
        requested_at = _parse_github_timestamp(review_request.get("createdAt"))
        if reviewer and requested_at is not None:
            review_request_times_by_reviewer.setdefault(reviewer, []).append(requested_at)

    has_change_request_review = False
    active_reviewers = set()
    for review in pr.get("reviews", {}).get("nodes", []):
        if review.get("state") != "CHANGES_REQUESTED":
            continue
        has_change_request_review = True
        reviewer = (review.get("author") or {}).get("login")
        submitted_at = _parse_github_timestamp(review.get("submittedAt"))
        if not reviewer or submitted_at is None:
            continue
        latest_review_request_at = max(
            review_request_times_by_reviewer.get(reviewer, []),
            default=None,
        )
        if latest_review_request_at is None or submitted_at >= latest_review_request_at:
            active_reviewers.add(reviewer)
    if review_decision == "CHANGES_REQUESTED" and not has_change_request_review:
        return {
            (req.get("requestedReviewer") or {}).get("login")
            for req in pr.get("reviewRequests", {}).get("nodes", [])
            if (req.get("requestedReviewer") or {}).get("login")
        }
    return active_reviewers


def has_active_change_request(pr):
    """Return True when GitHub says the PR is blocked by requested changes."""

    return bool(get_active_change_request_reviewers(pr))


def _get_all_prs(pr_states: List[str]) -> List[Dict[str, Any]]:
    """Fetch PRs for all tracked repositories concurrently."""
    repo_ids = get_repo_ids()
    if not repo_ids:
        return []
    with ThreadPoolExecutor(max_workers=len(repo_ids)) as executor:
        futures = [executor.submit(get_prs, repo_id, pr_states) for repo_id in repo_ids]
        all_prs: List[Dict[str, Any]] = []
        for future in futures:
            try:
                all_prs.extend(future.result())
            except Exception:
                continue
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


def _get_merged_prs(days: int = 30):
    """Return merged PRs within the last ``days`` days using GitHub search."""
    if not token:
        return []
    orgs = get_github_orgs()
    if not orgs:
        return []
    cutoff = datetime.utcnow() - timedelta(days=days)
    cutoff_date = cutoff.date().isoformat()
    org_filter = " ".join(f"org:{org}" for org in orgs)
    search_query = f"{org_filter} is:pr is:merged merged:>={cutoff_date}"
    query = gql(
        """
        query SearchMergedPRs($query: String!, $cursor: String) {
          search(type: ISSUE, query: $query, first: 100, after: $cursor) {
            nodes {
              ... on PullRequest {
                author { login }
                reviews(first: 10, states: [APPROVED]) {
                  nodes {
                    author { login }
                    state
                  }
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
    )
    prs = []
    cursor = None
    max_pages = 10
    pages = 0
    while True:
        try:
            data = _execute(query, variable_values={"query": search_query, "cursor": cursor})
        except Exception:
            return []
        payload = data.get("search", {}) or {}
        nodes = payload.get("nodes", []) or []
        for node in nodes:
            if node:
                prs.append(node)
        page_info = payload.get("pageInfo", {}) or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        pages += 1
        if pages >= max_pages:
            break
    return prs


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

    Includes pull requests with an open review request or active requested-changes
    reviewer that has been waiting more than 24 hours, even if the PR has
    previously been reviewed. Only includes PRs with fewer than 200 lines added.
    """
    all_prs = _get_all_prs(["OPEN"])
    stuck_prs = {}
    threshold = datetime.now(timezone.utc) - timedelta(hours=24)
    for pr in all_prs:
        additions = pr.get("additions")
        if additions is None or additions >= 200:
            continue
        if has_known_merge_conflicts(pr):
            continue
        active_change_request_reviewers = get_active_change_request_reviewers(pr)
        if not pr["reviewRequests"]["nodes"] and not active_change_request_reviewers:
            continue
        if has_failing_required_checks(pr):
            # waiting on author to fix checks
            continue
        for review in pr["timelineItems"]["nodes"]:
            requested_at = _parse_github_timestamp(review.get("createdAt"))
            if (
                review["requestedReviewer"]
                and requested_at is not None
                and requested_at < threshold
            ):
                reviewer = review["requestedReviewer"]["login"]
                open_review_requests = [
                    req["requestedReviewer"]["login"] for req in pr["reviewRequests"]["nodes"]
                ]
                if (
                    active_change_request_reviewers
                    and reviewer not in active_change_request_reviewers
                ):
                    continue
                if not active_change_request_reviewers and reviewer not in open_review_requests:
                    continue
                if reviewer not in stuck_prs:
                    stuck_prs[reviewer] = []
                stuck_prs[reviewer].append(pr)
    return stuck_prs


def get_pr_diff(owner: str, repo: str, number: int) -> str:
    """Return the diff for a pull request."""
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}"
    resp = requests.get(url, headers=rest_headers)
    resp.raise_for_status()
    return resp.text
