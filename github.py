import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, List

from dotenv import load_dotenv
from gql import Client, GraphQLRequest, gql
from gql.transport.aiohttp import AIOHTTPTransport
from tenacity import Retrying, before_sleep_log, stop_after_attempt, wait_exponential

from config import get_github_orgs
from time_window import TimeWindow

load_dotenv()


token = os.getenv("GITHUB_TOKEN")
headers = {"Authorization": f"bearer {token}"}

TRACKED_REPOSITORIES = (
    "apollosproject/apollos-platforms",
    "apollosproject/apollos-plugin",
    "apollosproject/apollos-cluster",
    "apollosproject/apollos-admin",
    "apollosproject/admin-transcriptions",
    "apollosproject/apollos-shovel",
    "apollosproject/apollos-embeds",
    "differential/crossroads-anywhere",
)
GITHUB_GRAPHQL_EXECUTE_TIMEOUT_SECONDS = 30
CURSOR_AGENT_LOGIN = "cursoragent"
_cursor_pr_cache_lock = threading.Lock()


class GitHubDataError(RuntimeError):
    """Raised when GitHub data would otherwise be silently incomplete."""


class _GitHubSearchLimitExceeded(RuntimeError):
    """Raised when a search must be partitioned to stay below GitHub's result cap."""


_thread_local = threading.local()


def _get_client():
    client = getattr(_thread_local, "client", None)
    if client is None:
        transport = AIOHTTPTransport(
            url="https://api.github.com/graphql",
            headers=headers,
        )
        client = Client(
            transport=transport,
            fetch_schema_from_transport=False,
            execute_timeout=GITHUB_GRAPHQL_EXECUTE_TIMEOUT_SECONDS,
        )
        _thread_local.client = client
    return client


def _execute(query, variable_values=None):
    client = _get_client()
    if variable_values is None:
        return client.execute(query)
    request = GraphQLRequest(query, variable_values=variable_values)
    return client.execute(request)


def _retrying(fn, *args, **kwargs):
    return Retrying(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(max=4),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
    )(fn, *args, **kwargs)


def _format_exception(exc: Exception) -> str:
    message = str(exc)
    if message:
        return message
    return type(exc).__name__


def has_known_merge_conflicts(pr):
    """Return True only when GitHub has confirmed the PR cannot merge cleanly."""

    return pr.get("mergeable") == "CONFLICTING"


def has_required_approval(pr):
    """Return True when GitHub says the PR has satisfied review requirements."""

    return pr.get("reviewDecision") == "APPROVED"


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

    latest_reviews: dict[str, tuple[datetime, str]] = {}
    for review in pr.get("reviews", {}).get("nodes", []):
        if review.get("state") not in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            continue
        reviewer = (review.get("author") or {}).get("login")
        submitted_at = _parse_github_timestamp(review.get("submittedAt"))
        if not reviewer or submitted_at is None:
            continue
        if reviewer not in latest_reviews or submitted_at >= latest_reviews[reviewer][0]:
            latest_reviews[reviewer] = (submitted_at, review["state"])

    active_reviewers = set()
    for reviewer, (submitted_at, state) in latest_reviews.items():
        if state != "CHANGES_REQUESTED":
            continue
        latest_review_request_at = max(
            review_request_times_by_reviewer.get(reviewer, []),
            default=None,
        )
        # Re-requesting review does not clear GitHub's CHANGES_REQUESTED decision.
        if (
            review_decision == "CHANGES_REQUESTED"
            or latest_review_request_at is None
            or submitted_at >= latest_review_request_at
        ):
            active_reviewers.add(reviewer)
    return active_reviewers


def _merged_search_qualifier(days: int = 30, window: TimeWindow | None = None) -> str:
    return TimeWindow.resolve(days, window=window).github_merged_qualifier()


def _search_prs(
    query, search_query: str, *, require_complete: bool = False, max_pages: int = 10
) -> List[Dict[str, Any]]:
    prs: List[Dict[str, Any]] = []
    cursor = None
    for _ in range(max_pages):
        try:
            # Retry this page, not the pages already fetched.
            data = _retrying(
                _execute, query, variable_values={"query": search_query, "cursor": cursor}
            )
        except Exception as exc:
            if require_complete:
                raise GitHubDataError(f"GitHub PR search failed: {_format_exception(exc)}") from exc
            return []
        payload = data.get("search", {}) or {}
        if require_complete and (payload.get("issueCount", 0) or 0) > 1000:
            raise _GitHubSearchLimitExceeded
        prs.extend(node for node in payload.get("nodes", []) or [] if node)
        page_info = payload.get("pageInfo", {}) or {}
        next_cursor = page_info.get("endCursor") if page_info.get("hasNextPage") else None
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return prs


REVIEW_QUEUE_SEARCH_PAGE_SIZE = 50  # 100 heavy PR nodes per page intermittently 502s
REVIEW_TIMELINE_FIELDS = """
    pageInfo { hasPreviousPage startCursor }
    nodes {
      __typename
      ... on ReadyForReviewEvent { createdAt }
      ... on ReviewRequestedEvent {
        createdAt
        requestedReviewer { ... on User { login } }
      }
    }
"""
REVIEW_TIMELINE_ARGS = "last: 100, itemTypes: [REVIEW_REQUESTED_EVENT, READY_FOR_REVIEW_EVENT]"
# GitHub silently returns empty or partial timelines when one query asks for more than about
# ten of them (measured 2026-10-01: 45 of 107 admin PRs wrong at 50 per query, 0 at 10).
REVIEW_TIMELINE_BATCH_SIZE = 10


def _attach_review_timelines(batch: List[Dict[str, Any]]) -> None:
    """Fetch the review-request timelines for up to REVIEW_TIMELINE_BATCH_SIZE PRs."""
    timeline = f"timelineItems({REVIEW_TIMELINE_ARGS}) {{ {REVIEW_TIMELINE_FIELDS} }}"
    lookups = " ".join(
        f"pr{i}: node(id: $id{i}) {{ ... on PullRequest {{ {timeline} }} }}"
        for i in range(len(batch))
    )
    variables = ", ".join(f"$id{i}: ID!" for i in range(len(batch)))
    query = gql(f"query ReviewTimelines({variables}) {{ {lookups} }}")
    data = _retrying(
        _execute, query, variable_values={f"id{i}": pr["id"] for i, pr in enumerate(batch)}
    )
    for i, pr in enumerate(batch):
        pr["timelineItems"] = data[f"pr{i}"]["timelineItems"]
        _complete_review_timeline(pr)


def _complete_review_timeline(pr: Dict[str, Any]) -> None:
    """Prepend older review-request events; bot re-requests can push teammates past 100."""
    query = gql(
        "query ReviewTimeline($id: ID!, $before: String) { node(id: $id) {"
        f" ... on PullRequest {{ timelineItems({REVIEW_TIMELINE_ARGS}, before: $before) {{"
        f" {REVIEW_TIMELINE_FIELDS} }} }} }} }}"
    )
    timeline = pr["timelineItems"]
    page_info = timeline.get("pageInfo") or {}
    while page_info.get("hasPreviousPage"):
        data = _retrying(
            _execute, query, variable_values={"id": pr["id"], "before": page_info["startCursor"]}
        )
        page = data["node"]["timelineItems"]
        timeline["nodes"] = page["nodes"] + timeline["nodes"]
        page_info = page.get("pageInfo") or {}


def search_open_prs(include_approved: bool = False) -> List[Dict[str, Any]]:
    """Return open, non-draft PRs across tracked repositories; drafts are filtered by GitHub."""
    if not token:
        return []
    query = gql(
        """
        query ReviewQueuePRs($query: String!, $cursor: String) {
          search(type: ISSUE, query: $query, first: %d, after: $cursor) {
            issueCount
            pageInfo { endCursor hasNextPage }
            nodes {
              ... on PullRequest {
                id
                number
                title
                url
                createdAt
                baseRefName
                headRefName
                additions
                deletions
                mergeable
                reviewDecision
                author { login }
                repository { nameWithOwner defaultBranchRef { name } }
                statusCheckRollup { state }
                # Each reviewer's latest approval, change request, or dismissal. latestReviews
                # would let a later COMMENTED thread reply hide an open change request.
                reviews: latestOpinionatedReviews(first: 100) {
                  nodes { author { login } state submittedAt }
                }
                reviewRequests(first: 100) {
                  nodes { requestedReviewer { ... on User { login } } }
                }
              }
            }
          }
        }
        """
        % REVIEW_QUEUE_SEARCH_PAGE_SIZE
    )
    approval_filter = "" if include_approved else " -review:approved"

    def search_repo(repo: str) -> List[Dict[str, Any]]:
        return _search_prs(
            query,
            f"repo:{repo} is:pr is:open draft:false{approval_filter}",
            require_complete=True,
            # GitHub search returns at most 1,000 results; read all of them.
            max_pages=1000 // REVIEW_QUEUE_SEARCH_PAGE_SIZE,
        )

    with ThreadPoolExecutor(max_workers=len(TRACKED_REPOSITORIES)) as executor:
        prs = [pr for found in executor.map(search_repo, TRACKED_REPOSITORIES) for pr in found]
        batches = [
            prs[start : start + REVIEW_TIMELINE_BATCH_SIZE]
            for start in range(0, len(prs), REVIEW_TIMELINE_BATCH_SIZE)
        ]
        list(executor.map(_attach_review_timelines, batches))
    return prs


def _search_complete_merged_pr_range(
    query,
    org_filter: str,
    start: date,
    end: date,
    *,
    parallel_depth: int = 0,
) -> List[Dict[str, Any]]:
    date_filter = f"merged:{start.isoformat()}..{end.isoformat()}"
    search_query = f"{org_filter} is:pr is:merged {date_filter}"
    try:
        return _search_prs(query, search_query, require_complete=True)
    except _GitHubSearchLimitExceeded:
        if start >= end:
            raise GitHubDataError(
                f"GitHub merged PR search exceeds the 1,000-result limit for {start.isoformat()}"
            ) from None
        midpoint = start + timedelta(days=(end - start).days // 2)
        if parallel_depth > 0:
            with ThreadPoolExecutor(max_workers=2) as executor:
                earlier = executor.submit(
                    _search_complete_merged_pr_range,
                    query,
                    org_filter,
                    start,
                    midpoint,
                    parallel_depth=parallel_depth - 1,
                )
                later = executor.submit(
                    _search_complete_merged_pr_range,
                    query,
                    org_filter,
                    midpoint + timedelta(days=1),
                    end,
                    parallel_depth=parallel_depth - 1,
                )
            return earlier.result() + later.result()
        return _search_complete_merged_pr_range(
            query, org_filter, start, midpoint
        ) + _search_complete_merged_pr_range(query, org_filter, midpoint + timedelta(days=1), end)


@lru_cache(maxsize=32)
def _get_cursor_authored_merged_prs_cached(search_query: str, _minute: int) -> List[Dict[str, Any]]:
    query = gql(
        """
        query CursorAuthoredMergedPRs($query: String!, $cursor: String) {
          search(type: ISSUE, query: $query, first: 100, after: $cursor) {
            nodes {
              ... on PullRequest {
                author { login }
                commits(first: 1) {
                  nodes {
                    commit {
                      author { user { login } }
                      authors(first: 10) {
                        nodes { user { login } }
                      }
                    }
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
    return _search_prs(query, search_query)


def _get_cursor_authored_merged_prs(
    days: int = 30, window: TimeWindow | None = None
) -> List[Dict[str, Any]]:
    orgs = get_github_orgs() if token else []
    if not orgs:
        return []
    org_filter = " ".join(f"org:{org}" for org in orgs)
    search_query = (
        f"{org_filter} is:pr is:merged {_merged_search_qualifier(days, window)} author:app/cursor"
    )
    with _cursor_pr_cache_lock:
        return _get_cursor_authored_merged_prs_cached(search_query, int(time.monotonic() // 60))


def _cursor_coauthors(pr: Dict[str, Any]) -> List[str]:
    pr_author = ((pr.get("author") or {}).get("login") or "").casefold()
    if pr_author != "cursor":
        return []

    commits = (pr.get("commits") or {}).get("nodes", []) or []
    if not commits:
        return []
    commit = (commits[0] or {}).get("commit") or {}
    primary_author = (
        ((commit.get("author") or {}).get("user") or {}).get("login") or ""
    ).casefold()
    if primary_author != CURSOR_AGENT_LOGIN:
        return []

    coauthors: Dict[str, str] = {}
    for author in (commit.get("authors") or {}).get("nodes", []) or []:
        login = ((author.get("user") or {}).get("login") or "").strip()
        normalized_login = login.casefold()
        if normalized_login and normalized_login != primary_author:
            coauthors.setdefault(normalized_login, login)
    return list(coauthors.values())


def _group_cursor_prs_by_coauthor(
    prs: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    prs_by_coauthor: Dict[str, List[Dict[str, Any]]] = {}
    canonical_logins: Dict[str, str] = {}
    for pr in prs:
        for coauthor in _cursor_coauthors(pr):
            coauthor = canonical_logins.setdefault(coauthor.casefold(), coauthor)
            prs_by_coauthor.setdefault(coauthor, []).append(pr)
    return prs_by_coauthor


def _get_merged_prs(days: int = 30, window: TimeWindow | None = None):
    """Return merged PRs within the last ``days`` days using GitHub search."""
    if not token:
        return []
    orgs = get_github_orgs()
    if not orgs:
        return []
    org_filter = " ".join(f"org:{org}" for org in orgs)
    query = gql(
        """
        query SearchMergedPRs($query: String!, $cursor: String) {
          search(type: ISSUE, query: $query, first: 100, after: $cursor) {
            issueCount
            nodes {
              ... on PullRequest {
                author { login }
                reviews(first: 100, states: [APPROVED]) {
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
    resolved_window = TimeWindow.resolve(days, window=window)
    return _search_complete_merged_pr_range(
        query,
        org_filter,
        resolved_window.start.date(),
        resolved_window.inclusive_end_date,
        parallel_depth=2,
    )


def get_merged_pr_counts_for_user(
    username: str, days: int = 30, window: TimeWindow | None = None
) -> tuple[int, int]:
    """Return credited-author and approved-review PR counts for one GitHub user."""
    if not token or not username:
        return 0, 0
    orgs = get_github_orgs()
    if not orgs:
        return 0, 0

    org_filter = " ".join(f"org:{org}" for org in orgs)
    base_query = f"{org_filter} is:pr is:merged {_merged_search_qualifier(days, window)}"
    authored_query = f"{base_query} author:{username}"
    reviewed_query = f"{base_query} reviewed-by:{username}"
    query = gql(
        """
        query MergedPRCounts($authored: String!, $reviewed: String!, $cursor: String) {
          authored: search(type: ISSUE, query: $authored, first: 1) { issueCount }
          reviewed: search(type: ISSUE, query: $reviewed, first: 100, after: $cursor) {
            nodes {
              ... on PullRequest {
                reviews(first: 100, states: [APPROVED]) {
                  nodes { author { login } }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
    )

    authored_count = 0
    reviewed_count = 0
    cursor = None
    normalized_username = username.casefold()
    while True:
        try:
            data = _execute(
                query,
                variable_values={
                    "authored": authored_query,
                    "reviewed": reviewed_query,
                    "cursor": cursor,
                },
            )
        except Exception:
            logging.exception("Failed to fetch merged PR counts for %s", username)
            return 0, 0

        authored = data.get("authored", {}) or {}
        authored_count = authored.get("issueCount", 0) or 0
        reviewed = data.get("reviewed", {}) or {}
        reviewed_count += sum(
            any(
                ((review.get("author") or {}).get("login") or "").casefold() == normalized_username
                for review in (pr.get("reviews") or {}).get("nodes", []) or []
            )
            for pr in reviewed.get("nodes", []) or []
            if pr
        )

        page_info = reviewed.get("pageInfo", {}) or {}
        next_cursor = page_info.get("endCursor") if page_info.get("hasNextPage") else None
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    authored_count += sum(
        len(prs)
        for author, prs in _group_cursor_prs_by_coauthor(
            _get_cursor_authored_merged_prs(days, window)
        ).items()
        if author.casefold() == normalized_username
    )

    return authored_count, reviewed_count


def _group_merged_prs_by_author(prs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    prs_by_author: Dict[str, List[Dict[str, Any]]] = {}
    for pr in prs:
        author = pr.get("author", {}).get("login")
        if not author:
            continue
        prs_by_author.setdefault(author, []).append(pr)
    return prs_by_author


def _group_merged_prs_by_reviewer(prs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    prs_by_reviewer: Dict[str, List[Dict[str, Any]]] = {}
    canonical_logins: Dict[str, str] = {}
    for pr in prs:
        seen_reviewers = set()
        for review in pr.get("reviews", {}).get("nodes", []):
            reviewer = ((review.get("author") or {}).get("login") or "").strip()
            if not reviewer or review.get("state") != "APPROVED":
                continue
            normalized_reviewer = reviewer.casefold()
            if normalized_reviewer in seen_reviewers:
                continue
            seen_reviewers.add(normalized_reviewer)
            reviewer = canonical_logins.setdefault(normalized_reviewer, reviewer)
            prs_by_reviewer.setdefault(reviewer, []).append(pr)
    return prs_by_reviewer


def get_merged_pr_activity(
    days: int = 30,
    window: TimeWindow | None = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    """Return merged PRs grouped by credited author and reviewer."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        prs_future = executor.submit(_get_merged_prs, days, window)
        cursor_prs_future = executor.submit(_get_cursor_authored_merged_prs, days, window)
        prs = prs_future.result()
        cursor_prs = cursor_prs_future.result()
    prs_by_author = _group_merged_prs_by_author(prs)
    canonical_authors = {author.casefold(): author for author in prs_by_author}
    for coauthor, coauthored_prs in _group_cursor_prs_by_coauthor(cursor_prs).items():
        author = canonical_authors.get(coauthor.casefold(), coauthor)
        prs_by_author.setdefault(author, []).extend(coauthored_prs)
    return prs_by_author, _group_merged_prs_by_reviewer(prs)
