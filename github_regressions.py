from __future__ import annotations

import os
import re
import time
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from gql import gql

from github import _execute

load_dotenv()

GITHUB_API_URL = "https://api.github.com"
GITHUB_REQUEST_TIMEOUT_SECONDS = 30
MAX_FIX_FILES = 50
PR_URL_RE = re.compile(
    r"^https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/pull/(?P<number>\d+)/?$",
    re.IGNORECASE,
)


class GitHubRegressionDataError(RuntimeError):
    """Raised when GitHub cannot provide complete regression-attribution data."""


def parse_pull_request_url(url: str) -> tuple[str, str, int] | None:
    match = PR_URL_RE.match(url.strip())
    if not match:
        return None
    return match["owner"], match["repo"], int(match["number"])


def deleted_line_numbers(patch: str | None) -> list[int]:
    """Return old-file line numbers deleted by a unified diff patch."""

    if not patch:
        return []

    deleted: list[int] = []
    old_line: int | None = None
    for line in patch.splitlines():
        if line.startswith("@@"):
            match = re.match(r"@@ -(\d+)(?:,\d+)? \+\d+(?:,\d+)? @@", line)
            old_line = int(match.group(1)) if match else None
            continue
        if old_line is None or line.startswith("\\"):
            continue
        if line.startswith("-"):
            deleted.append(old_line)
            old_line += 1
        elif line.startswith("+"):
            continue
        else:
            old_line += 1
    return deleted


def _rest_headers() -> dict[str, str]:
    token = os.getenv("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ApollosProject-Bug-Board",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _rest_get(path: str, params: dict[str, Any] | None = None) -> Any:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(
                f"{GITHUB_API_URL}{path}",
                headers=_rest_headers(),
                params=params,
                timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            status_code = getattr(exc.response, "status_code", None)
            retryable = status_code is None or status_code == 429 or status_code >= 500
            if attempt < 2 and retryable:
                retry_after = getattr(exc.response, "headers", {}).get("Retry-After")
                time.sleep(float(retry_after) if retry_after else 2**attempt)
                continue
            break
    error_response = getattr(last_error, "response", None)
    status = getattr(error_response, "status_code", None)
    detail = f" (HTTP {status})" if status is not None else ""
    raise GitHubRegressionDataError(
        f"GitHub REST request failed for {path}{detail}"
    ) from last_error


def _get_pull_request_files(owner: str, repo: str, number: int) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page = 1
    while len(files) < MAX_FIX_FILES:
        payload = _rest_get(
            f"/repos/{owner}/{repo}/pulls/{number}/files",
            params={"per_page": 100, "page": page},
        )
        if not isinstance(payload, list):
            raise GitHubRegressionDataError(
                f"GitHub returned invalid file data for {owner}/{repo}#{number}"
            )
        files.extend(item for item in payload if isinstance(item, dict))
        if len(payload) < 100:
            break
        page += 1
    return files[:MAX_FIX_FILES]


BLAME_QUERY = gql(
    """
    query RegressionBlame(
      $owner: String!,
      $repo: String!,
      $oid: GitObjectID!,
      $path: String!
    ) {
      repository(owner: $owner, name: $repo) {
        object(oid: $oid) {
          ... on Commit {
            blame(path: $path) {
              ranges {
                startingLine
                endingLine
                commit {
                  committedDate
                  associatedPullRequests(first: 10) {
                    nodes {
                      url
                      mergedAt
                      author { login }
                      reviews(first: 100, states: [APPROVED]) {
                        nodes { author { login } }
                      }
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


def _get_blame_ranges(owner: str, repo: str, oid: str, path: str) -> list[dict[str, Any]]:
    data = None
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            data = _execute(
                BLAME_QUERY,
                variable_values={"owner": owner, "repo": repo, "oid": oid, "path": path},
            )
            break
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2**attempt)
    if data is None:
        raise GitHubRegressionDataError(
            f"GitHub blame failed for {owner}/{repo}:{path}"
        ) from last_error

    repository = data.get("repository") if data else None
    commit = repository.get("object") if repository else None
    blame = commit.get("blame") if commit else None
    ranges = blame.get("ranges") if blame else None
    return [item for item in ranges or [] if isinstance(item, dict)]


def _parse_github_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _is_bot(login: str | None) -> bool:
    return bool(login and (login.casefold().endswith("[bot]") or login.casefold() == "dependabot"))


def _reviewer_logins(pull_request: dict[str, Any]) -> list[str]:
    author = ((pull_request.get("author") or {}).get("login") or "").casefold()
    reviewers = {
        login
        for review in (pull_request.get("reviews") or {}).get("nodes", []) or []
        if isinstance(review, dict)
        for login in [((review.get("author") or {}).get("login") or "")]
        if login and login.casefold() != author and not _is_bot(login)
    }
    return sorted(reviewers, key=str.casefold)


def _original_merged_pull_request(
    commit: dict[str, Any], fixing_merged_at: datetime
) -> dict[str, Any] | None:
    committed_at = _parse_github_datetime(commit.get("committedDate"))
    pull_requests = []
    for pull_request in (commit.get("associatedPullRequests") or {}).get("nodes", []) or []:
        if not isinstance(pull_request, dict):
            continue
        merged_at = _parse_github_datetime(pull_request.get("mergedAt"))
        if merged_at and merged_at <= fixing_merged_at:
            pull_requests.append((pull_request, merged_at))
    if not pull_requests:
        return None
    after_commit = (
        [item for item in pull_requests if item[1] >= committed_at] if committed_at else []
    )
    return min(after_commit or pull_requests, key=lambda item: item[1])[0]


def _line_overlap_count(deleted_lines: list[int], start: int, end: int) -> int:
    return bisect_right(deleted_lines, end) - bisect_left(deleted_lines, start)


def _candidate_score(
    line_count: int, candidate_merged_at: datetime, fixing_merged_at: datetime
) -> tuple[float, int]:
    age_days = max(int((fixing_merged_at - candidate_merged_at).total_seconds() / 86400), 0)
    return line_count / (1 + age_days / 30), age_days
