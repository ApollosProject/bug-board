from __future__ import annotations

import os
import re
import time
from typing import Any

import requests
from dotenv import load_dotenv

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
