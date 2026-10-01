"""Rank open PRs into one shared "what should I review?" list."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from github import (
    _parse_github_timestamp,
    get_active_change_request_reviewers,
    has_known_merge_conflicts,
    has_required_approval,
)

READY = "ready"
CI_RUNNING = "ci_running"
NOT_READY = "not_ready"
APPROVED = "approved"

# Linear priority: 1 Urgent, 2 High, 3 Medium, 4 Low, 0 No priority.
PRIORITY_BANDS = ((1, "Urgent"), (2, "High"), (3, "Medium"), (4, "Low"), (0, "No priority"))

# (max changed lines, label); smallest first so quick reviews clear fastest.
SIZE_BUCKETS: tuple[tuple[int | None, str], ...] = ((50, "XS"), (200, "S"), (500, "M"), (None, "L"))


def ticket_number(pr: dict[str, Any], team_key: str) -> int | None:
    """Return the Linear issue number from the branch name, else the title."""
    pattern = re.compile(rf"\b{re.escape(team_key)}-(\d+)\b", re.IGNORECASE)
    for text in (pr.get("headRefName"), pr.get("title")):
        match = pattern.search(text or "")
        if match:
            return int(match.group(1))
    return None


def ci_state(pr: dict[str, Any]) -> str:
    rollup = pr.get("statusCheckRollup")
    if rollup is None:
        return "none"
    state = rollup.get("state")
    if state == "SUCCESS":
        return "passing"
    if state in ("PENDING", "EXPECTED"):
        return "running"
    return "failing"


def _requested_reviewers(pr: dict[str, Any]) -> list[str]:
    return [
        login
        for request in (pr.get("reviewRequests") or {}).get("nodes", [])
        if (login := (request.get("requestedReviewer") or {}).get("login"))
    ]


def _is_approved(pr: dict[str, Any]) -> bool:
    if has_required_approval(pr):
        return True
    # Repos without required reviews leave reviewDecision empty; use each reviewer's latest review.
    if pr.get("reviewDecision") is not None:
        return False
    latest: dict[str, tuple[datetime, str]] = {}
    for review in (pr.get("reviews") or {}).get("nodes", []):
        login = (review.get("author") or {}).get("login")
        submitted_at = _parse_github_timestamp(review.get("submittedAt"))
        if not login or submitted_at is None:
            continue
        if review.get("state") not in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            continue
        if login not in latest or submitted_at >= latest[login][0]:
            latest[login] = (submitted_at, review["state"])
    states = {state for _, state in latest.values()}
    return "APPROVED" in states and "CHANGES_REQUESTED" not in states


def classify(pr: dict[str, Any]) -> tuple[str, str | None]:
    """Return (section, not-ready reason) for one PR."""
    if _is_approved(pr):
        return APPROVED, None
    default_branch = ((pr.get("repository") or {}).get("defaultBranchRef") or {}).get("name")
    if pr.get("baseRefName") != default_branch:
        return NOT_READY, "Stacked"
    if has_known_merge_conflicts(pr):
        return NOT_READY, "Conflicts"
    ci = ci_state(pr)
    if ci == "failing":
        return NOT_READY, "CI failing"
    change_requesters = get_active_change_request_reviewers(pr)
    # Each change request blocks until that reviewer is re-requested.
    if change_requesters - set(_requested_reviewers(pr)):
        return NOT_READY, "Changes requested"
    if ci == "running":
        return CI_RUNNING, None
    return READY, None


def _counts_toward_wait(node: dict[str, Any], team_logins: set[str]) -> bool:
    if node.get("__typename") == "ReadyForReviewEvent":
        return True
    # Bots such as mary-pr-poppins are re-requested on every push; only teammates reset the clock.
    login = (node.get("requestedReviewer") or {}).get("login") or ""
    return login.lower() in team_logins


def waiting_since(pr: dict[str, Any], team_logins: set[str]) -> datetime | None:
    """Latest ready-for-review or teammate review request, else when the PR was opened."""
    events = [
        timestamp
        for node in (pr.get("timelineItems") or {}).get("nodes", [])
        if _counts_toward_wait(node, team_logins)
        and (timestamp := _parse_github_timestamp(node.get("createdAt")))
    ]
    if events:
        return max(events)
    return _parse_github_timestamp(pr.get("createdAt"))


def size_bucket(changed_lines: int) -> tuple[int, str]:
    for rank, (limit, label) in enumerate(SIZE_BUCKETS):
        if limit is None or changed_lines <= limit:
            return rank, label
    raise AssertionError("SIZE_BUCKETS must end with an unbounded bucket")


def _format_waiting(hours: int) -> str:
    return f"{hours}h" if hours < 24 else f"{hours // 24}d"


def _row(
    pr: dict[str, Any], issue: dict[str, Any] | None, now: datetime, team_logins: set[str]
) -> dict[str, Any]:
    additions = pr.get("additions") or 0
    deletions = pr.get("deletions") or 0
    size_rank, size_label = size_bucket(additions + deletions)
    since = waiting_since(pr, team_logins) or now
    hours = max(int((now - since).total_seconds() // 3600), 0)
    return {
        "url": pr.get("url"),
        "repo": ((pr.get("repository") or {}).get("nameWithOwner") or "").split("/")[-1],
        "number": pr.get("number"),
        "title": pr.get("title"),
        "author": (pr.get("author") or {}).get("login"),
        "base": pr.get("baseRefName"),
        "issue": issue,
        "priority": int((issue or {}).get("priority") or 0),
        "additions": additions,
        "deletions": deletions,
        "size": size_label,
        "size_rank": size_rank,
        "waiting_hours": hours,
        "waiting": _format_waiting(hours),
        "ci": ci_state(pr),
        "reviewers": [login for login in _requested_reviewers(pr) if login.lower() in team_logins],
        "reason": None,
    }


def _review_order(row: dict[str, Any]) -> tuple[int, int, int]:
    band = next(rank for rank, (value, _) in enumerate(PRIORITY_BANDS) if value == row["priority"])
    return band, row["size_rank"], -row["waiting_hours"]


def build_review_queue(
    prs: list[dict[str, Any]],
    issues_by_number: dict[int, dict[str, Any]],
    now: datetime,
    team_key: str,
    team_logins: set[str],
) -> dict[str, Any]:
    """Group PRs into ready (by Linear priority), CI running, not ready, and approved."""
    sections: dict[str, list[dict[str, Any]]] = {
        READY: [],
        CI_RUNNING: [],
        NOT_READY: [],
        APPROVED: [],
    }
    for pr in prs:
        section, reason = classify(pr)
        number = ticket_number(pr, team_key)
        row = _row(pr, issues_by_number.get(number) if number else None, now, team_logins)
        row["reason"] = reason
        sections[section].append(row)

    ready = sorted(sections[READY], key=_review_order)
    return {
        "ready_groups": [
            {"label": label, "rows": rows}
            for value, label in PRIORITY_BANDS
            if (rows := [row for row in ready if row["priority"] == value])
        ],
        "ready_count": len(ready),
        "running": sorted(sections[CI_RUNNING], key=_review_order),
        "not_ready": sorted(sections[NOT_READY], key=lambda row: -row["waiting_hours"]),
        "approved": sorted(sections[APPROVED], key=lambda row: -row["waiting_hours"]),
    }


def filter_queue(
    queue: dict[str, Any], author: str | None = None, reviewer: str | None = None
) -> dict[str, Any]:
    """Keep rows authored by ``author`` and/or awaiting ``reviewer`` (GitHub logins)."""
    author = (author or "").lower()
    reviewer = (reviewer or "").lower()
    if not author and not reviewer:
        return queue

    def keep(row: dict[str, Any]) -> bool:
        if author and (row["author"] or "").lower() != author:
            return False
        return not reviewer or reviewer in {login.lower() for login in row["reviewers"]}

    groups = [
        {**group, "rows": rows}
        for group in queue["ready_groups"]
        if (rows := [row for row in group["rows"] if keep(row)])
    ]
    return {
        "ready_groups": groups,
        "ready_count": sum(len(group["rows"]) for group in groups),
        **{
            section: [row for row in queue[section] if keep(row)]
            for section in ("running", "not_ready", "approved")
        },
    }
