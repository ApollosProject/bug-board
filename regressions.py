from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from config import load_config
from constants import ENGINEERING_TEAM_SLUG
from github import get_merged_pr_counts_for_user
from github_regressions import (
    GitHubRegressionDataError,
    get_fixing_pr_attribution,
    get_pull_request_attribution_metadata,
    parse_pull_request_url,
)
from linear.issues import get_completed_regression_candidates
from time_window import TimeWindow

REGRESSION_DAYS = 30
REGRESSION_OVERRIDES_PATH = Path(__file__).with_name("regression_overrides.yml")
FIXING_LINK_KINDS = {"closes", "contributes"}
MAX_ATTRIBUTION_WORKERS = 2


def extract_fixing_pr_urls(issue: dict[str, Any]) -> list[str]:
    urls = set()
    for attachment in (issue.get("attachments") or {}).get("nodes", []) or []:
        metadata = attachment.get("metadata") if isinstance(attachment, dict) else None
        url = metadata.get("url") if isinstance(metadata, dict) else None
        if (
            metadata
            and metadata.get("status") == "merged"
            and metadata.get("linkKind") in FIXING_LINK_KINDS
            and isinstance(url, str)
            and parse_pull_request_url(url) is not None
        ):
            urls.add(url.rstrip("/"))
    return sorted(urls)


def load_regression_overrides(
    path: str | os.PathLike[str] = REGRESSION_OVERRIDES_PATH,
) -> dict[str, dict[str, Any]]:
    override_path = Path(path)
    if not override_path.exists():
        return {}
    with override_path.open() as stream:
        payload = yaml.safe_load(stream) or {}
    overrides = payload.get("overrides", payload) if isinstance(payload, dict) else {}
    if not isinstance(overrides, dict):
        return {}
    return {
        str(identifier): value for identifier, value in overrides.items() if isinstance(value, dict)
    }


def merge_issue_attributions(
    issue: dict[str, Any],
    fixing_urls: list[str],
    attributions_by_fix_url: dict[str, dict[str, Any] | None],
) -> dict[str, Any]:
    candidates_by_url: dict[str, dict[str, Any]] = {}
    for fixing_url in fixing_urls:
        for incoming in (attributions_by_fix_url.get(fixing_url) or {}).get("candidates", []):
            candidate_url = incoming.get("url")
            if not isinstance(candidate_url, str) or candidate_url in fixing_urls:
                continue
            candidate = candidates_by_url.get(candidate_url)
            if candidate is None:
                candidate = {**incoming, "line_count": 0, "score": 0.0}
                candidate.pop("age_days", None)
                candidates_by_url[candidate_url] = candidate
            candidate["line_count"] += int(incoming.get("line_count") or 0)
            candidate["score"] += float(incoming.get("score") or 0)
    candidates = sorted(
        candidates_by_url.values(),
        key=lambda item: (item.get("score", 0), item.get("line_count", 0)),
        reverse=True,
    )
    return {
        "identifier": issue.get("identifier"),
        "candidates": candidates,
        "attribution": candidates[0] if candidates else None,
    }


def apply_regression_overrides(
    records: list[dict[str, Any]],
    overrides: dict[str, dict[str, Any]],
    metadata_loader: Callable[[str], dict[str, Any] | None] = (
        get_pull_request_attribution_metadata
    ),
) -> list[dict[str, Any]]:
    corrected = []
    for original in records:
        record = dict(original)
        override = overrides.get(str(record.get("identifier")))
        if not override:
            corrected.append(record)
            continue
        if override.get("ignored") is True:
            continue
        inducing_url = override.get("inducing_pr")
        if isinstance(inducing_url, str):
            inducing_url = inducing_url.rstrip("/")
            candidate = next(
                (
                    item
                    for item in record.get("candidates") or []
                    if item.get("url") == inducing_url
                ),
                None,
            )
            if candidate is None:
                try:
                    candidate = metadata_loader(inducing_url)
                except GitHubRegressionDataError as exc:
                    logging.warning("Unable to load regression override: %s", exc)
            if candidate is not None:
                record["attribution"] = candidate
        corrected.append(record)
    return corrected


def collect_regression_attributions(
    window: TimeWindow,
    *,
    overrides_path: str | os.PathLike[str] = REGRESSION_OVERRIDES_PATH,
) -> tuple[list[dict[str, Any]], int]:
    if not os.getenv("LINEAR_API_KEY") or not os.getenv("GITHUB_TOKEN"):
        return [], 0
    issues = get_completed_regression_candidates(window.duration_days, window)
    fixing_urls_by_issue = {str(issue.get("id")): extract_fixing_pr_urls(issue) for issue in issues}
    all_fixing_urls = {url for fixing_urls in fixing_urls_by_issue.values() for url in fixing_urls}
    results: dict[str, dict[str, Any] | None] = {}
    failed_urls = set()
    with ThreadPoolExecutor(
        max_workers=min(MAX_ATTRIBUTION_WORKERS, max(len(all_fixing_urls), 1))
    ) as executor:
        futures = {executor.submit(get_fixing_pr_attribution, url): url for url in all_fixing_urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                result = future.result()
                results[url] = result
                if result is not None and not result.get("complete", True):
                    failed_urls.add(url)
            except Exception as exc:
                logging.warning("Regression attribution failed for %s: %s", url, exc)
                failed_urls.add(url)
                results[url] = None
    records = [
        merge_issue_attributions(
            issue,
            fixing_urls_by_issue[str(issue.get("id"))],
            results,
        )
        for issue in issues
        if fixing_urls_by_issue[str(issue.get("id"))]
    ]
    return (
        apply_regression_overrides(
            records,
            load_regression_overrides(overrides_path),
        ),
        len(failed_urls),
    )


def _engineering_people() -> dict[str, dict[str, str]]:
    return {
        username.casefold(): {"slug": slug, "github_username": username}
        for slug, info in load_config().get("people", {}).items()
        for username in [info.get("github_username")]
        if info.get("team") == ENGINEERING_TEAM_SLUG and isinstance(username, str) and username
    }


def _team_pr_counts(
    people: dict[str, dict[str, str]],
    window: TimeWindow,
) -> tuple[dict[str, int], dict[str, int]]:
    authored: dict[str, int] = {}
    reviewed: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=min(4, max(len(people), 1))) as executor:
        futures = {
            executor.submit(
                get_merged_pr_counts_for_user,
                person["github_username"],
                window.duration_days,
                window,
            ): username
            for username, person in people.items()
        }
        for future in as_completed(futures):
            username = futures[future]
            authored[username], reviewed[username] = future.result()
    return authored, reviewed


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator * 100, 1) if denominator else None


def _pull_request_link(attribution: dict[str, Any]) -> dict[str, str] | None:
    raw_url = attribution.get("url")
    if not isinstance(raw_url, str):
        return None
    url = raw_url.strip().rstrip("/")
    if (parsed := parse_pull_request_url(url)) is None:
        return None
    _owner, repo, number = parsed
    return {"url": url, "label": f"{repo}#{number}"}


def _sorted_pull_request_links(
    links: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    return sorted(links.values(), key=lambda link: (link["label"].casefold(), link["url"]))


def _person_metrics(
    records: list[dict[str, Any]],
    people: dict[str, dict[str, str]],
    authored_counts: dict[str, int],
    reviewed_counts: dict[str, int],
    window: TimeWindow,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    authored_regressions = {username: 0 for username in people}
    approved_regressions = {username: 0 for username in people}
    authored_pull_requests: dict[str, dict[str, dict[str, str]]] = {
        username: {} for username in people
    }
    approved_pull_requests: dict[str, dict[str, dict[str, str]]] = {
        username: {} for username in people
    }
    for record in records:
        attribution = record.get("attribution")
        if not isinstance(attribution, dict):
            continue
        merged_at = _parse_datetime(attribution.get("merged_at"))
        if merged_at is None or not window.start <= merged_at < window.end:
            continue
        pull_request = _pull_request_link(attribution)
        author = attribution.get("author")
        if isinstance(author, str) and author.casefold() in authored_regressions:
            username = author.casefold()
            authored_regressions[username] += 1
            if pull_request is not None:
                authored_pull_requests[username].setdefault(pull_request["url"], pull_request)
        for reviewer in attribution.get("reviewers") or []:
            if isinstance(reviewer, str) and reviewer.casefold() in approved_regressions:
                username = reviewer.casefold()
                approved_regressions[username] += 1
                if pull_request is not None:
                    approved_pull_requests[username].setdefault(pull_request["url"], pull_request)

    author_rows, reviewer_rows = [], []
    for username, person in people.items():
        authored_count = authored_counts.get(username, 0)
        reviewed_count = reviewed_counts.get(username, 0)
        author_rows.append(
            {
                **person,
                "regression_count": authored_regressions[username],
                "pr_count": authored_count,
                "rate": _rate(authored_regressions[username], authored_count),
                "pull_requests": _sorted_pull_request_links(authored_pull_requests[username]),
            }
        )
        reviewer_rows.append(
            {
                **person,
                "regression_count": approved_regressions[username],
                "pr_count": reviewed_count,
                "rate": _rate(approved_regressions[username], reviewed_count),
                "pull_requests": _sorted_pull_request_links(approved_pull_requests[username]),
            }
        )
    return author_rows, reviewer_rows


def build_regression_summary() -> dict[str, Any]:
    if not os.getenv("LINEAR_API_KEY") or not os.getenv("GITHUB_TOKEN"):
        return {
            "days": REGRESSION_DAYS,
            "configured": False,
            "complete": False,
            "author_metrics": [],
            "reviewer_metrics": [],
        }
    window = TimeWindow.from_days(REGRESSION_DAYS)
    people = _engineering_people()
    authored_counts, reviewed_counts = _team_pr_counts(people, window)
    records, failed_count = collect_regression_attributions(window)
    author_metrics, reviewer_metrics = _person_metrics(
        records, people, authored_counts, reviewed_counts, window
    )
    authored_regressions = sum(row["regression_count"] for row in author_metrics)
    approved_regressions = sum(row["regression_count"] for row in reviewer_metrics)
    authored_prs = sum(row["pr_count"] for row in author_metrics)
    reviewed_prs = sum(row["pr_count"] for row in reviewer_metrics)
    return {
        "days": REGRESSION_DAYS,
        "configured": True,
        "complete": failed_count == 0,
        "regression_count": len(records),
        "attributed_count": sum(record.get("attribution") is not None for record in records),
        "authored_regression_count": authored_regressions,
        "authored_pr_count": authored_prs,
        "author_regression_rate": _rate(authored_regressions, authored_prs),
        "approved_regression_count": approved_regressions,
        "reviewed_pr_count": reviewed_prs,
        "reviewer_escape_rate": _rate(approved_regressions, reviewed_prs),
        "author_metrics": author_metrics,
        "reviewer_metrics": reviewer_metrics,
    }
