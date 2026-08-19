from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

import yaml

from github_regressions import (
    GitHubRegressionDataError,
    get_fixing_pr_attribution,
    get_pull_request_attribution_metadata,
    parse_pull_request_url,
)
from linear.issues import get_completed_regression_candidates
from time_window import TimeWindow

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
