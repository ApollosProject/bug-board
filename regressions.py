from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from github_regressions import parse_pull_request_url

REGRESSION_OVERRIDES_PATH = "regression_overrides.yml"
FIXING_LINK_KINDS = {"closes", "contributes"}


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
