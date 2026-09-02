from __future__ import annotations

import csv
import io
import re
from collections.abc import Mapping, Sequence
from typing import Any

from config import load_config
from constants import (
    CYCLE_PROJECT_LEAD_POINTS_PER_WEEK,
    CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK,
)
from person_stats import z_score

TEAM_METRIC_COLUMNS = tuple(
    zip(
        (
            "prs_merged prs_reviewed urgent_issues high_issues medium_issues "
            "low_issues project_lead_weeks project_contributor_weeks"
        ).split(),
        (
            "PRs merged|PRs approved|Urgent issues|High issues|Medium issues|"
            "Low issues|Project lead weeks|Project contributor weeks"
        ).split("|"),
        strict=True,
    )
)
TEAM_METRIC_KEYS = tuple(key for key, _label in TEAM_METRIC_COLUMNS)
CSV_COLUMNS = [
    "person",
    "slug",
    *(name for key in TEAM_METRIC_KEYS for name in (key, f"{key}_z")),
]


def _name(slug: str, info: Mapping[str, Any] | None = None) -> str:
    raw = (info or {}).get("linear_username") or slug
    return re.sub(r"[._-]+", " ", raw if isinstance(raw, str) else slug).title()


def _format_z(z_value: float | None) -> str:
    return "" if z_value is None else f"{z_value:.1f}"


def _attach_team_metric_z_scores(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = [dict(row) for row in rows]
    for key in TEAM_METRIC_KEYS:
        values = [float(row.get(key) or 0) for row in result]
        for row, value in zip(result, values, strict=True):
            row[f"{key}_z"] = _format_z(z_score(value, values))
    return result


def render_team_metrics_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(
        {col: row.get(col, "") for col in CSV_COLUMNS} for row in _attach_team_metric_z_scores(rows)
    )
    return buf.getvalue()


def _row(entry: Mapping[str, Any], name: str | None = None) -> dict[str, Any]:
    slug = entry.get("slug") if isinstance(entry.get("slug"), str) else ""
    raw_points = entry.get("points")
    raw_counts = entry.get("counts")
    points = raw_points if isinstance(raw_points, Mapping) else {}
    counts = raw_counts if isinstance(raw_counts, Mapping) else {}
    return {
        "person": name or entry.get("display_name") or _name(str(slug)),
        "slug": slug,
        "prs_merged": int(counts.get("prs") or 0),
        "prs_reviewed": int(counts.get("reviews") or 0),
        "urgent_issues": int(counts.get("urgent") or 0),
        "high_issues": int(counts.get("high") or 0),
        "medium_issues": int(counts.get("medium") or 0),
        "low_issues": int(counts.get("low") or 0),
        "project_lead_weeks": int(points.get("cycle_lead") or 0)
        // CYCLE_PROJECT_LEAD_POINTS_PER_WEEK,
        "project_contributor_weeks": int(points.get("cycle_member") or 0)
        // CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK,
    }


def build_team_metric_rows(
    entries: Sequence[Mapping[str, Any]],
    *,
    people: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows = [_row(entry) for entry in entries if isinstance(entry, Mapping)]
    if people is None:
        people = {
            slug: info
            for slug, info in load_config().get("people", {}).items()
            if isinstance(info, Mapping)
        }
    for row in rows:
        row["person"] = _name(str(row["slug"]), people.get(str(row["slug"])))
    seen = {row["slug"] for row in rows if row["slug"]}
    for slug, info in people.items():
        if slug not in seen:
            rows.append(_row({"slug": slug}, _name(slug, info)))
    rows.sort(key=lambda row: (-int(row["prs_merged"]), str(row["person"])))
    return rows
