from __future__ import annotations

import csv
import io
import re
from collections.abc import Mapping, Sequence
from typing import Any

from config import load_config
from constants import ENGINEERING_TEAM_SLUG
from person_stats import z_score
from regression_cache import get_cached_regression_summary

PARAMS = (
    ("urgent", "urgent_issues", "urgent_points"),
    ("high", "high_issues", "high_points"),
    ("medium", "medium_issues", "medium_points"),
    ("low", "low_issues", "low_points"),
    ("reviews", "pr_reviews", "pr_review_points"),
    ("prs", "prs_merged", "pr_points"),
    ("cycle_lead", None, "cycle_lead_points"),
    ("cycle_member", None, "cycle_member_points"),
)
CSV_COLUMNS = [
    "person",
    "slug",
    "score",
    "score_stdev",
    *(
        name
        for _key, count, points in PARAMS
        for name in ((count, points, f"{points}_stdev") if count else (points, f"{points}_stdev"))
    ),
    "regressions_authored",
    "author_regression_rate",
    "regressions_approved",
    "reviewer_escape_rate",
]
POINT_COLS = [points for _key, _count, points in PARAMS]


def _name(slug: str, info: Mapping[str, Any] | None = None) -> str:
    raw = (info or {}).get("linear_username") or slug
    return re.sub(r"[._-]+", " ", raw if isinstance(raw, str) else slug).title()


def render_leaderboard_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows({col: row.get(col, "") for col in CSV_COLUMNS} for row in rows)
    return buf.getvalue()


def _row(entry: Mapping[str, Any], name: str | None = None) -> dict[str, Any]:
    slug = entry.get("slug") if isinstance(entry.get("slug"), str) else ""
    raw_points = entry.get("points")
    raw_counts = entry.get("counts")
    points = raw_points if isinstance(raw_points, Mapping) else {}
    counts = raw_counts if isinstance(raw_counts, Mapping) else {}
    row = {
        "person": name or entry.get("display_name") or _name(str(slug)),
        "slug": slug,
        "score": int(entry.get("score") or 0),
    }
    for key, count_col, point_col in PARAMS:
        if count_col:
            row[count_col] = int(counts.get(key) or 0)
        row[point_col] = int(points.get(key) or 0)
    return row


def build_leaderboard_export_rows(
    entries: Sequence[Mapping[str, Any]],
    *,
    people: Mapping[str, Mapping[str, Any]] | None = None,
    regression_summary: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows = [_row(entry) for entry in entries if isinstance(entry, Mapping)]
    if people is None:
        people = {
            slug: info
            for slug, info in load_config().get("people", {}).items()
            if isinstance(info, Mapping) and info.get("team") == ENGINEERING_TEAM_SLUG
        }
    seen = {row["slug"] for row in rows if row["slug"]}
    for slug, info in people.items():
        if slug not in seen:
            rows.append(_row({"slug": slug, "score": 0}, _name(slug, info)))
    rows.sort(key=lambda row: (-int(row["score"]), str(row["person"])))
    for column in ["score", *POINT_COLS]:
        values = [float(row[column] or 0) for row in rows]
        for row, value in zip(rows, values, strict=True):
            z_value = z_score(value, values)
            row[f"{column}_stdev"] = "" if z_value is None else f"{z_value:.1f}"
    if regression_summary is None:
        regression_summary = get_cached_regression_summary()
    ready = bool(regression_summary and regression_summary.get("configured"))
    authors = {
        item["slug"]: item
        for item in (regression_summary or {}).get("author_metrics") or []
        if isinstance(item, Mapping) and isinstance(item.get("slug"), str)
    }
    reviewers = {
        item["slug"]: item
        for item in (regression_summary or {}).get("reviewer_metrics") or []
        if isinstance(item, Mapping) and isinstance(item.get("slug"), str)
    }
    for row in rows:
        author = authors.get(str(row["slug"]), {}) if ready else None
        reviewer = reviewers.get(str(row["slug"]), {}) if ready else None
        row["regressions_authored"] = (
            "" if author is None else int(author.get("regression_count") or 0)
        )
        row["author_regression_rate"] = (
            "" if not author or author.get("rate") is None else author["rate"]
        )
        row["regressions_approved"] = (
            "" if reviewer is None else int(reviewer.get("regression_count") or 0)
        )
        row["reviewer_escape_rate"] = (
            "" if not reviewer or reviewer.get("rate") is None else reviewer["rate"]
        )
    return rows
