from __future__ import annotations

import csv
import io
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from constants import ENGINEERING_TEAM_SLUG
from person_stats import z_score

PARAMETER_SPECS: tuple[tuple[str, str | None, str], ...] = (
    ("urgent", "urgent_issues", "urgent_points"),
    ("high", "high_issues", "high_points"),
    ("medium", "medium_issues", "medium_points"),
    ("low", "low_issues", "low_points"),
    ("reviews", "pr_reviews", "pr_review_points"),
    ("prs", "prs_merged", "pr_points"),
    ("cycle_lead", None, "cycle_lead_points"),
    ("cycle_member", None, "cycle_member_points"),
)

CSV_COLUMNS: tuple[str, ...] = (
    "person",
    "slug",
    "score",
    "score_stdev",
    *(
        name
        for _key, count_column, points_column in PARAMETER_SPECS
        for name in (
            *([count_column] if count_column else []),
            points_column,
            f"{points_column}_stdev",
        )
    ),
    "regressions_authored",
    "author_regression_rate",
    "regressions_approved",
    "reviewer_escape_rate",
)


def format_export_name(slug: str, info: Mapping[str, Any] | None = None) -> str:
    username = (info or {}).get("linear_username") or slug
    if not isinstance(username, str) or not username:
        username = slug
    return re.sub(r"[._-]+", " ", username).title()


def leaderboard_csv_filename(context: Mapping[str, Any]) -> str:
    preset = context.get("preset_days")
    if isinstance(preset, int):
        return f"leaderboard-{preset}d.csv"
    start = context.get("start") or "start"
    end = context.get("end") or "end"
    return f"leaderboard-{start}-to-{end}.csv"


def render_leaderboard_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(CSV_COLUMNS), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})
    return buffer.getvalue()


def build_leaderboard_export_rows(
    entries: Sequence[Mapping[str, Any]],
    *,
    engineering_people: Mapping[str, Mapping[str, Any]] | None = None,
    regression_summary: Mapping[str, Any] | None = None,
    format_name: Callable[[str, Mapping[str, Any] | None], str] = format_export_name,
) -> list[dict[str, Any]]:
    rows = [
        _entry_to_base_row(entry, format_name=format_name)
        for entry in entries
        if isinstance(entry, Mapping)
    ]
    rows.extend(
        _missing_engineering_rows(
            rows,
            engineering_people or {},
            format_name=format_name,
        )
    )
    rows.sort(key=lambda row: (-int(row["score"] or 0), str(row["person"])))
    _attach_parameter_stdevs(rows)
    _attach_regression_columns(rows, regression_summary)
    return rows


def _entry_to_base_row(
    entry: Mapping[str, Any],
    *,
    format_name: Callable[[str, Mapping[str, Any] | None], str],
) -> dict[str, Any]:
    slug = entry.get("slug")
    slug_value = slug if isinstance(slug, str) and slug else ""
    display_name = entry.get("display_name")
    if not isinstance(display_name, str) or not display_name:
        display_name = format_name(slug_value, None) if slug_value else ""
    raw_points = entry.get("points")
    raw_counts = entry.get("counts")
    points = raw_points if isinstance(raw_points, Mapping) else {}
    counts = raw_counts if isinstance(raw_counts, Mapping) else {}
    row: dict[str, Any] = {
        "person": display_name,
        "slug": slug_value,
        "score": int(entry.get("score") or 0),
    }
    for key, count_column, points_column in PARAMETER_SPECS:
        if count_column:
            row[count_column] = int(counts.get(key) or 0)
        row[points_column] = int(points.get(key) or 0)
    return row


def _missing_engineering_rows(
    rows: Sequence[Mapping[str, Any]],
    engineering_people: Mapping[str, Mapping[str, Any]],
    *,
    format_name: Callable[[str, Mapping[str, Any] | None], str],
) -> list[dict[str, Any]]:
    present = {str(row.get("slug")) for row in rows if row.get("slug")}
    missing: list[dict[str, Any]] = []
    for slug, info in engineering_people.items():
        if info.get("team") != ENGINEERING_TEAM_SLUG:
            continue
        if slug in present:
            continue
        missing.append(
            _entry_to_base_row(
                {
                    "slug": slug,
                    "display_name": format_name(slug, info),
                    "score": 0,
                    "points": {},
                    "counts": {},
                },
                format_name=format_name,
            )
        )
    return missing


def _attach_parameter_stdevs(rows: list[dict[str, Any]]) -> None:
    numeric_columns = ["score", *(points_column for _key, _count, points_column in PARAMETER_SPECS)]
    for column in numeric_columns:
        values = [float(row.get(column) or 0) for row in rows]
        for row, value in zip(rows, values, strict=True):
            z = z_score(value, values)
            row[f"{column}_stdev"] = "" if z is None else f"{z:.1f}"


def _metrics_by_slug(metrics: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(metrics, list):
        return {}
    by_slug: dict[str, Mapping[str, Any]] = {}
    for item in metrics:
        if isinstance(item, Mapping) and isinstance(item.get("slug"), str):
            by_slug[item["slug"]] = item
    return by_slug


def _regression_value(metric: Mapping[str, Any] | None, field: str) -> Any:
    if metric is None:
        return 0 if field == "regression_count" else ""
    value = metric.get(field)
    if field == "regression_count":
        return int(value or 0)
    return "" if value is None else value


def _attach_regression_columns(
    rows: list[dict[str, Any]],
    regression_summary: Mapping[str, Any] | None,
) -> None:
    configured = bool(regression_summary and regression_summary.get("configured"))
    authors = _metrics_by_slug((regression_summary or {}).get("author_metrics"))
    reviewers = _metrics_by_slug((regression_summary or {}).get("reviewer_metrics"))
    for row in rows:
        slug = str(row.get("slug") or "")
        if not configured:
            row["regressions_authored"] = ""
            row["author_regression_rate"] = ""
            row["regressions_approved"] = ""
            row["reviewer_escape_rate"] = ""
            continue
        author = authors.get(slug)
        reviewer = reviewers.get(slug)
        row["regressions_authored"] = _regression_value(author, "regression_count")
        row["author_regression_rate"] = _regression_value(author, "rate")
        row["regressions_approved"] = _regression_value(reviewer, "regression_count")
        row["reviewer_escape_rate"] = _regression_value(reviewer, "rate")
