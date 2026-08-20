from __future__ import annotations

import csv
import io
import re
from collections.abc import Mapping, Sequence
from typing import Any

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
    *(name for _key, count, points in PARAMS for name in ((count, points) if count else (points,))),
]


def _name(slug: str) -> str:
    return re.sub(r"[._-]+", " ", slug).title()


def render_leaderboard_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows({col: row.get(col, "") for col in CSV_COLUMNS} for row in rows)
    return buf.getvalue()


def _row(entry: Mapping[str, Any]) -> dict[str, Any]:
    slug = entry.get("slug") if isinstance(entry.get("slug"), str) else ""
    raw_points = entry.get("points")
    raw_counts = entry.get("counts")
    points = raw_points if isinstance(raw_points, Mapping) else {}
    counts = raw_counts if isinstance(raw_counts, Mapping) else {}
    row = {
        "person": entry.get("display_name") or _name(str(slug)),
        "slug": slug,
        "score": int(entry.get("score") or 0),
    }
    for key, count_col, point_col in PARAMS:
        if count_col:
            row[count_col] = int(counts.get(key) or 0)
        row[point_col] = int(points.get(key) or 0)
    return row


def build_leaderboard_export_rows(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [_row(entry) for entry in entries if isinstance(entry, Mapping)]
    rows.sort(key=lambda row: (-int(row["score"]), str(row["person"])))
    return rows
