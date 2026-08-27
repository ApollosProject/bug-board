#!/usr/bin/env python3
"""Export one person's card metrics + σ as JSON for external dashboards.

Reuses the exact computation behind the ``/team/<slug>`` page
(:func:`app._build_person_context`) so the numbers and σ values match the web
dashboard. Intended to be run on a schedule (the whole-team GitHub + Linear
fetch is expensive) and read by lightweight consumers such as the ``bridge``
TUI.

Usage::

    python person_metrics_export.py --slug alice            # JSON to stdout
    python person_metrics_export.py --slug alice \\
        --windows 7,30,90 --out /path/to/metrics.json

Requires the same environment as the app (``LINEAR_API_KEY``, ``GITHUB_TOKEN``).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

from app import _build_person_context
from config import load_config
from github import get_github_orgs
from person_stats import (
    CARD_METRIC_KEYS,
    CARD_METRIC_LABELS,
    LOWER_IS_BETTER_METRIC_KEYS,
)

# Metric whose value is a schedule variance ("1d late" / "2.5d early" / "on
# time"); rewritten to a compact signed form (+ late, − early, 0 on time).
_EARLY_LATE_KEY = "lead_completed_projects_avg_early_late"
_EARLY_LATE_RE = re.compile(r"([\d.]+)d\s+(late|early)", re.IGNORECASE)
# The engineering baseline the σ is measured against is rendered into the card
# tooltip by ``format_stdev_tooltip`` ("eng trimmed avg 12.3 · σ 4.5 · hint").
# Pull it back out so consumers can compare against the team average directly
# instead of only seeing their own distance from it.
_BASELINE_RE = re.compile(r"avg\s+([\d.]+)\s*·\s*σ\s+([\d.]+)")


def _parse_sigma(label: str | None) -> float | None:
    """Turn a σ label ("+1.2σ" / "−0.8σ" / "0.0σ") into a float for bucketing.

    ``format_stdev_label`` renders the sign with a Unicode minus (U+2212); map
    it back to ASCII before parsing.
    """
    if not label:
        return None
    cleaned = label.replace("σ", "").replace("−", "-").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_baseline(tooltip: str | None) -> tuple[float | None, float | None]:
    """Extract (team mean, team σ) from a σ tooltip; (None, None) if absent."""
    if not tooltip:
        return None, None
    match = _BASELINE_RE.search(tooltip)
    if not match:
        return None, None
    return float(match.group(1)), float(match.group(2))


def _compact_early_late(formatted: Any) -> Any:
    """ "1d late" -> "+1d", "2.5d early" -> "-2.5d", "on time" -> "0d"."""
    if not isinstance(formatted, str):
        return formatted
    text = formatted.strip().lower()
    if text == "on time":
        return "0d"
    match = _EARLY_LATE_RE.match(text)
    if not match:
        return formatted
    sign = "+" if match.group(2) == "late" else "-"
    return f"{sign}{match.group(1)}d"


def export_person_metrics(slug: str, windows: list[int]) -> dict[str, Any]:
    """Build the JSON payload of {window: {metric: {value, sigma, z}}}."""
    person_cfg = load_config().get("people", {}).get(slug) or {}
    payload: dict[str, Any] = {
        "slug": slug,
        # Identity + org scope so consumers can query the same GitHub surface
        # (e.g. today-so-far counts) without re-deriving this config.
        "github_username": person_cfg.get("github_username"),
        "orgs": list(get_github_orgs()),
        "generated_at": time.time(),
        "order": list(CARD_METRIC_KEYS),
        "labels": {key: CARD_METRIC_LABELS[key] for key in CARD_METRIC_KEYS},
        "lower_is_better": sorted(LOWER_IS_BETTER_METRIC_KEYS),
        "windows": {},
    }
    for days in windows:
        # A fresh cache epoch forces a live recompute for each scheduled run.
        context = _build_person_context(slug, days, int(time.time()))
        stdevs = context.get("metric_stdevs") or {}
        window_metrics: dict[str, Any] = {}
        for key in CARD_METRIC_KEYS:
            stat = stdevs.get(key) or {}
            label = stat.get("label")
            value = context.get(key)
            if key == _EARLY_LATE_KEY:
                value = _compact_early_late(value)
            team_mean, team_sigma = _parse_baseline(stat.get("tooltip"))
            window_metrics[key] = {
                "value": value,
                "sigma": label,
                "z": _parse_sigma(label),
                "team_mean": team_mean,
                "team_sigma": team_sigma,
            }
        payload["windows"][str(days)] = window_metrics
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a person's card metrics + σ as JSON.")
    parser.add_argument("--slug", required=True, help="people-config slug")
    parser.add_argument("--windows", default="7,30,90", help="comma-separated day windows")
    parser.add_argument(
        "--out",
        default="-",
        help="output JSON path, or '-' for stdout (default)",
    )
    args = parser.parse_args()
    windows = [int(part) for part in args.windows.split(",") if part.strip()]
    payload = export_person_metrics(args.slug, windows)
    serialized = json.dumps(payload, indent=2) + "\n"
    if args.out == "-":
        sys.stdout.write(serialized)
        return
    out_path = Path(args.out).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(serialized)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
