from __future__ import annotations

import statistics
from collections.abc import Iterable, Mapping

CARD_METRIC_KEYS = (
    "prs_merged",
    "prs_reviewed",
    "priority_bugs_fixed",
    "priority_bug_avg_time_to_fix",
    "all_work_done",
    "avg_all_time_to_fix",
    "lead_current_projects",
    "lead_completed_projects",
    "lead_incomplete_projects",
    "lead_completed_projects_avg_early_late",
)
CARD_METRIC_LABELS = {
    "prs_merged": "PRs Merged",
    "prs_reviewed": "PRs Reviewed",
    "priority_bugs_fixed": "Priority Bugs Fixed",
    "priority_bug_avg_time_to_fix": "Priority Bug Time to Fix",
    "all_work_done": "All Work Done",
    "avg_all_time_to_fix": "Time to Completion",
    "lead_current_projects": "Current Projects",
    "lead_completed_projects": "Completed Project Weeks",
    "lead_incomplete_projects": "Incomplete Projects",
    "lead_completed_projects_avg_early_late": "Avg Days Early/Late",
}

# Displayed σ is oriented so + is better than the engineering average and − is worse.
LOWER_IS_BETTER_METRIC_KEYS = frozenset(
    {
        "priority_bug_avg_time_to_fix",
        "avg_all_time_to_fix",
        "lead_incomplete_projects",
        "lead_completed_projects_avg_early_late",
    }
)
STDEV_DIRECTION_HINTS = {
    "priority_bug_avg_time_to_fix": "lower is better",
    "avg_all_time_to_fix": "lower is better",
    "lead_incomplete_projects": "lower is better",
    "lead_completed_projects_avg_early_late": "earlier is better",
}

STDEV_COLOR_THRESHOLD = 1.5

MetricValue = float | int | None


def issue_card_values(items: list[dict]) -> dict[str, MetricValue]:
    bugs = [
        issue
        for issue in items
        if issue.get("priority", 5) <= 2
        and any(lbl.get("name") == "Bug" for lbl in issue.get("labels", {}).get("nodes", []))
    ]
    bug_times = [
        i["assignee_time_to_fix"] for i in bugs if i.get("assignee_time_to_fix") is not None
    ]
    times = [i["assignee_time_to_fix"] for i in items if i.get("assignee_time_to_fix") is not None]
    return {
        "priority_bugs_fixed": len(bugs),
        "priority_bug_avg_time_to_fix": (
            int(sum(bug_times) / len(bug_times)) if bug_times else None
        ),
        "all_work_done": len(items),
        "avg_all_time_to_fix": int(sum(times) / len(times)) if times else None,
    }


def performance_outliers(
    people_metrics: Mapping[str, Mapping[str, MetricValue]],
) -> dict[str, dict[str, list[dict[str, str]]]]:
    team_metrics = list(people_metrics.values())
    outliers: dict[str, dict[str, list[dict[str, str]]]] = {}
    for slug, metrics in people_metrics.items():
        high: list[dict[str, str]] = []
        low: list[dict[str, str]] = []
        for key, entry in metric_stdevs_for_person(metrics, team_metrics).items():
            tone = entry.get("tone")
            if tone not in {"high", "low"}:
                continue
            item = {"name": CARD_METRIC_LABELS[key], "label": entry["label"]}
            (high if tone == "high" else low).append(item)
        if high or low:
            outliers[slug] = {"high": high, "low": low}
    return outliers


def z_score(value: float, values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    stdev = statistics.pstdev(values)
    if stdev == 0:
        return None
    return (value - statistics.fmean(values)) / stdev


def format_stdev_label(z: float) -> str:
    if abs(z) < 0.05:
        return "0.0σ"
    sign = "+" if z > 0 else "−"
    return f"{sign}{abs(z):.1f}σ"


def stdev_tone(z: float, *, threshold: float = STDEV_COLOR_THRESHOLD) -> str | None:
    if z >= threshold:
        return "high"
    if z <= -threshold:
        return "low"
    return None


def format_stdev_tooltip(values: list[float], *, hint: str | None = None) -> str:
    tooltip = f"eng avg {statistics.fmean(values):.1f} · σ {statistics.pstdev(values):.1f}"
    return f"{tooltip} · {hint}" if hint else tooltip


def metric_stdevs_for_person(
    person_metrics: Mapping[str, MetricValue],
    team_metrics: Iterable[Mapping[str, MetricValue]],
) -> dict[str, dict[str, str]]:
    team_list = list(team_metrics)
    result: dict[str, dict[str, str]] = {}
    for key in CARD_METRIC_KEYS:
        person_value = person_metrics.get(key)
        if person_value is None:
            continue
        values = [float(value) for metrics in team_list if (value := metrics.get(key)) is not None]
        z = z_score(float(person_value), values)
        if z is None:
            continue
        if key in LOWER_IS_BETTER_METRIC_KEYS:
            z = -z
        entry: dict[str, str] = {
            "label": format_stdev_label(z),
            "tooltip": format_stdev_tooltip(values, hint=STDEV_DIRECTION_HINTS.get(key)),
        }
        tone = stdev_tone(z)
        if tone is not None:
            entry["tone"] = tone
        result[key] = entry
    return result
