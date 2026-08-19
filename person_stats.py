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

LOWER_IS_BETTER_METRIC_KEYS = frozenset(
    {
        "priority_bug_avg_time_to_fix",
        "avg_all_time_to_fix",
        "lead_completed_projects_avg_early_late",
    }
)

STDEV_COLOR_THRESHOLD = 1.5
STDEV_COLOR_METRIC_KEYS = frozenset({"prs_merged", "prs_reviewed", "all_work_done"})

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


def format_stdev_tooltip(values: list[float], *, lower_is_better: bool = False) -> str:
    tooltip = f"eng avg {statistics.fmean(values):.1f} · σ {statistics.pstdev(values):.1f}"
    return f"{tooltip} · lower is better" if lower_is_better else tooltip


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
        lower_is_better = key in LOWER_IS_BETTER_METRIC_KEYS
        if lower_is_better:
            z = -z
        entry: dict[str, str] = {
            "label": format_stdev_label(z),
            "tooltip": format_stdev_tooltip(values, lower_is_better=lower_is_better),
        }
        if key in STDEV_COLOR_METRIC_KEYS:
            tone = stdev_tone(z)
            if tone is not None:
                entry["tone"] = tone
        result[key] = entry
    return result
