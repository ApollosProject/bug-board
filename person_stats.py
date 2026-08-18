from __future__ import annotations

import statistics
from collections.abc import Iterable, Mapping
from typing import Any

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

MetricValue = float | int | None
PersonCardMetrics = dict[str, MetricValue]


def _is_priority_bug(issue: dict[str, Any]) -> bool:
    if issue.get("priority", 5) > 2:
        return False
    return any(lbl.get("name") == "Bug" for lbl in issue.get("labels", {}).get("nodes", []))


def completed_work_metrics(completed_items: list[dict[str, Any]]) -> PersonCardMetrics:
    priority_fix_times: list[Any] = []
    priority_bugs_fixed = 0
    for issue in completed_items:
        if not _is_priority_bug(issue):
            continue
        priority_bugs_fixed += 1
        if issue.get("assignee_time_to_fix") is not None:
            priority_fix_times.append(issue["assignee_time_to_fix"])

    all_fix_times = [
        issue["assignee_time_to_fix"]
        for issue in completed_items
        if issue.get("assignee_time_to_fix") is not None
    ]
    return {
        "priority_bugs_fixed": priority_bugs_fixed,
        "priority_bug_avg_time_to_fix": (
            int(sum(priority_fix_times) / len(priority_fix_times)) if priority_fix_times else None
        ),
        "all_work_done": len(completed_items),
        "avg_all_time_to_fix": (
            int(sum(all_fix_times) / len(all_fix_times)) if all_fix_times else None
        ),
    }


def person_card_metrics(
    *,
    prs_merged: int,
    prs_reviewed: int,
    completed_items: list[dict[str, Any]],
    lead_current_projects: int,
    lead_completed_projects: int,
    lead_incomplete_projects: int,
    average_completed_project_variance: float | None,
) -> PersonCardMetrics:
    return {
        "prs_merged": prs_merged,
        "prs_reviewed": prs_reviewed,
        **completed_work_metrics(completed_items),
        "lead_current_projects": lead_current_projects,
        "lead_completed_projects": lead_completed_projects,
        "lead_incomplete_projects": lead_incomplete_projects,
        "lead_completed_projects_avg_early_late": average_completed_project_variance,
    }


def z_score(value: float, values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    stdev = statistics.pstdev(values)
    if stdev == 0:
        return 0.0
    return (value - statistics.fmean(values)) / stdev


def format_stdev_label(z: float) -> str:
    if abs(z) < 0.05:
        return "0.0σ"
    sign = "+" if z > 0 else "−"
    return f"{sign}{abs(z):.1f}σ"


def format_stdev_tooltip(z: float, values: list[float]) -> str:
    mean = statistics.fmean(values)
    stdev = statistics.pstdev(values)
    summary = f"mean {mean:.1f}, σ {stdev:.1f}"
    if abs(z) < 0.05:
        return f"0.0σ from the engineering group average ({summary})"
    direction = "above" if z > 0 else "below"
    return f"{abs(z):.1f}σ {direction} the engineering group average ({summary})"


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
        result[key] = {
            "label": format_stdev_label(z),
            "tooltip": format_stdev_tooltip(z, values),
        }
    return result
