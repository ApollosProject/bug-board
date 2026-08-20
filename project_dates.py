from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone

INACTIVE_PROJECT_STATUS_NAMES = {
    "completed",
    "incomplete",
    "canceled",
    "cancelled",
    "released",
}
COMPLETED_PROJECT_STATUS_NAMES = {"completed", "released"}
INCOMPLETE_PROJECT_STATUS_NAMES = {"incomplete"}
CANCELED_PROJECT_STATUS_NAMES = {"canceled", "cancelled"}


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def get_project_planned_weeks(project: dict) -> int:
    start = parse_iso_date(project.get("startDate"))
    target = parse_iso_date(project.get("targetDate"))
    if start is None and target is None:
        return 1
    if start is None:
        start = target
    if target is None:
        target = start
    if start is None or target is None:
        return 1
    if start > target:
        start, target = target, start
    inclusive_days = (target - start).days + 1
    return max(1, round(inclusive_days / 7))


def get_project_status_name(project: dict) -> str:
    status = project.get("status") or {}
    name = status.get("name")
    if not isinstance(name, str):
        return ""
    return name.strip().lower()


def is_incomplete_project(project: dict) -> bool:
    return get_project_status_name(project) in INCOMPLETE_PROJECT_STATUS_NAMES


def is_completed_project(project: dict) -> bool:
    status_name = get_project_status_name(project)
    if status_name in INCOMPLETE_PROJECT_STATUS_NAMES | CANCELED_PROJECT_STATUS_NAMES:
        return False
    return bool(project.get("completedAt")) or status_name in COMPLETED_PROJECT_STATUS_NAMES


def is_inactive_project(project: dict) -> bool:
    return bool(project.get("completedAt")) or (
        get_project_status_name(project) in INACTIVE_PROJECT_STATUS_NAMES
    )


def get_project_schedule_variance_days(project: dict) -> int | None:
    target_date = parse_iso_date(project.get("targetDate"))
    completed_date = parse_iso_date(project.get("completedAt"))
    if target_date is None or completed_date is None:
        return None
    return (completed_date - target_date).days


def completed_project_weeks(projects: list[dict]) -> int:
    return sum(
        get_project_planned_weeks(project) for project in projects if is_completed_project(project)
    )


def _format_hours(delta: timedelta) -> str:
    seconds = max(delta.total_seconds(), 0)
    hours = math.ceil(seconds / 3600) if seconds else 0
    return f"{hours}h"


def format_project_start_status(
    start_date: date | None, now: datetime | None = None
) -> tuple[int | None, str | None]:
    if start_date is None:
        return None, None

    current_time = now or datetime.now(timezone.utc)
    start_at = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    starts_in = (start_date - current_time.date()).days

    if starts_in <= 0:
        return starts_in, None
    if start_at - current_time < timedelta(days=1):
        return starts_in, f"starts in {_format_hours(start_at - current_time)}"
    return starts_in, f"starts in {starts_in}d"


def format_project_target_status(
    target_date: date | None, now: datetime | None = None
) -> tuple[int | None, str | None]:
    if target_date is None:
        return None, None

    current_time = now or datetime.now(timezone.utc)
    deadline_at = datetime.combine(
        target_date + timedelta(days=1),
        time.min,
        tzinfo=timezone.utc,
    )
    delta = deadline_at - current_time
    days_left = (target_date - current_time.date()).days

    if abs(delta) < timedelta(days=1):
        direction = "left" if delta.total_seconds() >= 0 else "overdue"
        return days_left, f"{_format_hours(abs(delta))} {direction}"
    if days_left < 0:
        return days_left, f"{abs(days_left)}d overdue"
    return days_left, f"{days_left}d left"
