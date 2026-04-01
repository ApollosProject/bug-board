from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


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
