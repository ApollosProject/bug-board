from __future__ import annotations

from datetime import datetime, timedelta, timezone

from constants import (
    CYCLE_PROJECT_LEAD_POINTS_PER_WEEK,
    CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK,
)
from linear.projects import get_projects


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _build_week_segments(
    timeframe_start: datetime, now: datetime
) -> list[tuple[datetime, datetime]]:
    segments: list[tuple[datetime, datetime]] = []
    segment_end = now
    while segment_end > timeframe_start:
        segment_start = max(timeframe_start, segment_end - timedelta(days=7))
        segments.append((segment_start, segment_end))
        segment_end = segment_start
    return segments


def _calculate_cycle_project_points(
    days: int, now: datetime | None = None
) -> tuple[dict[str, int], dict[str, int]]:
    if days <= 0:
        return {}, {}
    now = now or datetime.now(timezone.utc)
    projects = get_projects()
    timeframe_start = now - timedelta(days=days)
    week_segments = _build_week_segments(timeframe_start, now)
    points_by_lead: dict[str, int] = {}
    points_by_member: dict[str, int] = {}
    for project in projects:
        status_name = (project.get("status") or {}).get("name")
        if status_name != "Completed":
            continue
        lead_name = (project.get("lead") or {}).get("displayName")
        if not lead_name:
            continue
        target_at = _parse_date(project.get("targetDate"))
        if not target_at:
            continue
        window_end = min(target_at + timedelta(days=1), now)
        if window_end <= timeframe_start:
            continue
        start_at = _parse_date(project.get("startDate")) or target_at
        if start_at > target_at:
            start_at = target_at
        window_start = max(start_at, timeframe_start)
        if window_end <= window_start:
            continue
        members = {
            member
            for member in project.get("members", []) or []
            if member and member != lead_name
        }
        for segment_start, segment_end in week_segments:
            overlap_start = max(window_start, segment_start)
            overlap_end = min(window_end, segment_end)
            if overlap_end > overlap_start:
                points_by_lead[lead_name] = (
                    points_by_lead.get(lead_name, 0)
                    + CYCLE_PROJECT_LEAD_POINTS_PER_WEEK
                )
                for member in members:
                    points_by_member[member] = (
                        points_by_member.get(member, 0)
                        + CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK
                    )
    return points_by_lead, points_by_member


def calculate_cycle_project_lead_points(
    days: int, now: datetime | None = None
) -> dict[str, int]:
    lead_points, _ = _calculate_cycle_project_points(days, now)
    return lead_points


def calculate_cycle_project_member_points(
    days: int, now: datetime | None = None
) -> dict[str, int]:
    _, member_points = _calculate_cycle_project_points(days, now)
    return member_points
