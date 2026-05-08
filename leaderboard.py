from __future__ import annotations

from datetime import datetime, timedelta, timezone

from constants import (
    CYCLE_PROJECT_LEAD_POINTS_PER_WEEK,
    CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK,
)
from linear.projects import get_completed_project_issue_assignees, get_projects


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


def _get_project_status_type(project: dict) -> str:
    status_type = ((project.get("status") or {}).get("type") or "").strip().lower()
    return status_type


def _is_completed_project(project: dict) -> bool:
    return _get_project_status_type(project) == "completed"


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
        if not _is_completed_project(project):
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
        scoring_segments = [
            (segment_start, segment_end)
            for segment_start, segment_end in week_segments
            if min(window_end, segment_end) > max(window_start, segment_start)
        ]
        if not scoring_segments:
            continue
        project_id = project.get("id")
        contributors = (
            {
                contributor
                for contributor in get_completed_project_issue_assignees(project_id)
                if contributor and contributor != lead_name
            }
            if project_id
            else set()
        )
        for _ in scoring_segments:
            points_by_lead[lead_name] = (
                points_by_lead.get(lead_name, 0) + CYCLE_PROJECT_LEAD_POINTS_PER_WEEK
            )
            for contributor in contributors:
                points_by_member[contributor] = (
                    points_by_member.get(contributor, 0) + CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK
                )
    return points_by_lead, points_by_member


def calculate_cycle_project_lead_points(days: int, now: datetime | None = None) -> dict[str, int]:
    lead_points, _ = _calculate_cycle_project_points(days, now)
    return lead_points


def calculate_cycle_project_member_points(days: int, now: datetime | None = None) -> dict[str, int]:
    _, member_points = _calculate_cycle_project_points(days, now)
    return member_points
