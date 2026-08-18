from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache

from constants import (
    CYCLE_PROJECT_LEAD_POINTS_PER_WEEK,
    CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK,
)
from linear.projects import get_completed_project_issue_assignees_by_project, get_projects


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
    scoring_projects: list[tuple[str | None, str, int]] = []
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
        scoring_weeks = sum(
            1
            for segment_start, segment_end in week_segments
            if min(window_end, segment_end) > max(window_start, segment_start)
        )
        if not scoring_weeks:
            continue
        scoring_projects.append((project.get("id"), lead_name, scoring_weeks))

    project_ids = [project_id for project_id, _, _ in scoring_projects if project_id]
    assignees_by_project = (
        get_completed_project_issue_assignees_by_project(project_ids) if project_ids else {}
    )

    points_by_lead: dict[str, int] = {}
    points_by_member: dict[str, int] = {}
    for project_id, lead_name, scoring_weeks in scoring_projects:
        points_by_lead[lead_name] = (
            points_by_lead.get(lead_name, 0) + CYCLE_PROJECT_LEAD_POINTS_PER_WEEK * scoring_weeks
        )
        if not project_id:
            continue
        for contributor in assignees_by_project.get(project_id, []):
            if not contributor or contributor == lead_name:
                continue
            points_by_member[contributor] = (
                points_by_member.get(contributor, 0)
                + CYCLE_PROJECT_MEMBER_POINTS_PER_WEEK * scoring_weeks
            )
    return points_by_lead, points_by_member


@lru_cache(maxsize=16)
def _calculate_cycle_project_points_cached(
    days: int, now_key: str
) -> tuple[dict[str, int], dict[str, int]]:
    return _calculate_cycle_project_points(days, datetime.fromisoformat(now_key))


def calculate_cycle_project_points(
    days: int, now: datetime | None = None
) -> tuple[dict[str, int], dict[str, int]]:
    if now is None:
        return _calculate_cycle_project_points(days, None)
    return _calculate_cycle_project_points_cached(days, now.isoformat())


def calculate_cycle_project_lead_points(days: int, now: datetime | None = None) -> dict[str, int]:
    lead_points, _ = calculate_cycle_project_points(days, now)
    return lead_points


def calculate_cycle_project_member_points(days: int, now: datetime | None = None) -> dict[str, int]:
    _, member_points = calculate_cycle_project_points(days, now)
    return member_points
