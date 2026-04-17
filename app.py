import logging
import os
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, TypedDict, TypeVar
from urllib.parse import quote

from flask import Flask, abort, jsonify, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

from airflow_fleet_health import AirflowFleetHealthError, evaluate_fleet_health
from config import load_config
from constants import ENGINEERING_TEAM_SLUG, PRIORITY_TO_SCORE
from fleet_health_cache import (
    get_cached_fleet_health,
    should_use_redis_cache,
)
from github import merged_prs_by_author, merged_prs_by_reviewer
from leaderboard import (
    calculate_cycle_project_lead_points,
    calculate_cycle_project_member_points,
)
from linear.issues import (
    by_assignee,
    by_platform,
    by_project,
    get_completed_issues_for_person,
    get_completed_issues_summary,
    get_created_issues,
    get_open_issues,
    get_open_issues_for_person,
    get_time_data,
)
from linear.projects import get_projects
from project_dates import (
    format_project_start_status,
    format_project_target_status,
    parse_iso_date,
)
from support import get_support_slugs


def normalize_identity(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]", "", value.lower())


def format_display_name(linear_username: str) -> str:
    return re.sub(r"[._-]+", " ", linear_username).title()


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


def get_project_status_name(project: dict[str, Any]) -> str:
    status = project.get("status") or {}
    name = status.get("name")
    if not isinstance(name, str):
        return ""
    return name.strip().lower()


def is_incomplete_project(project: dict[str, Any]) -> bool:
    return get_project_status_name(project) in INCOMPLETE_PROJECT_STATUS_NAMES


def is_completed_project(project: dict[str, Any]) -> bool:
    status_name = get_project_status_name(project)
    if status_name in INCOMPLETE_PROJECT_STATUS_NAMES | CANCELED_PROJECT_STATUS_NAMES:
        return False
    return bool(project.get("completedAt")) or status_name in COMPLETED_PROJECT_STATUS_NAMES


def is_inactive_project(project: dict[str, Any]) -> bool:
    return bool(project.get("completedAt")) or (
        get_project_status_name(project) in INACTIVE_PROJECT_STATUS_NAMES
    )


def _annotate_project_schedule_fields(projects: list[dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc)
    for project in projects:
        target = project.get("targetDate")
        start = project.get("startDate")
        target_date = parse_iso_date(target)
        start_date = parse_iso_date(start)

        if target and target_date is None:
            app.logger.warning(
                "Invalid targetDate %r for project %r",
                target,
                project.get("id"),
            )
        if start and start_date is None:
            app.logger.warning(
                "Invalid startDate %r for project %r",
                start,
                project.get("id"),
            )

        days_left, target_status_text = format_project_target_status(
            target_date,
            now=now,
        )
        starts_in, start_status_text = format_project_start_status(
            start_date,
            now=now,
        )
        project["days_left"] = days_left
        project["starts_in"] = starts_in
        project["target_status_text"] = target_status_text
        project["start_status_text"] = start_status_text


def get_project_schedule_variance_days(project: dict[str, Any]) -> int | None:
    target_date = parse_iso_date(project.get("targetDate"))
    completed_date = parse_iso_date(project.get("completedAt"))
    if target_date is None or completed_date is None:
        return None
    return (completed_date - target_date).days


def format_average_project_schedule_variance(
    average_variance_days: float | None,
) -> str | None:
    if average_variance_days is None:
        return None
    if abs(average_variance_days) < 0.05:
        return "on time"

    magnitude = abs(average_variance_days)
    if magnitude.is_integer():
        display = str(int(magnitude))
    else:
        display = f"{magnitude:.1f}".rstrip("0").rstrip(".")

    direction = "late" if average_variance_days > 0 else "early"
    return f"{display}d {direction}"


app = Flask(__name__)


def _apply_proxy_fix(flask_app: Flask) -> None:
    setattr(flask_app, "wsgi_app", ProxyFix(flask_app.wsgi_app, x_prefix=1))


_apply_proxy_fix(app)

INDEX_CONTEXT_CACHE_MAXSIZE = 16
DEFAULT_ASTRO_UI_BASE_URL = "https://cloud.astronomer.io/cljsvo8d800yz01giqt70a7e7"
AIRFLOW_REQUIRED_ENV_VARS = ("AIRFLOW_API_BASE_URL", "AIRFLOW_API_TOKEN")


def _is_truthy_env_var(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _is_development_mode() -> bool:
    if app.debug:
        return True
    if _is_truthy_env_var("FLASK_DEBUG"):
        return True
    if _is_truthy_env_var("DEBUG"):
        return True
    return os.getenv("FLASK_ENV", "").strip().lower() == "development"


def _should_allow_live_airflow_eval_for_dashboard() -> bool:
    return not should_use_redis_cache() and _is_development_mode()


def _get_missing_airflow_env_vars() -> list[str]:
    return [
        env_name for env_name in AIRFLOW_REQUIRED_ENV_VARS if not os.getenv(env_name, "").strip()
    ]


def _add_missing_airflow_config_details(payload: dict[str, Any]) -> dict[str, Any]:
    missing_airflow_env_vars = _get_missing_airflow_env_vars()
    if not missing_airflow_env_vars:
        return payload

    updated_payload = dict(payload)
    updated_payload.update(
        {
            "error_type": "missing_airflow_credentials",
            "missing_airflow_env_vars": missing_airflow_env_vars,
        }
    )
    return updated_payload


def _get_airflow_fleet_health_payload(
    allow_live_eval: bool = True,
) -> tuple[dict[str, Any], int]:
    if should_use_redis_cache():
        cached = get_cached_fleet_health()
        if cached is not None:
            payload, status = cached
            return _add_missing_airflow_config_details(payload), status
        logging.warning(
            "Airflow fleet health cache miss or stale value while REDIS_URL is configured"
        )
        return _add_missing_airflow_config_details({"status": "unknown"}), 503

    if not allow_live_eval:
        logging.warning(
            "Skipping live airflow fleet health evaluation because cached data is required"
        )
        return _add_missing_airflow_config_details({"status": "unknown"}), 503

    try:
        return evaluate_fleet_health()
    except AirflowFleetHealthError:
        payload = _add_missing_airflow_config_details(
            {
                "status": "unknown",
                "error_message": "Unable to evaluate airflow fleet health",
            }
        )
        if payload.get("error_type") == "missing_airflow_credentials":
            logging.warning(
                "Airflow fleet health evaluation skipped because required env vars are missing: %s",
                ", ".join(payload["missing_airflow_env_vars"]),
            )
        else:
            logging.exception("Airflow fleet health evaluation failed")
        return payload, 503


def _coerce_failed_dag_entries(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []

    entries: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        dag_id = item.get("dag_id")
        if not isinstance(dag_id, str) or not dag_id:
            continue
        state = item.get("state")
        entry = {
            "dag_id": dag_id,
            "state": state if isinstance(state, str) and state else "unknown",
            "astro_dag_url": _build_astro_dag_url(dag_id),
        }
        dag_run_id = item.get("dag_run_id")
        if isinstance(dag_run_id, str) and dag_run_id:
            entry["dag_run_id"] = dag_run_id
        entries.append(entry)
    return entries


def _get_astro_ui_base_url() -> str:
    airflow_api_base_url = os.getenv("AIRFLOW_API_BASE_URL", "").strip().rstrip("/")
    if not airflow_api_base_url:
        return DEFAULT_ASTRO_UI_BASE_URL
    return re.sub(r"/api/v\d+$", "", airflow_api_base_url)


def _build_astro_failed_dags_url() -> str:
    return f"{_get_astro_ui_base_url()}/dags?status=failed&state=active"


def _build_astro_dag_url(dag_id: str) -> str:
    dag_id_encoded = quote(dag_id, safe="")
    return f"{_get_astro_ui_base_url()}/dags/{dag_id_encoded}"


def _get_failed_dag_entries(payload: dict[str, Any]) -> tuple[list[dict[str, str]], bool]:
    failed_dags = _coerce_failed_dag_entries(payload.get("failed_dags"))
    if failed_dags:
        return failed_dags, False

    top_failed_dags = _coerce_failed_dag_entries(payload.get("top_failed_dags"))
    failed_runs = payload.get("failed_runs")
    is_partial = (
        bool(top_failed_dags)
        and isinstance(failed_runs, int)
        and len(top_failed_dags) < failed_runs
    )
    return top_failed_dags, is_partial


def _format_checked_at(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        checked_at = datetime.fromisoformat(normalized)
    except ValueError:
        return value

    if checked_at.tzinfo is None:
        return checked_at.strftime("%Y-%m-%d %I:%M:%S %p")
    return checked_at.astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")


def _require_airflow_fleet_monitor_token() -> None:
    expected_token = os.getenv("AIRFLOW_FLEET_MONITOR_TOKEN")
    if not expected_token:
        return

    bearer_token = request.headers.get("Authorization", "").removeprefix("Bearer ")
    request_token = request.args.get("token", default="", type=str)
    if bearer_token != expected_token and request_token != expected_token:
        abort(401)


@app.route("/healthz")
def healthz():
    response = jsonify({"status": "ok"})
    response.headers["Cache-Control"] = "no-store"
    return response, 200


@app.route("/airflow-fleet-health")
def airflow_fleet_health():
    _require_airflow_fleet_monitor_token()

    payload, status = _get_airflow_fleet_health_payload(allow_live_eval=False)

    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store"
    return response, status


@app.route("/failing-dags")
def failing_dags_dashboard():
    payload, status = _get_airflow_fleet_health_payload(
        allow_live_eval=_should_allow_live_airflow_eval_for_dashboard()
    )
    failed_dags, is_partial_list = _get_failed_dag_entries(payload)
    failed_runs = payload.get("failed_runs")
    missing_airflow_env_vars = [
        env_name
        for env_name in payload.get("missing_airflow_env_vars", [])
        if isinstance(env_name, str) and env_name
    ]
    has_missing_airflow_credentials = bool(missing_airflow_env_vars)
    status_variant = (
        "setup-required" if has_missing_airflow_credentials else payload.get("status", "unknown")
    )
    status_label = (
        "Setup required"
        if has_missing_airflow_credentials
        else str(payload.get("status", "unknown"))
    )

    return render_template(
        "failing_dags.html",
        astro_failed_dags_url=_build_astro_failed_dags_url(),
        checked_at=_format_checked_at(payload.get("checked_at")),
        dags_without_runs=payload.get("dags_without_runs"),
        evaluated_dags=payload.get("evaluated_dags"),
        failed_dag_count=(failed_runs if isinstance(failed_runs, int) else len(failed_dags)),
        failed_dags=failed_dags,
        failed_fetches=payload.get("failed_fetches"),
        failure_ratio=payload.get("failure_ratio"),
        has_missing_airflow_credentials=has_missing_airflow_credentials,
        http_status=status,
        is_partial_failed_dag_list=is_partial_list,
        missing_airflow_env_vars=missing_airflow_env_vars,
        non_terminal_dags=payload.get("non_terminal_dags"),
        status=payload.get("status", "unknown"),
        status_label=status_label,
        status_variant=status_variant,
        threshold_ratio=payload.get("threshold_ratio"),
        total_active_dags=payload.get("active_dags_total"),
    )


def record_breakdown(
    store_points: dict[str, dict[str, int]],
    store_counts: dict[str, dict[str, int]],
    key: str,
    category: str,
    points: int,
    count_increment: int = 0,
) -> None:
    if points == 0:
        return
    person_points = store_points.setdefault(key, {})
    person_points[category] = person_points.get(category, 0) + points
    if count_increment:
        person_counts = store_counts.setdefault(key, {})
        person_counts[category] = person_counts.get(category, 0) + count_increment


# Maximum time in seconds to wait for background tasks in the index context.
# This shorter timeout is used for multiple concurrent futures in _build_index_context
# where we prefer to show partial data rather than hang indefinitely.
INDEX_FUTURE_TIMEOUT = 10

# Configuration constants
# Timeout in seconds for ThreadPoolExecutor result() calls in individual routes.
# This longer timeout is used for single operations in routes like /team/<slug>.
EXECUTOR_TIMEOUT_SECONDS = 30
# Number of worker threads used in the index route for parallel data fetching
INDEX_THREADPOOL_MAX_WORKERS = 12
# Number of worker threads used in the /team/<slug> route when fetching
# Linear and GitHub data concurrently
TEAM_THREADPOOL_MAX_WORKERS = 3
# Cache time-to-live in seconds for the index page
INDEX_CACHE_TTL_SECONDS = 60


class BreakdownCategory(TypedDict):
    key: str
    label: str
    count_label: str | None


class LeaderboardEntry(TypedDict):
    slug: str | None
    display_name: str | None
    score: int
    breakdown: str | None


BREAKDOWN_CATEGORIES: list[BreakdownCategory] = [
    {"key": "urgent", "label": "Urgent issues", "count_label": "issue"},
    {"key": "high", "label": "High issues", "count_label": "issue"},
    {"key": "medium", "label": "Medium issues", "count_label": "issue"},
    {"key": "low", "label": "Low issues", "count_label": "issue"},
    {"key": "reviews", "label": "PR reviews", "count_label": "review"},
    {"key": "prs", "label": "PRs merged", "count_label": "PR"},
    {"key": "cycle_lead", "label": "Completed project lead", "count_label": None},
    {"key": "cycle_member", "label": "Completed project member", "count_label": None},
]

PRIORITY_BREAKDOWN_KEYS = {
    1: "urgent",
    2: "high",
    3: "medium",
    4: "low",
    5: "low",
}


def format_breakdown_text(
    points_map: dict[str, int] | None, count_map: dict[str, int] | None
) -> str:
    if not points_map:
        return ""
    count_map = count_map or {}
    lines: list[str] = []
    for entry in BREAKDOWN_CATEGORIES:
        key = entry["key"]
        points = points_map.get(key, 0)
        if not points:
            continue
        line = f"{entry['label']}: {points} pts"
        count = count_map.get(key, 0)
        count_label = entry["count_label"]
        if count and count_label:
            label = count_label if count == 1 else f"{count_label}s"
            line = f"{line} ({count} {label})"
        lines.append(line)
    return "\n".join(lines)


@app.template_filter("first_name")
def first_name_filter(name: str) -> str:
    parts = re.split(r"[.\-\s]+", name)
    if parts and parts[0]:
        return parts[0].title()
    return name.title()


@app.template_filter("mmdd")
def mmdd_filter(date_str: str) -> str:
    """Format an ISO date string as MM/DD."""
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str).date()
        return dt.strftime("%m/%d")
    except ValueError:
        return date_str


ResultType = TypeVar("ResultType")


def get_future_result_with_timeout(
    future: Future[ResultType], default_value: ResultType, timeout: int = INDEX_FUTURE_TIMEOUT
) -> ResultType:
    """
    Get result from a future with a timeout, returning a default value on timeout.

    Args:
        future: The concurrent.futures.Future to get result from
        default_value: Value to return if timeout occurs
        timeout: Maximum time to wait in seconds (default: INDEX_FUTURE_TIMEOUT)

    Returns:
        The future's result, or default_value if timeout occurs
    """
    try:
        return future.result(timeout=timeout)
    except TimeoutError:
        return default_value


def _build_leaderboard_entries(
    days: int,
    completed_bugs: list,
    completed_new_features: list,
    completed_technical_changes: list,
    merged_reviews: dict,
    merged_authored_prs: dict,
) -> list[LeaderboardEntry]:
    config_data = load_config()
    people_config = config_data.get("people", {})
    engineering_team_slugs = {
        slug for slug, info in people_config.items() if info.get("team") == ENGINEERING_TEAM_SLUG
    }

    alias_to_slug = {}
    github_to_slug = {}
    display_name_overrides = {}

    for slug, info in people_config.items():
        linear_username = info.get("linear_username") or slug
        display_name_overrides[slug] = format_display_name(linear_username)
        for alias in {
            slug,
            linear_username,
            display_name_overrides[slug],
        }:
            normalized = normalize_identity(alias)
            if normalized:
                alias_to_slug[normalized] = slug
        github_username = info.get("github_username")
        if github_username:
            github_to_slug[normalize_identity(github_username)] = slug

    def resolve_slug(*identities: str | None) -> str | None:
        for identity in identities:
            normalized = normalize_identity(identity)
            if normalized and normalized in alias_to_slug:
                return alias_to_slug[normalized]
        return None

    scores_by_slug: dict[str, int] = {}
    scores_by_external: dict[str, int] = {}
    names_by_external: dict[str, str] = {}
    names_by_slug: dict[str, str] = {}
    points_breakdown_by_slug: dict[str, dict[str, int]] = {}
    points_breakdown_by_external: dict[str, dict[str, int]] = {}
    count_breakdown_by_slug: dict[str, dict[str, int]] = {}
    count_breakdown_by_external: dict[str, dict[str, int]] = {}

    completed_work = completed_bugs + completed_new_features + completed_technical_changes

    for issue in completed_work:
        assignee = issue.get("assignee")
        if not assignee:
            continue
        raw_identity = assignee.get("name") or assignee.get("displayName") or ""
        display_name = assignee.get("displayName") or format_display_name(raw_identity)
        slug = resolve_slug(assignee.get("name"), assignee.get("displayName"))
        priority = issue.get("priority")
        points = PRIORITY_TO_SCORE.get(priority, 0)
        category_key = PRIORITY_BREAKDOWN_KEYS.get(priority)
        if slug:
            scores_by_slug[slug] = scores_by_slug.get(slug, 0) + points
            names_by_slug.setdefault(
                slug,
                display_name or display_name_overrides.get(slug, display_name),
            )
            if category_key:
                record_breakdown(
                    points_breakdown_by_slug,
                    count_breakdown_by_slug,
                    slug,
                    category_key,
                    points,
                    1,
                )
        else:
            key = normalize_identity(display_name) or normalize_identity(raw_identity)
            if not key:
                continue
            scores_by_external[key] = scores_by_external.get(key, 0) + points
            names_by_external.setdefault(key, display_name or raw_identity)
            if category_key:
                record_breakdown(
                    points_breakdown_by_external,
                    count_breakdown_by_external,
                    key,
                    category_key,
                    points,
                    1,
                )

    for reviewer, prs in merged_reviews.items():
        review_points = len(prs)
        if review_points == 0:
            continue
        slug = github_to_slug.get(normalize_identity(reviewer))
        if slug:
            scores_by_slug[slug] = scores_by_slug.get(slug, 0) + review_points
            names_by_slug.setdefault(
                slug, display_name_overrides.get(slug, format_display_name(reviewer))
            )
            record_breakdown(
                points_breakdown_by_slug,
                count_breakdown_by_slug,
                slug,
                "reviews",
                review_points,
                review_points,
            )
        else:
            key = normalize_identity(reviewer)
            if not key:
                continue
            scores_by_external[key] = scores_by_external.get(key, 0) + review_points
            names_by_external.setdefault(key, format_display_name(reviewer))
            record_breakdown(
                points_breakdown_by_external,
                count_breakdown_by_external,
                key,
                "reviews",
                review_points,
                review_points,
            )

    for author, prs in merged_authored_prs.items():
        pr_points = len(prs)
        if pr_points == 0:
            continue
        slug = github_to_slug.get(normalize_identity(author))
        if slug:
            scores_by_slug[slug] = scores_by_slug.get(slug, 0) + pr_points
            names_by_slug.setdefault(
                slug, display_name_overrides.get(slug, format_display_name(author))
            )
            record_breakdown(
                points_breakdown_by_slug,
                count_breakdown_by_slug,
                slug,
                "prs",
                pr_points,
                pr_points,
            )
        else:
            key = normalize_identity(author)
            if not key:
                continue
            scores_by_external[key] = scores_by_external.get(key, 0) + pr_points
            names_by_external.setdefault(key, format_display_name(author))
            record_breakdown(
                points_breakdown_by_external,
                count_breakdown_by_external,
                key,
                "prs",
                pr_points,
                pr_points,
            )

    cycle_lead_points = calculate_cycle_project_lead_points(days)
    for lead_name, points in cycle_lead_points.items():
        slug = resolve_slug(lead_name, format_display_name(lead_name))
        if slug:
            scores_by_slug[slug] = scores_by_slug.get(slug, 0) + points
            names_by_slug.setdefault(
                slug, display_name_overrides.get(slug, format_display_name(lead_name))
            )
            record_breakdown(
                points_breakdown_by_slug,
                count_breakdown_by_slug,
                slug,
                "cycle_lead",
                points,
            )
        else:
            key = normalize_identity(lead_name)
            if not key:
                continue
            scores_by_external[key] = scores_by_external.get(key, 0) + points
            names_by_external.setdefault(key, format_display_name(lead_name))
            record_breakdown(
                points_breakdown_by_external,
                count_breakdown_by_external,
                key,
                "cycle_lead",
                points,
            )

    cycle_member_points = calculate_cycle_project_member_points(days)
    for member_name, points in cycle_member_points.items():
        slug = resolve_slug(member_name, format_display_name(member_name))
        if slug:
            scores_by_slug[slug] = scores_by_slug.get(slug, 0) + points
            names_by_slug.setdefault(
                slug, display_name_overrides.get(slug, format_display_name(member_name))
            )
            record_breakdown(
                points_breakdown_by_slug,
                count_breakdown_by_slug,
                slug,
                "cycle_member",
                points,
            )
        else:
            key = normalize_identity(member_name)
            if not key:
                continue
            scores_by_external[key] = scores_by_external.get(key, 0) + points
            names_by_external.setdefault(key, format_display_name(member_name))
            record_breakdown(
                points_breakdown_by_external,
                count_breakdown_by_external,
                key,
                "cycle_member",
                points,
            )

    leaderboard_entries: list[LeaderboardEntry] = [
        {
            "slug": slug,
            "display_name": names_by_slug.get(slug) or display_name_overrides.get(slug),
            "score": score,
            "breakdown": format_breakdown_text(
                points_breakdown_by_slug.get(slug),
                count_breakdown_by_slug.get(slug),
            )
            or None,
        }
        for slug, score in scores_by_slug.items()
    ]
    leaderboard_entries.extend(
        [
            {
                "slug": None,
                "display_name": names_by_external[key],
                "score": score,
                "breakdown": format_breakdown_text(
                    points_breakdown_by_external.get(key),
                    count_breakdown_by_external.get(key),
                )
                or None,
            }
            for key, score in scores_by_external.items()
        ]
    )

    leaderboard_entries = [
        entry
        for entry in leaderboard_entries
        if (slug := entry.get("slug")) is not None and slug in engineering_team_slugs
    ]

    leaderboard_entries.sort(key=lambda entry: entry["score"], reverse=True)

    return leaderboard_entries


@lru_cache(maxsize=INDEX_CONTEXT_CACHE_MAXSIZE)
def _build_priority_stats_context(days: int, _cache_epoch: int) -> dict:
    with ThreadPoolExecutor(max_workers=INDEX_THREADPOOL_MAX_WORKERS) as executor:
        created_priority_future = executor.submit(get_created_issues, 2, "Bug", days)
        completed_priority_future = executor.submit(get_completed_issues_summary, 2, "Bug", days)
        completed_bugs_future = executor.submit(get_completed_issues_summary, 5, "Bug", days)
        completed_new_features_future = executor.submit(
            get_completed_issues_summary, 5, "New Feature", days
        )
        completed_technical_changes_future = executor.submit(
            get_completed_issues_summary, 5, "Technical Change", days
        )

    created_priority_bugs = get_future_result_with_timeout(created_priority_future, [])
    completed_priority_result = get_future_result_with_timeout(completed_priority_future, [])
    completed_priority_bugs = [
        issue for issue in completed_priority_result if not issue.get("project")
    ]
    completed_bugs_result = get_future_result_with_timeout(completed_bugs_future, [])
    completed_bugs = [issue for issue in completed_bugs_result if not issue.get("project")]
    completed_new_features_result = get_future_result_with_timeout(
        completed_new_features_future, []
    )
    completed_new_features = [
        issue for issue in completed_new_features_result if not issue.get("project")
    ]
    completed_technical_changes_result = get_future_result_with_timeout(
        completed_technical_changes_future, []
    )
    completed_technical_changes = [
        issue for issue in completed_technical_changes_result if not issue.get("project")
    ]

    time_data = get_time_data(completed_priority_bugs)
    fixes_per_day = (
        len(completed_bugs + completed_new_features + completed_technical_changes) / days
        if days
        else 0
    )

    total_completed_issues = len(
        completed_bugs + completed_new_features + completed_technical_changes
    )
    if total_completed_issues:
        priority_percentage = int(
            round(len(completed_priority_bugs) / total_completed_issues * 100)
        )
    else:
        priority_percentage = 0

    issues_by_platform = by_platform(created_priority_bugs)
    platform_labels = list(issues_by_platform.keys())
    platform_values = [len(issues_by_platform[label]) for label in platform_labels]

    return {
        "days": days,
        "issue_count": len(created_priority_bugs),
        "fixes_per_day": fixes_per_day,
        "priority_percentage": priority_percentage,
        "queue_time_data": time_data["queue"],
        "lead_time_data": time_data["lead"],
        "platform_labels": platform_labels,
        "platform_values": platform_values,
    }


@lru_cache(maxsize=INDEX_CONTEXT_CACHE_MAXSIZE)
def _build_open_items_context(days: int, _cache_epoch: int) -> dict:
    with ThreadPoolExecutor(max_workers=INDEX_THREADPOOL_MAX_WORKERS) as executor:
        open_priority_future = executor.submit(get_open_issues, 2, "Bug")
        open_bugs_future = executor.submit(get_open_issues, 5, "Bug")
        open_new_features_future = executor.submit(get_open_issues, 5, "New Feature")
        open_technical_changes_future = executor.submit(get_open_issues, 5, "Technical Change")

    open_priority_bugs = get_future_result_with_timeout(open_priority_future, [])
    open_bugs_result = get_future_result_with_timeout(open_bugs_future, [])
    open_new_features_result = get_future_result_with_timeout(open_new_features_future, [])
    open_technical_changes_result = get_future_result_with_timeout(
        open_technical_changes_future, []
    )
    open_work = open_bugs_result + open_new_features_result + open_technical_changes_result

    return {
        "days": days,
        "priority_issues": sorted(open_priority_bugs, key=lambda x: x["createdAt"]),
        "open_assigned_work": sorted(
            [
                issue
                for issue in open_work
                if issue["assignee"] is not None and issue["priority"] > 2
            ],
            key=lambda x: x["createdAt"],
            reverse=True,
        ),
    }


@lru_cache(maxsize=INDEX_CONTEXT_CACHE_MAXSIZE)
def _build_leaderboard_context(days: int, _cache_epoch: int) -> dict:
    with ThreadPoolExecutor(max_workers=INDEX_THREADPOOL_MAX_WORKERS) as executor:
        completed_bugs_future = executor.submit(get_completed_issues_summary, 5, "Bug", days)
        completed_new_features_future = executor.submit(
            get_completed_issues_summary, 5, "New Feature", days
        )
        completed_technical_changes_future = executor.submit(
            get_completed_issues_summary, 5, "Technical Change", days
        )
        reviews_future = executor.submit(merged_prs_by_reviewer, days)
        authored_prs_future = executor.submit(merged_prs_by_author, days)

    completed_bugs_result = get_future_result_with_timeout(completed_bugs_future, [])
    completed_bugs = [issue for issue in completed_bugs_result if not issue.get("project")]
    completed_new_features_result = get_future_result_with_timeout(
        completed_new_features_future, []
    )
    completed_new_features = [
        issue for issue in completed_new_features_result if not issue.get("project")
    ]
    completed_technical_changes_result = get_future_result_with_timeout(
        completed_technical_changes_future, []
    )
    completed_technical_changes = [
        issue for issue in completed_technical_changes_result if not issue.get("project")
    ]

    merged_reviews = get_future_result_with_timeout(reviews_future, {})
    merged_authored_prs = get_future_result_with_timeout(authored_prs_future, {})

    leaderboard_entries = _build_leaderboard_entries(
        days=days,
        completed_bugs=completed_bugs,
        completed_new_features=completed_new_features,
        completed_technical_changes=completed_technical_changes,
        merged_reviews=merged_reviews,
        merged_authored_prs=merged_authored_prs,
    )

    return {
        "days": days,
        "leaderboard_entries": leaderboard_entries,
    }


# use a query string parameter for days on the index route
@app.route("/")
def index():
    days = request.args.get("days", default=30, type=int)
    return render_template("index.html", days=days)


@app.route("/partials/index/priority-stats")
def index_priority_stats_partial():
    days = request.args.get("days", default=30, type=int)
    cache_epoch = int(time.time() / INDEX_CACHE_TTL_SECONDS)
    context = _build_priority_stats_context(days, cache_epoch)
    return render_template("partials/index_priority_stats.html", **context)


@app.route("/partials/index/open-items")
def index_open_items_partial():
    days = request.args.get("days", default=30, type=int)
    cache_epoch = int(time.time() / INDEX_CACHE_TTL_SECONDS)
    context = _build_open_items_context(days, cache_epoch)
    return render_template("partials/index_open_items.html", **context)


@app.route("/partials/index/leaderboard")
def index_leaderboard_partial():
    days = request.args.get("days", default=30, type=int)
    cache_epoch = int(time.time() / INDEX_CACHE_TTL_SECONDS)
    context = _build_leaderboard_context(days, cache_epoch)
    return render_template("partials/index_leaderboard.html", **context)


@app.route("/team/<slug>")
def team_slug(slug):
    """Display open and completed work for a team member."""
    days = request.args.get("days", default=30, type=int)
    config = load_config()
    person_cfg = config.get("people", {}).get(slug)
    if not person_cfg:
        abort(404)
    login = person_cfg.get("linear_username", slug)
    person_name = login.replace(".", " ").replace("-", " ").title()
    return render_template(
        "person.html",
        person_slug=slug,
        person_name=person_name,
        days=days,
    )


@app.route("/team")
def team():
    return render_template("team.html")


@app.route("/partials/team/content")
def team_content_partial():
    cache_epoch = int(time.time() / INDEX_CACHE_TTL_SECONDS)
    context = _build_team_context(cache_epoch)
    return render_template("partials/team_content.html", **context)


@app.route("/partials/team/<slug>/content")
def team_person_content_partial(slug):
    days = request.args.get("days", default=30, type=int)
    config = load_config()
    person_cfg = config.get("people", {}).get(slug)
    if not person_cfg:
        abort(404)
    cache_epoch = int(time.time() / INDEX_CACHE_TTL_SECONDS)
    context = _build_person_context(slug, days, cache_epoch)
    return render_template("partials/person_content.html", **context)


@lru_cache(maxsize=INDEX_CONTEXT_CACHE_MAXSIZE)
def _build_team_context(_cache_epoch: int) -> dict:
    config = load_config()
    people_config = config.get("people", {})
    engineering_team_slugs = {
        slug for slug, info in people_config.items() if info.get("team") == ENGINEERING_TEAM_SLUG
    }

    def format_name(key):
        data = people_config.get(key, {})
        name = data.get("linear_username", key)
        return name.replace(".", " ").replace("-", " ").title()

    def normalize(name: str) -> str:
        """Normalize a Linear display name or username for comparison."""
        return name.replace(".", " ").replace("-", " ").title()

    name_to_slug = {}
    for slug, info in people_config.items():
        username = info.get("linear_username", slug)
        full = normalize(username)
        # Map the full normalized name to the slug
        name_to_slug[full] = slug
        first = full.split()[0]
        # Also map first name if unique (don't overwrite existing mapping)
        name_to_slug.setdefault(first, slug)

    def slug_for_name(name: str | None) -> str | None:
        if not name:
            return None
        normalized = normalize(name)
        if not normalized:
            return None
        slug = name_to_slug.get(normalized)
        if slug:
            return slug
        parts = normalized.split()
        if not parts:
            return None
        return name_to_slug.get(parts[0])

    def project_has_engineering_member(project: dict) -> bool:
        """Return True when a project includes an engineering team member."""
        participants: list[str] = []
        lead = (project.get("lead") or {}).get("displayName")
        if lead:
            participants.append(lead)
        members = project.get("members") or []
        participants.extend(members)
        for name in participants:
            slug = slug_for_name(name)
            if slug and slug in engineering_team_slugs:
                return True
        return False

    cycle_projects = get_projects()
    _annotate_project_schedule_fields(cycle_projects)
    for proj in cycle_projects:
        proj["is_inactive"] = is_inactive_project(proj)

    # group projects by initiatives
    projects_by_initiative: dict[str, list[dict[str, Any]]] = {}
    seen_project_ids = set()
    for project in cycle_projects:
        project_id = project.get("id") or project.get("name")
        if project_id in seen_project_ids:
            continue
        seen_project_ids.add(project_id)
        nodes = project.get("initiatives", {}).get("nodes", [])
        initiative_names = [init.get("name") or "Unnamed Initiative" for init in nodes]
        if not initiative_names:
            initiative_names = ["No Initiative"]
        primary_initiative = sorted(initiative_names)[0]
        projects_by_initiative.setdefault(primary_initiative, []).append(project)
    # sort initiatives alphabetically
    projects_by_initiative = dict(sorted(projects_by_initiative.items(), key=lambda x: x[0]))

    # Separate completed or incomplete projects from the initiative buckets
    completed_projects = []
    for name, projects in list(projects_by_initiative.items()):
        remaining = []
        for project in projects:
            if not project_has_engineering_member(project):
                continue
            if project.get("is_inactive"):
                completed_projects.append(project)
            else:
                remaining.append(project)
        if remaining:
            projects_by_initiative[name] = remaining
        else:
            del projects_by_initiative[name]

    # Determine which team members are participating in active projects
    active_projects = [p for projs in projects_by_initiative.values() for p in projs]

    cycle_member_slugs = set()
    member_projects: dict[str, set[tuple[str, str]]] = {}
    for project in active_projects:
        # Only include projects that have started (start date today or earlier)
        starts_in = project.get("starts_in")
        if starts_in is not None and starts_in > 0:
            continue
        lead = (project.get("lead") or {}).get("displayName")
        participants = []
        if lead:
            participants.append(lead)
        participants.extend(project.get("members", []))
        for name in participants:
            slug = slug_for_name(name)
            if slug and slug in engineering_team_slugs:
                project_name = project.get("name")
                project_url = project.get("url")
                if not isinstance(project_name, str) or not isinstance(project_url, str):
                    continue
                cycle_member_slugs.add(slug)
                member_projects.setdefault(slug, set()).add((project_name, project_url))

    # Convert sets back to sorted lists of dicts
    member_projects_list = {
        slug: [{"name": name, "url": url} for name, url in sorted(projects, key=lambda x: x[0])]
        for slug, projects in member_projects.items()
    }

    developers = sorted(
        [{"slug": slug, "name": format_name(slug)} for slug in cycle_member_slugs],
        key=lambda d: d["name"],
    )

    support_slugs = [slug for slug in get_support_slugs() if slug in engineering_team_slugs]
    on_call_support = sorted(
        [{"slug": name, "name": format_name(name)} for name in support_slugs],
        key=lambda d: d["name"],
    )

    # Map open priority bug issues to on-call support members
    priority_bugs = get_open_issues(2, "Bug")
    bugs_by_assignee = by_assignee(priority_bugs)
    support_issues = {}
    for assignee, data in bugs_by_assignee.items():
        slug = slug_for_name(assignee)
        if slug and slug in engineering_team_slugs:
            support_issues[slug] = [
                {"title": issue["title"], "url": issue["url"]} for issue in data["issues"]
            ]

    return {
        "developers": developers,
        "developer_projects": member_projects_list,
        "cycle_projects_by_initiative": projects_by_initiative,
        "completed_cycle_projects": completed_projects,
        "on_call_support": on_call_support,
        "support_issues": support_issues,
    }


@lru_cache(maxsize=INDEX_CONTEXT_CACHE_MAXSIZE)
def _build_person_context(slug: str, days: int, _cache_epoch: int) -> dict:
    config = load_config()
    person_cfg = config.get("people", {}).get(slug) or {}
    login = person_cfg.get("linear_username", slug)
    person_name = login.replace(".", " ").replace("-", " ").title()
    raw_github_username = person_cfg.get("github_username")
    github_username = (
        raw_github_username
        if isinstance(raw_github_username, str) and raw_github_username
        else None
    )
    with ThreadPoolExecutor(max_workers=TEAM_THREADPOOL_MAX_WORKERS) as executor:
        open_future = executor.submit(get_open_issues_for_person, login)
        completed_future = executor.submit(get_completed_issues_for_person, login, days)
        github_future = None
        if github_username:
            github_future = executor.submit(
                lambda: (
                    merged_prs_by_author(days),
                    merged_prs_by_reviewer(days),
                )
            )
        open_items = sorted(
            open_future.result(timeout=EXECUTOR_TIMEOUT_SECONDS),
            key=lambda x: x["updatedAt"],
            reverse=True,
        )
        completed_items = sorted(
            completed_future.result(timeout=EXECUTOR_TIMEOUT_SECONDS),
            key=lambda x: x["completedAt"],
            reverse=True,
        )
        if github_future and github_username:
            author_map, reviewer_map = github_future.result(timeout=EXECUTOR_TIMEOUT_SECONDS)
            prs_merged = len(author_map.get(github_username, []))
            prs_reviewed = len(reviewer_map.get(github_username, []))
        else:
            prs_merged = prs_reviewed = 0

    priority_fix_times = []
    priority_bugs_fixed = 0
    for issue in completed_items:
        is_priority_bug = issue.get("priority", 5) <= 2 and any(
            lbl.get("name") == "Bug" for lbl in issue.get("labels", {}).get("nodes", [])
        )
        if not is_priority_bug:
            continue
        priority_bugs_fixed += 1
        if issue.get("assignee_time_to_fix") is not None:
            fix_time = issue["assignee_time_to_fix"]
            priority_fix_times.append(fix_time)

    if priority_fix_times:
        avg_priority_bug_fix = int(sum(priority_fix_times) / len(priority_fix_times))
    else:
        avg_priority_bug_fix = None

    # Compute metrics for all completed work
    all_work_done = len(completed_items)
    all_fix_times = [
        issue["assignee_time_to_fix"]
        for issue in completed_items
        if issue.get("assignee_time_to_fix") is not None
    ]
    if all_fix_times:
        avg_all_time_to_fix = int(sum(all_fix_times) / len(all_fix_times))
    else:
        avg_all_time_to_fix = None

    # Group open and completed items by project
    open_by_project = by_project(open_items)
    completed_by_project = by_project(completed_items)

    for issues in open_by_project.values():
        issues.sort(key=lambda x: x["updatedAt"], reverse=True)
    for issues in completed_by_project.values():
        issues.sort(key=lambda x: x["completedAt"], reverse=True)

    # Fetch all projects and annotate date helpers
    cycle_projects = get_projects()
    _annotate_project_schedule_fields(cycle_projects)
    for proj in cycle_projects:
        proj["is_inactive"] = is_inactive_project(proj)

    def _normalize_text(value: str | None) -> str:
        if not value:
            return ""
        cleaned = value.replace(".", " ").replace("-", " ").strip()
        return re.sub(r"\s+", " ", cleaned).lower()

    def normalize_display_name(value: str | None) -> str:
        return _normalize_text(value)

    normalized_person_name = normalize_display_name(
        person_cfg.get("linear_display_name") or person_name
    )
    led_projects = [
        project
        for project in cycle_projects
        if normalize_display_name((project.get("lead") or {}).get("displayName"))
        == normalized_person_name
    ]
    lead_completed_projects = sum(1 for project in led_projects if is_completed_project(project))
    lead_incomplete_projects = sum(1 for project in led_projects if is_incomplete_project(project))
    lead_current_projects = sum(1 for project in led_projects if not project.get("is_inactive"))
    lead_completed_project_variances = [
        variance_days
        for project in led_projects
        if is_completed_project(project)
        for variance_days in [get_project_schedule_variance_days(project)]
        if variance_days is not None
    ]
    if lead_completed_project_variances:
        average_completed_project_variance = sum(lead_completed_project_variances) / len(
            lead_completed_project_variances
        )
    else:
        average_completed_project_variance = None

    project_names = {proj.get("name") for proj in cycle_projects if proj.get("name")}

    on_support = slug in get_support_slugs()
    if on_support:
        open_current_cycle = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj in ["Customer Success", "No Project"]
        }
        open_other = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj not in ["Customer Success", "No Project"]
        }
    else:
        open_current_cycle = {
            proj: issues for proj, issues in open_by_project.items() if proj in project_names
        }
        open_other = {
            proj: issues for proj, issues in open_by_project.items() if proj not in project_names
        }

    work_by_platform = by_platform(open_items + completed_items)
    platform_labels = list(work_by_platform.keys())
    platform_values = [len(work_by_platform[label]) for label in platform_labels]

    return {
        "person_slug": slug,
        "person_name": person_name,
        "linear_username": login,
        "github_username": github_username,
        "days": days,
        "open_current_cycle": open_current_cycle,
        "open_other": open_other,
        "completed_by_project": completed_by_project,
        "on_call_support": on_support,
        "work_by_platform": work_by_platform,
        "prs_merged": prs_merged,
        "prs_reviewed": prs_reviewed,
        "priority_bug_avg_time_to_fix": avg_priority_bug_fix,
        "priority_bugs_fixed": priority_bugs_fixed,
        "all_work_done": all_work_done,
        "avg_all_time_to_fix": avg_all_time_to_fix,
        "lead_completed_projects": lead_completed_projects,
        "lead_current_projects": lead_current_projects,
        "lead_incomplete_projects": lead_incomplete_projects,
        "lead_completed_projects_avg_early_late": format_average_project_schedule_variance(
            average_completed_project_variance
        ),
        "platform_labels": platform_labels,
        "platform_values": platform_values,
    }


if __name__ == "__main__":
    app.run()
