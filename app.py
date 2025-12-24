import re
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from datetime import datetime
from functools import lru_cache
from typing import TypedDict, TypeVar

from flask import Flask, abort, render_template, request

from config import load_config
from constants import PRIORITY_TO_SCORE
from github import merged_prs_by_author, merged_prs_by_reviewer
from linear.issues import (
    by_assignee,
    by_platform,
    by_project,
    get_completed_issues_summary,
    get_completed_issues_for_person,
    get_created_issues,
    get_open_issues,
    get_open_issues_for_person,
    get_time_data,
)
from linear.projects import get_projects
from support import get_support_slugs
from leaderboard import (
    calculate_cycle_project_lead_points,
    calculate_cycle_project_member_points,
)

app = Flask(__name__)

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


T = TypeVar('T')


def get_future_result_with_timeout(
    future: Future[T],
    default_value: T,
    timeout: int = INDEX_FUTURE_TIMEOUT
) -> T:
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


@lru_cache(maxsize=16)
def _build_index_context(days: int, _cache_epoch: int) -> dict:
    with ThreadPoolExecutor(max_workers=INDEX_THREADPOOL_MAX_WORKERS) as executor:
        created_priority_future = executor.submit(
            get_created_issues, 2, "Bug", days
        )
        open_priority_future = executor.submit(get_open_issues, 2, "Bug")
        completed_priority_future = executor.submit(
            get_completed_issues_summary, 2, "Bug", days
        )
        completed_bugs_future = executor.submit(
            get_completed_issues_summary, 5, "Bug", days
        )
        completed_new_features_future = executor.submit(
            get_completed_issues_summary, 5, "New Feature", days
        )
        completed_technical_changes_future = executor.submit(
            get_completed_issues_summary, 5, "Technical Change", days
        )
        open_bugs_future = executor.submit(get_open_issues, 5, "Bug")
        open_new_features_future = executor.submit(
            get_open_issues, 5, "New Feature"
        )
        open_technical_changes_future = executor.submit(
            get_open_issues, 5, "Technical Change"
        )
        reviews_future = executor.submit(merged_prs_by_reviewer, days)
        authored_prs_future = executor.submit(merged_prs_by_author, days)

    created_priority_bugs = get_future_result_with_timeout(created_priority_future, [])
    open_priority_bugs = get_future_result_with_timeout(open_priority_future, [])

    # Only include non-project issues in the index summary
    completed_priority_result = get_future_result_with_timeout(
        completed_priority_future, []
    )
    completed_priority_bugs = [
        issue
        for issue in completed_priority_result
        if not issue.get("project")
    ]
    completed_bugs_result = get_future_result_with_timeout(completed_bugs_future, [])
    completed_bugs = [
        issue
        for issue in completed_bugs_result
        if not issue.get("project")
    ]
    completed_new_features_result = get_future_result_with_timeout(
        completed_new_features_future, []
    )
    completed_new_features = [
        issue
        for issue in completed_new_features_result
        if not issue.get("project")
    ]
    completed_technical_changes_result = get_future_result_with_timeout(
        completed_technical_changes_future, []
    )
    completed_technical_changes = [
        issue
        for issue in completed_technical_changes_result
        if not issue.get("project")
    ]
    open_bugs_result = get_future_result_with_timeout(open_bugs_future, [])
    open_new_features_result = get_future_result_with_timeout(open_new_features_future, [])
    open_technical_changes_result = get_future_result_with_timeout(
        open_technical_changes_future, []
    )
    open_work = (
        open_bugs_result
        + open_new_features_result
        + open_technical_changes_result
    )
    time_data = get_time_data(completed_priority_bugs)
    fixes_per_day = (
        len(completed_bugs + completed_new_features + completed_technical_changes)
        / days
    )

    config_data = load_config()
    people_config = config_data.get("people", {})
    apollos_team_slugs = {
        slug
        for slug, info in people_config.items()
        if info.get("team") == "apollos_engineering"
    }

    def format_display_name(linear_username: str) -> str:
        return re.sub(r"[._-]+", " ", linear_username).title()

    alias_to_slug = {}
    github_to_slug = {}
    display_name_overrides = {}

    def normalize_identity(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"[^a-z0-9]", "", value.lower())

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
            person_counts[category] = (
                person_counts.get(category, 0) + count_increment
            )

    completed_work = (
        completed_bugs + completed_new_features + completed_technical_changes
    )

    for issue in completed_work:
        assignee = issue.get("assignee")
        if not assignee:
            continue
        raw_identity = assignee.get("name") or assignee.get("displayName") or ""
        display_name = assignee.get("displayName") or format_display_name(
            raw_identity
        )
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

    merged_reviews = get_future_result_with_timeout(reviews_future, {})
    merged_authored_prs = get_future_result_with_timeout(authored_prs_future, {})

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
        if (slug := entry.get("slug")) is not None and slug in apollos_team_slugs
    ]

    leaderboard_entries.sort(key=lambda entry: entry["score"], reverse=True)

    total_completed_issues = len(
        completed_bugs + completed_new_features + completed_technical_changes
    )

    return {
        "days": days,
        "priority_issues": sorted(
            open_priority_bugs, key=lambda x: x["createdAt"]
        ),
        "issue_count": len(created_priority_bugs),
        "priority_percentage": int(round(
            len(completed_priority_bugs) / total_completed_issues * 100
        )) if total_completed_issues else 0,
        "leaderboard_entries": leaderboard_entries,
        "all_issues": created_priority_bugs + open_priority_bugs,
        "issues_by_platform": by_platform(created_priority_bugs),
        "lead_time_data": time_data["lead"],
        "queue_time_data": time_data["queue"],
        "open_assigned_work": sorted(
            [
                issue
                for issue in open_work
                if issue["assignee"] is not None and issue["priority"] > 2
            ],
            key=lambda x: x["createdAt"],
            reverse=True,
        ),
        "fixes_per_day": fixes_per_day,
    }


# use a query string parameter for days on the index route
@app.route("/")
def index():
    days = request.args.get("days", default=30, type=int)
    cache_epoch = int(time.time() / INDEX_CACHE_TTL_SECONDS)
    context = _build_index_context(days, cache_epoch)
    return render_template("index.html", **context)


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
    github_username = person_cfg.get("github_username")
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
        if github_future:
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
    # attach start/target date info and compute days left
    for proj in cycle_projects:
        target = proj.get("targetDate")
        start = proj.get("startDate")
        days_left = None
        starts_in = None
        if target:
            try:
                target_dt = datetime.fromisoformat(target).date()
                days_left = (target_dt - datetime.utcnow().date()).days
            except ValueError:
                # If the target date is not in a valid ISO format, treat it as missing.
                pass
        if start:
            try:
                start_dt = datetime.fromisoformat(start).date()
                starts_in = (start_dt - datetime.utcnow().date()).days
            except ValueError:
                # If the start date is not in a valid ISO format, treat it as missing.
                pass
        proj["days_left"] = days_left
        proj["starts_in"] = starts_in

    def normalize_display_name(value: str | None) -> str:
        if not value:
            return ""
        cleaned = value.replace(".", " ").replace("-", " ").strip()
        return re.sub(r"\s+", " ", cleaned).lower()

    normalized_person_name = normalize_display_name(
        person_cfg.get("linear_display_name") or person_name
    )
    inactive_project_statuses = {"Completed", "Incomplete", "Canceled"}
    led_projects = [
        project
        for project in cycle_projects
        if normalize_display_name(
            (project.get("lead") or {}).get("displayName")
        )
        == normalized_person_name
    ]
    lead_completed_projects = sum(
        1
        for project in led_projects
        if (project.get("status") or {}).get("name") == "Completed"
    )
    lead_incomplete_projects = sum(
        1
        for project in led_projects
        if (project.get("status") or {}).get("name") == "Incomplete"
    )
    lead_current_projects = sum(
        1
        for project in led_projects
        if (project.get("status") or {}).get("name") not in inactive_project_statuses
    )
    project_names = {
        proj.get("name") for proj in cycle_projects if proj.get("name")
    }

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
            proj: issues
            for proj, issues in open_by_project.items()
            if proj in project_names
        }
        open_other = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj not in project_names
        }

    work_by_platform = by_platform(open_items + completed_items)

    return render_template(
        "person.html",
        person_slug=slug,
        person_name=person_name,
        linear_username=login,
        github_username=github_username,
        days=days,
        open_current_cycle=open_current_cycle,
        open_other=open_other,
        completed_by_project=completed_by_project,
        on_call_support=on_support,
        work_by_platform=work_by_platform,
        prs_merged=prs_merged,
        prs_reviewed=prs_reviewed,
        priority_bug_avg_time_to_fix=avg_priority_bug_fix,
        priority_bugs_fixed=priority_bugs_fixed,
        all_work_done=all_work_done,
        avg_all_time_to_fix=avg_all_time_to_fix,
        lead_completed_projects=lead_completed_projects,
        lead_current_projects=lead_current_projects,
        lead_incomplete_projects=lead_incomplete_projects,
    )


@app.route("/team")
def team():
    config = load_config()
    people_config = config.get("people", {})
    apollos_team_slugs = {
        slug
        for slug, info in people_config.items()
        if info.get("team") == "apollos_engineering"
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

    def project_has_apollos_member(project: dict) -> bool:
        """Return True when a project includes an Apollos engineer."""
        participants: list[str] = []
        lead = (project.get("lead") or {}).get("displayName")
        if lead:
            participants.append(lead)
        members = project.get("members") or []
        participants.extend(members)
        for name in participants:
            slug = slug_for_name(name)
            if slug and slug in apollos_team_slugs:
                return True
        return False

    platform_teams = {}
    for slug, info in config.get("platforms", {}).items():
        lead = info.get("lead")
        developers = [dev for dev in info.get("developers", []) if dev != lead]
        developers = sorted(developers, key=lambda d: format_name(d))
        members = [{"name": format_name(lead), "lead": True}] + [
            {"name": format_name(dev), "lead": False} for dev in developers
        ]
        platform_teams[slug] = members
    cycle_projects = get_projects()
    # attach start/target date info and compute days left
    for proj in cycle_projects:
        target = proj.get("targetDate")
        start = proj.get("startDate")
        days_left = None
        starts_in = None
        if target:
            try:
                target_dt = datetime.fromisoformat(target).date()
                days_left = (target_dt - datetime.utcnow().date()).days
            except ValueError:
                # If the target date is malformed, treat it as missing and log a warning.
                app.logger.warning("Invalid targetDate %r for project %r", target, proj.get("id"))
        if start:
            try:
                start_dt = datetime.fromisoformat(start).date()
                starts_in = (start_dt - datetime.utcnow().date()).days
            except ValueError:
                # If the start date is malformed, treat it as missing and log a warning.
                app.logger.warning("Invalid startDate %r for project %r", start, proj.get("id"))
        proj["days_left"] = days_left
        proj["starts_in"] = starts_in

    # group projects by initiatives
    projects_by_initiative = {}
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
    projects_by_initiative = dict(
        sorted(projects_by_initiative.items(), key=lambda x: x[0])
    )

    inactive_project_statuses = {"Completed", "Incomplete", "Canceled"}

    # Separate completed or incomplete projects from the initiative buckets
    completed_projects = []
    for name, projects in list(projects_by_initiative.items()):
        remaining = []
        for project in projects:
            if not project_has_apollos_member(project):
                continue
            if project.get("status", {}).get("name") in inactive_project_statuses:
                completed_projects.append(project)
            else:
                remaining.append(project)
        if remaining:
            projects_by_initiative[name] = remaining
        else:
            del projects_by_initiative[name]

    # Determine which team members are participating in active projects
    active_projects = [
        p for projs in projects_by_initiative.values() for p in projs
    ]

    cycle_member_slugs = set()
    member_projects = {}
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
            if slug and slug in apollos_team_slugs:
                cycle_member_slugs.add(slug)
                member_projects.setdefault(slug, set()).add(
                    (project.get("name"), project.get("url"))
                )

    # Convert sets back to sorted lists of dicts
    member_projects = {
        slug: [
            {"name": name, "url": url}
            for name, url in sorted(projects, key=lambda x: x[0])
        ]
        for slug, projects in member_projects.items()
    }

    developers = sorted(
        [{"slug": slug, "name": format_name(slug)} for slug in cycle_member_slugs],
        key=lambda d: d["name"],
    )

    support_slugs = [slug for slug in get_support_slugs() if slug in apollos_team_slugs]
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
        if slug and slug in apollos_team_slugs:
            support_issues[slug] = [
                {"title": issue["title"], "url": issue["url"]}
                for issue in data["issues"]
            ]

    return render_template(
        "team.html",
        platform_teams=platform_teams,
        developers=developers,
        developer_projects=member_projects,
        cycle_projects_by_initiative=projects_by_initiative,
        completed_cycle_projects=completed_projects,
        on_call_support=on_call_support,
        support_issues=support_issues,
    )


if __name__ == "__main__":
    app.run()
