import logging
import os
import re
import time
from datetime import datetime, timezone

import requests
import schedule
from dotenv import load_dotenv
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from config import load_config
from constants import ENGINEERING_TEAM_SLUG, PRIORITY_TO_SCORE
from fleet_health_cache import refresh_fleet_health_cache, should_use_redis_cache
from github import (
    get_pr_diff,
    get_prs_waiting_for_review_by_reviewer,
    get_prs_with_changes_requested_by_reviewer,
    merged_prs_by_author,
    merged_prs_by_reviewer,
)
from issue_timing import format_issue_sla_text, parse_linear_dt
from leaderboard import (
    calculate_cycle_project_lead_points,
    calculate_cycle_project_member_points,
)
from linear.issues import (
    get_completed_issues,
    get_completed_issues_for_person,
    get_open_issues,
    get_stale_issues_by_assignee,
)
from linear.projects import get_projects
from openai_client import get_chat_function_call
from project_dates import format_project_target_status, parse_iso_date
from support import get_support_slugs

load_dotenv()

# Retry configuration for the with_retries decorator.
MAX_RETRY_COUNT = 3
RETRY_SLEEP_SECONDS = 5
MAX_DIFF_CHARS = 12000
MAX_DIFF_FILES = 20
FLEET_HEALTH_REFRESH_DEFAULT_SECONDS = 60
INACTIVE_PROJECT_STATUS_NAMES = {
    "completed",
    "incomplete",
    "canceled",
    "cancelled",
    "released",
}


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _parse_linear_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _is_inactive_project(project: dict) -> bool:
    status_name = ((project.get("status") or {}).get("name") or "").strip().lower()
    return bool(project.get("completedAt")) or status_name in INACTIVE_PROJECT_STATUS_NAMES


def _normalize_linear_display_name(name: str) -> str:
    return name.replace(".", " ").replace("-", " ").title()


def _is_engineering_lead_project(project: dict, people_config: dict) -> bool:
    engineering_slugs = {
        slug
        for slug, info in people_config.items()
        if info.get("team") == ENGINEERING_TEAM_SLUG
    }
    name_to_slug = {}
    for slug, info in people_config.items():
        username = info.get("linear_username") or slug
        full = _normalize_linear_display_name(username)
        name_to_slug[full] = slug
        first = full.split()[0]
        name_to_slug.setdefault(first, slug)

    lead = (project.get("lead") or {}).get("displayName")
    if not lead:
        return False
    normalized = _normalize_linear_display_name(lead)
    slug = name_to_slug.get(normalized) or name_to_slug.get(normalized.split()[0])
    return slug in engineering_slugs


def post_to_slack(markdown: str):
    """Send a message to Slack and raise or log on failure."""
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        logging.error("SLACK_WEBHOOK_URL environment variable is not set or empty.")
        raise RuntimeError("Missing SLACK_WEBHOOK_URL environment variable.")
    response = requests.post(url, json={"text": markdown})
    if response.status_code != 200:
        logging.error("Slack API returned %s: %s", response.status_code, response.text)
    response.raise_for_status()


def refresh_airflow_fleet_health_cache_job():
    payload, status = refresh_fleet_health_cache()
    logging.info(
        "Refreshed airflow fleet health cache (status=%s, http_status=%s, evaluated_dags=%s)",
        payload.get("status"),
        status,
        payload.get("evaluated_dags"),
    )


def post_to_manager_slack(markdown: str):
    """Send a message to the manager Slack webhook, or return early if not configured."""
    url = os.environ.get("MANAGER_SLACK_WEBHOOK_URL")
    if not url:
        logging.error(
            "MANAGER_SLACK_WEBHOOK_URL environment variable is not set or empty; "
            "unable to send manager Slack message."
        )
        return
    response = requests.post(url, json={"text": markdown})
    if response.status_code != 200:
        logging.error(
            "Manager Slack API returned %s: %s", response.status_code, response.text
        )
    response.raise_for_status()


def format_bug_line(bug):
    """Return a formatted Slack message line for a bug."""
    sla_text = format_issue_sla_text(bug, now=datetime.now(timezone.utc))
    timing_text = sla_text or f"+{bug['daysOpen']}d"

    reviewer = (
        get_slack_markdown_by_linear_username(bug["assignee"]["displayName"])
        if bug["assignee"]
        else ""
    )
    platform_text = f", {bug['platform']}" if bug["platform"] else ""
    reviewer_text = f", {reviewer}" if reviewer else ""
    content = (
        f"<{bug['url']}|{bug['title']}> "
        f"({timing_text}{platform_text}{reviewer_text})"
    )
    if bug.get("priority") == 1:
        return f"- \U0001f6a8 {content} \U0001f6a8"
    return f"- {content}"


def with_retries(func):
    """Decorator that retries the wrapped function.

    Retries up to MAX_RETRY_COUNT times on failure. After each failure, logs the
    exception and waits RETRY_SLEEP_SECONDS before retrying. After the final
    attempt, the last exception is re-raised.
    """
    logger = logging.getLogger(__name__)
    retry_decorator = retry(
        reraise=True,
        stop=stop_after_attempt(MAX_RETRY_COUNT),
        wait=wait_fixed(RETRY_SLEEP_SECONDS),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    return retry_decorator(func)


def get_team_members(team_slug: str):
    """Return the subset of people config entries on a given team."""
    config = load_config()
    people = config.get("people", {})
    return {
        slug: info for slug, info in people.items() if info.get("team") == team_slug
    }


def get_slack_markdown_by_linear_username(username):
    # Handle missing or empty usernames explicitly to avoid unnecessary config access.
    if username is None or (isinstance(username, str) and not username.strip()):
        return "No Assignee"

    config = load_config()
    for person in config["people"].values():
        if person.get("linear_username") == username:
            return f"<@{person['slack_id']}>"
    return "No Assignee"


def get_slack_markdown_by_github_username(username):
    # Validate input to avoid propagating None or empty usernames.
    if username is None or (isinstance(username, str) and not username.strip()):
        logging.warning(
            "get_slack_markdown_by_github_username called with invalid username: %r",
            username,
        )
        return "Unknown user"
    config = load_config()
    for person in config["people"].values():
        if person.get("github_username") == username:
            return f"<@{person['slack_id']}>"
    return username


def _get_pr_diffs(issue):
    """Return a list of diffs for PRs linked in the issue attachments."""

    def summarize_diff(diff_text: str) -> str:
        files = []
        for line in diff_text.splitlines():
            if not line.startswith("diff --git "):
                continue
            parts = line.split(" ")
            if len(parts) < 4:
                continue
            path = parts[2]
            if path.startswith("a/"):
                path = path[2:]
            if path and path not in files:
                files.append(path)
        total_files = len(files)
        shown_files = files[:MAX_DIFF_FILES]
        file_list = ", ".join(shown_files)
        if total_files > MAX_DIFF_FILES:
            file_list += f", +{total_files - MAX_DIFF_FILES} more"
        if not file_list:
            file_list = "File list unavailable"
        return (
            f"Diff too large ({len(diff_text)} chars). "
            f"Files ({total_files}): {file_list}"
        )

    diffs = []
    for attachment in issue.get("attachments", {}).get("nodes", []):
        metadata = attachment.get("metadata", {})
        url = metadata.get("url")
        if not url:
            continue
        match = re.search(r"github.com/([^/]+)/([^/]+)/pull/(\d+)", url)
        if not match:
            continue
        owner, repo, number = match.groups()
        try:
            diff = get_pr_diff(owner, repo, int(number))
            if len(diff) > MAX_DIFF_CHARS:
                diffs.append(summarize_diff(diff))
            else:
                diffs.append(diff)
        except Exception as e:  # pragma: no cover - network errors are ignored
            logging.error(
                "Failed to fetch diff for %s/%s#%s (error type: %s)",
                owner,
                repo,
                number,
                type(e).__name__,
            )
    return diffs


@with_retries
def post_priority_bugs():
    config = load_config()
    open_priority_bugs = get_open_issues(2, "Bug")
    unassigned = [bug for bug in open_priority_bugs if bug["assignee"] is None]
    now = datetime.now(timezone.utc)

    def issue_has_sla(issue: dict) -> bool:
        return any(
            issue.get(field_name)
            for field_name in (
                "slaType",
                "slaStartedAt",
                "slaMediumRiskAt",
                "slaHighRiskAt",
                "slaBreachesAt",
            )
        )

    def issue_reached_sla(issue: dict, field_name: str) -> bool:
        reached_at = parse_linear_dt(issue.get(field_name))
        if not reached_at:
            return False
        return reached_at <= now

    open_priority_bugs = [bug for bug in open_priority_bugs if issue_has_sla(bug)]
    unassigned = [bug for bug in open_priority_bugs if bug["assignee"] is None]

    overdue = [
        bug for bug in open_priority_bugs if issue_reached_sla(bug, "slaBreachesAt")
    ]
    overdue_ids = {bug["id"] for bug in overdue if bug.get("id")}
    at_risk = [
        bug
        for bug in open_priority_bugs
        if bug.get("id") not in overdue_ids and issue_reached_sla(bug, "slaHighRiskAt")
    ]

    sections = []
    if unassigned:
        unassigned_section = "*Unassigned Priority Bugs*\n\n"
        unassigned_section += "\n".join(
            [
                format_bug_line(bug)
                for bug in sorted(
                    unassigned,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        assigned = {
            bug["assignee"]["displayName"]
            for bug in open_priority_bugs
            if bug["assignee"]
        }
        notified_slack_ids: set[str] = set()
        slug_by_slack_id: dict[str, str] = {}
        support_slugs = get_support_slugs()
        for slug in support_slugs:
            person = config["people"].get(slug)
            if not person:
                continue
            if person.get("linear_username") in assigned:
                continue
            slack_id = person.get("slack_id")
            if not slack_id:
                continue
            notified_slack_ids.add(slack_id)
            slug_by_slack_id.setdefault(slack_id, slug)

        if notified_slack_ids:
            notified_lines = []
            for slack_id in sorted(
                notified_slack_ids,
                key=lambda sid: slug_by_slack_id.get(sid, ""),
            ):
                notified_lines.append(f"<@{slack_id}>")

            notified_text = "\n".join(notified_lines)
            unassigned_section += f"\n\nattn:\n\n{notified_text}"
        sections.append(unassigned_section)
    if at_risk:
        sections.append(
            "*At Risk*\n\n"
            + "\n".join(
                [
                    format_bug_line(bug)
                    for bug in sorted(
                        at_risk,
                        key=lambda x: x["daysOpen"],
                        reverse=True,
                    )
                ]
            )
        )
    if overdue:
        sections.append(
            "*Overdue*\n\n"
            + "\n".join(
                [
                    format_bug_line(bug)
                    for bug in sorted(
                        overdue,
                        key=lambda x: x["daysOpen"],
                        reverse=True,
                    )
                ]
            )
        )
    if sections:
        markdown = "\n\n".join(sections)
        markdown += f"\n\n<{os.getenv('APP_URL')}|View Bug Board>"
        post_to_slack(markdown)


@with_retries
def post_leaderboard():
    days = 7
    config = load_config()
    people_config = config.get("people", {})
    engineering_team_slugs = {
        slug
        for slug, info in people_config.items()
        if info.get("team") == ENGINEERING_TEAM_SLUG
    }

    def normalize_identity(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"[^a-z0-9]", "", value.lower())

    alias_to_slug = {}
    for slug, info in people_config.items():
        linear_username = info.get("linear_username") or slug
        display_alias = (
            re.sub(r"[._-]+", " ", linear_username).title() if linear_username else slug
        )
        aliases = {slug, linear_username, display_alias}
        slack_id = info.get("slack_id")
        if slack_id:
            aliases.add(f"<@{slack_id}>")
        github_username = info.get("github_username")
        if github_username:
            aliases.add(github_username)
        for alias in aliases:
            normalized = normalize_identity(alias)
            if normalized:
                alias_to_slug[normalized] = slug

    items = (
        get_completed_issues(5, "Bug", days)
        + get_completed_issues(5, "New Feature", days)
        + get_completed_issues(5, "Technical Change", days)
    )
    items = [item for item in items if not item.get("project")]
    priority_to_score = PRIORITY_TO_SCORE
    leaderboard = {}
    for item in items:
        assignee = item["assignee"]
        if not assignee:
            continue
        assignee_name = assignee["displayName"]
        slack_markdown = get_slack_markdown_by_linear_username(assignee_name)
        if slack_markdown not in leaderboard:
            leaderboard[slack_markdown] = 0
        score = priority_to_score.get(item["priority"], 0)
        leaderboard[slack_markdown] += score

    for reviewer, prs in merged_prs_by_reviewer(days).items():
        slack_markdown = get_slack_markdown_by_github_username(reviewer)
        if slack_markdown not in leaderboard:
            leaderboard[slack_markdown] = 0
        leaderboard[slack_markdown] += len(prs)

    for author, prs in merged_prs_by_author(days).items():
        slack_markdown = get_slack_markdown_by_github_username(author)
        if slack_markdown not in leaderboard:
            leaderboard[slack_markdown] = 0
        leaderboard[slack_markdown] += len(prs)

    cycle_points = calculate_cycle_project_lead_points(days)
    for lead_name, points in cycle_points.items():
        slack_markdown = get_slack_markdown_by_linear_username(lead_name)
        key = slack_markdown if slack_markdown != "No Assignee" else lead_name
        leaderboard[key] = leaderboard.get(key, 0) + points

    member_points = calculate_cycle_project_member_points(days)
    for member_name, points in member_points.items():
        slack_markdown = get_slack_markdown_by_linear_username(member_name)
        key = slack_markdown if slack_markdown != "No Assignee" else member_name
        leaderboard[key] = leaderboard.get(key, 0) + points

    filtered_leaderboard = {
        assignee: score
        for assignee, score in leaderboard.items()
        if alias_to_slug.get(normalize_identity(assignee)) in engineering_team_slugs
    }
    leaderboard = dict(
        sorted(filtered_leaderboard.items(), key=lambda x: x[1], reverse=True)
    )
    medals = ["🥇", "🥈", "🥉"]
    markdown = "*Weekly Leaderboard*\n\n"
    for rank, (assignee, score) in enumerate(leaderboard.items()):
        if rank >= 3:
            break
        markdown += f"{medals[rank]} {assignee}: {score}\n"
    markdown += "\n\n"
    markdown += (
        "_scores - 20pts for urgent, 10pts for high, 5pts for medium, 1pt for low, "
        "1pt per merged PR, 1pt per PR review, 30pts/week for completed cycle project leads, "
        "15pts/week for completed cycle project members_\n\n"
    )
    markdown += f"<{os.getenv('APP_URL')}?days={days}|View Bug Board>"
    post_to_slack(markdown)


@with_retries
def post_stale():
    engineering_team_members = get_team_members(ENGINEERING_TEAM_SLUG)
    people_by_github_username = {
        person.get("github_username"): person
        for person in engineering_team_members.values()
        if person.get("github_username")
    }
    engineering_linear_usernames = {
        person.get("linear_username")
        for person in engineering_team_members.values()
        if person.get("linear_username")
    }
    prs = get_prs_waiting_for_review_by_reviewer()
    cr_prs = get_prs_with_changes_requested_by_reviewer()
    stale_issues = get_stale_issues_by_assignee(
        get_open_issues(5, "Bug")
        + get_open_issues(5, "New Feature")
        + get_open_issues(5, "Technical Change"),
        7,
    )
    if not prs and not stale_issues:
        return

    markdown = ""
    filtered = {}
    for reviewer, pr_list in prs.items():
        if reviewer not in people_by_github_username:
            continue
        keep = []
        for pr in pr_list:
            crers = [r for r, pls in cr_prs.items() if pr in pls]
            if any(r != reviewer for r in crers):
                continue
            keep.append(pr)
        if keep:
            filtered[reviewer] = keep
    prs = filtered
    if prs:
        markdown += (
            "*PRs - Checks Passing, Waiting for Review (+24h, <200 lines added)*\n"
        )
        for reviewer, pr_list in prs.items():
            if not pr_list:
                continue
            unique_prs = {pr["url"]: pr for pr in pr_list}.values()
            reviewer_slack_id = people_by_github_username.get(reviewer, {}).get(
                "slack_id"
            )
            if reviewer_slack_id:
                reviewer_slack_markdown = f"<@{reviewer_slack_id}>"
            else:
                reviewer_slack_markdown = reviewer
            markdown += f"\n{reviewer_slack_markdown}:\n\n"
            pr_days = []
            for pr in unique_prs:
                events = [
                    ev
                    for ev in pr.get("timelineItems", {}).get("nodes", [])
                    if ev.get("requestedReviewer", {}).get("login") == reviewer
                ]
                if events:
                    created = max(ev["createdAt"] for ev in events)
                    # Parse GitHub-style ISO 8601 timestamp with explicit UTC timezone
                    dt = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=timezone.utc
                    )
                    days_waiting = (datetime.now(timezone.utc) - dt).days
                else:
                    days_waiting = 0
                pr_days.append((days_waiting, pr))

            for days_waiting, pr in sorted(pr_days, key=lambda x: x[0], reverse=True):
                markdown += f"- <{pr['url']}|{pr['title']}> (+{days_waiting}d)\n"
        markdown += "\n\n"

    filtered_stale_issues = {
        assignee: issues
        for assignee, issues in stale_issues.items()
        if assignee in engineering_linear_usernames and issues
    }

    if not prs and not filtered_stale_issues:
        return

    if any(filtered_stale_issues.values()):
        markdown += "*Stale Open Issues*\n"
        for assignee, issues in filtered_stale_issues.items():
            assignee_slack_markdown = get_slack_markdown_by_linear_username(assignee)
            markdown += f"\n{assignee_slack_markdown}:\n\n"
            for issue in issues:
                markdown += (
                    f"- <{issue['url']}|{issue['title']}>" f" ({issue['daysStale']}d)\n"
                )
        markdown += "\n\n"
    markdown += f"<{os.getenv('APP_URL')}|View Bug Board>"

    post_to_slack(markdown)


@with_retries
def post_inactive_engineers():
    """Send list of engineers with no completed Linear issues in the last 7 days."""
    engineering_team_members = get_team_members(ENGINEERING_TEAM_SLUG)
    inactive = []
    base_url = os.getenv("APP_URL", "")
    for person_key, person in engineering_team_members.items():
        login = person.get("linear_username")
        if not login:
            continue
        try:
            completed = get_completed_issues_for_person(login, 7)
        except Exception as e:
            logging.error(f"Failed to fetch completed issues for {login}: {e}")
            continue
        if not completed:
            # link to user page filtered to last 7 days
            url = f"{base_url.rstrip('/')}/team/{person_key}?days=7"
            inactive.append(f"- <{url}|{person_key}>")
    if not inactive:
        return
    markdown = "*Engineers with no completed issues in the last 7 days*\n\n"
    markdown += "\n".join(inactive)
    post_to_manager_slack(markdown)


@with_retries
def post_upcoming_projects():
    """Notify leads about projects starting on Monday."""
    projects = get_projects()
    people_config = load_config().get("people", {})
    upcoming = []
    today = datetime.now(timezone.utc).date()
    canceled_statuses = {"canceled", "cancelled"}
    for project in projects:
        if not _is_engineering_lead_project(project, people_config):
            continue
        start = project.get("startDate")
        if not start:
            continue
        status_name = (project.get("status") or {}).get("name")
        if status_name and status_name.lower() in canceled_statuses:
            continue
        try:
            start_dt = datetime.fromisoformat(start).date()
        except ValueError:
            continue
        days_until = (start_dt - today).days
        if start_dt.weekday() == 0 and 0 <= days_until <= 5:
            lead = (project.get("lead") or {}).get("displayName")
            if not lead:
                continue
            lead_md = get_slack_markdown_by_linear_username(lead)
            upcoming.append(f"- <{project['url']}|{project['name']}> - Lead: {lead_md}")
    if upcoming:
        markdown = "*Projects Starting Monday*\n\n" + "\n".join(upcoming)
        post_to_slack(markdown)


@with_retries
def post_overdue_projects():
    """Notify Slack about active projects whose target date has passed."""
    projects = get_projects()
    people_config = load_config().get("people", {})
    now = datetime.now(timezone.utc)
    overdue = []

    for project in projects:
        if _is_inactive_project(project):
            continue
        if not _is_engineering_lead_project(project, people_config):
            continue

        target_dt = parse_iso_date(project.get("targetDate"))
        _days_left, target_status_text = format_project_target_status(
            target_dt,
            now=now,
        )
        if not target_dt or not target_status_text or not target_status_text.endswith(
            "overdue"
        ):
            continue

        lead = (project.get("lead") or {}).get("displayName")
        lead_md = get_slack_markdown_by_linear_username(lead) if lead else "No Lead"
        overdue.append(
            {
                "name": project.get("name") or "Untitled Project",
                "target_dt": target_dt,
                "line": (
                    f"- <{project['url']}|{project['name']}> - "
                    f"{target_status_text} - Lead: {lead_md}"
                ),
            }
        )

    if overdue:
        ordered_lines = [
            item["line"]
            for item in sorted(
                overdue,
                key=lambda item: (item["target_dt"], item["name"].lower()),
            )
        ]
        markdown = "*Overdue Projects*\n\n" + "\n".join(ordered_lines)
        post_to_slack(markdown)


@with_retries
def post_friday_deadlines():
    """Notify leads about projects ending on Friday."""
    projects = get_projects()
    people_config = load_config().get("people", {})
    projects = [
        project
        for project in projects
        if _is_engineering_lead_project(project, people_config)
    ]

    upcoming = []
    today = datetime.now(timezone.utc).date()
    inactive_statuses = {"Completed", "Incomplete", "Canceled"}

    for project in projects:
        target = project.get("targetDate")
        if not target:
            continue
        status_name = (project.get("status") or {}).get("name")
        if status_name in inactive_statuses:
            continue
        try:
            target_dt = datetime.fromisoformat(target).date()
        except ValueError:
            continue
        days_until = (target_dt - today).days
        if target_dt.weekday() == 4 and 0 <= days_until <= 5:
            lead = (project.get("lead") or {}).get("displayName")
            lead_md = get_slack_markdown_by_linear_username(lead) if lead else "No Lead"
            upcoming.append(f"- <{project['url']}|{project['name']}> - Lead: {lead_md}")
    if upcoming:
        markdown = "*Projects Due Friday*\n\n" + "\n".join(upcoming)
        post_to_slack(markdown)


@with_retries
def post_weekly_changelog():
    """Generate a customer changelog from completed issues."""

    issues = (
        get_completed_issues(5, "Bug", 7)
        + get_completed_issues(5, "New Feature", 7)
        + get_completed_issues(5, "Technical Change", 7)
    )
    if not issues:
        return

    # remove any duplicate issues by id to avoid repeated entries in changelog
    seen_ids = set()
    unique = []
    for issue in issues:
        if issue.get("id") and issue["id"] not in seen_ids:
            seen_ids.add(issue["id"])
            unique.append(issue)
    issues = unique

    if not issues:
        return

    chunks = []
    for issue in issues:
        desc = issue.get("description") or ""
        comments = " ".join(
            c.get("body", "") for c in issue.get("comments", {}).get("nodes", [])
        )
        diffs = _get_pr_diffs(issue)
        chunk_parts = [
            f"ID: {issue['id']}",
            f"Title: {issue['title']}",
            f"Platform: {issue.get('platform', '')}",
            f"Description: {desc}",
            f"Comments: {comments}",
        ]
        if diffs:
            chunk_parts.append("Diff:\n" + "\n".join(diffs))
        chunks.append("\n".join(chunk_parts))

    instructions = (
        "Create a short customer-facing changelog from the provided issues. "
        "Each issue chunk begins with 'ID: <issue id>'. "
        "Group items under 'New Features', 'Bug Fixes', and 'Improvements'. "
        "List each change as a short sentence with no markdown or bullet characters. "
        "Ignore technical tasks, internal changes, and unfinished work. "
        "Ensure each change appears only once in the changelog. "
        "When a chunk includes a 'Diff:' section, use that diff as additional context. "
        "Return a JSON object with keys 'New Features', 'Bug Fixes', and 'Improvements'. "
        "Each item should be an object with fields 'id' (the issue id)"
        "and 'summary' (the changelog text)."
    )
    input_text = "\n\n".join(chunks)

    # Use OpenAI function calling to generate a structured changelog
    item_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "summary": {"type": "string"},
        },
        "required": ["id", "summary"],
    }

    function_spec = {
        "name": "generate_changelog",
        "description": "Generate a customer-facing changelog.",
        "parameters": {
            "type": "object",
            "properties": {
                "New Features": {"type": "array", "items": item_schema},
                "Bug Fixes": {"type": "array", "items": item_schema},
                "Improvements": {"type": "array", "items": item_schema},
            },
            "required": ["New Features", "Bug Fixes", "Improvements"],
        },
    }
    try:
        changelog_data = get_chat_function_call(
            instructions,
            user_input=input_text,
            functions=function_spec,
            function_call_name="generate_changelog",
        )
    except Exception as e:
        logging.error(
            "Failed to generate changelog via function call. Error: %s",
            e,
        )
        changelog_data = {}

    url_by_id = {issue["id"]: issue["url"] for issue in issues}

    sections = []
    for heading in ["New Features", "Bug Fixes", "Improvements"]:
        items = changelog_data.get(heading, [])
        if items:
            sections.append(f"*{heading}*")
            for item in items:
                summary = item.get("summary", "")
                issue_id = item.get("id")
                url = url_by_id.get(issue_id)
                if url:
                    sections.append(f"- <{url}|{summary}>")
                else:
                    sections.append(f"- {summary}")
            sections.append("")

    changelog_text = "*Changelog (Experimental)*\n\n" + "\n".join(sections).rstrip()
    changelog_text += f"\n\n<{os.getenv('APP_URL')}|View Bug Board>"
    post_to_slack(changelog_text)


def run_debug_jobs() -> None:
    if should_use_redis_cache():
        refresh_airflow_fleet_health_cache_job()
    post_inactive_engineers()
    post_priority_bugs()
    post_leaderboard()
    # post_weekly_changelog()
    post_stale()
    post_overdue_projects()
    post_upcoming_projects()
    post_friday_deadlines()


def configure_scheduled_jobs() -> None:
    if should_use_redis_cache():
        refresh_interval_seconds = _read_positive_int_env(
            "AIRFLOW_FLEET_HEALTH_REFRESH_SECONDS",
            FLEET_HEALTH_REFRESH_DEFAULT_SECONDS,
        )
        schedule.every(refresh_interval_seconds).seconds.do(
            refresh_airflow_fleet_health_cache_job
        )
        refresh_airflow_fleet_health_cache_job()
        logging.info(
            "Scheduled airflow fleet health cache refresh every %s seconds",
            refresh_interval_seconds,
        )
    else:
        logging.info(
            "REDIS_URL not set; airflow fleet health cache refresh is disabled"
        )

    schedule.every().friday.at("13:00").do(post_inactive_engineers)
    schedule.every().day.at("12:00").do(post_priority_bugs)
    schedule.every().friday.at("20:00").do(post_leaderboard)
    # schedule.every().thursday.at("19:00").do(post_weekly_changelog)
    schedule.every().day.at("14:00").do(post_stale)
    schedule.every().day.at("14:00", "America/New_York").do(post_overdue_projects)
    schedule.every().friday.at("12:00").do(post_upcoming_projects)
    schedule.every().monday.at("12:00").do(post_friday_deadlines)


def main() -> None:
    if os.getenv("DEBUG") == "true":
        run_debug_jobs()
        return

    configure_scheduled_jobs()
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
