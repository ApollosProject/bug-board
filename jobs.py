import logging
import os
import re
import time
from datetime import datetime, timezone
from functools import wraps

import requests
import schedule
from dotenv import load_dotenv

from config import load_config
from constants import PRIORITY_TO_SCORE
from github import (
    get_pr_diff,
    get_prs_waiting_for_review_by_reviewer,
    get_prs_with_changes_requested_by_reviewer,
    merged_prs_by_author,
    merged_prs_by_reviewer,
)
from linear.issues import (
    get_completed_issues,
    get_completed_issues_for_person,
    get_open_issues,
    get_stale_issues_by_assignee,
)
from linear.projects import get_projects
from openai_client import get_chat_function_call
from support import get_support_slugs
from leaderboard import (
    calculate_cycle_project_lead_points,
    calculate_cycle_project_member_points,
)

load_dotenv()

# Retry configuration for the with_retries decorator.
RETRY_COUNT = 3
RETRY_SLEEP_SECONDS = 5


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


def post_to_manager_slack(markdown: str):
    """Send a message to the manager Slack webhook and raise or log on failure."""
    url = os.environ.get("MANAGER_SLACK_WEBHOOK_URL")
    if not url:
        logging.error(
            "MANAGER_SLACK_WEBHOOK_URL environment variable is not set; "
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
    reviewer = (
        get_slack_markdown_by_linear_username(bug["assignee"]["displayName"])
        if bug["assignee"]
        else ""
    )
    platform_text = f", {bug['platform']}" if bug["platform"] else ""
    reviewer_text = f", {reviewer}" if reviewer else ""
    content = (
        f"<{bug['url']}|{bug['title']}> "
        f"(+{bug['daysOpen']}d{platform_text}{reviewer_text})"
    )
    if bug.get("priority") == 1:
        return f"- \U0001f6a8 {content} \U0001f6a8"
    return f"- {content}"


def with_retries(func):
    """Decorator that retries the wrapped function up to 3 times.

    On each failure, it logs the exception, waits 5 seconds, and retries.
    After the third failed attempt, the last exception is re-raised.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(RETRY_COUNT):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Function {func.__name__} failed: {e}")
                if attempt == RETRY_COUNT - 1:
                    raise
                time.sleep(RETRY_SLEEP_SECONDS)

    return wrapper


def get_team_members(team_slug: str):
    """Return the subset of people config entries on a given team."""
    config = load_config()
    people = config.get("people", {})
    return {
        slug: info
        for slug, info in people.items()
        if info.get("team") == team_slug
    }


def get_slack_markdown_by_linear_username(username):
    config = load_config()
    for person in config["people"]:
        if config["people"][person]["linear_username"] == username:
            return f"<@{config['people'][person]['slack_id']}>"
    return "No Assignee"


def get_slack_markdown_by_github_username(username):
    config = load_config()
    for person in config["people"].values():
        if person.get("github_username") == username:
            return f"<@{person['slack_id']}>"
    return username


def _get_pr_diffs(issue):
    """Return a list of diffs for PRs linked in the issue attachments."""
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
            diffs.append(diff)
        except Exception as e:  # pragma: no cover - network errors are ignored
            logging.error(
                "Failed to fetch diff for %s/%s#%s: %s", owner, repo, number, e
            )
    return diffs


@with_retries
def post_priority_bugs():
    config = load_config()
    open_priority_bugs = get_open_issues(2, "Bug")
    unassigned = [bug for bug in open_priority_bugs if bug["assignee"] is None]
    urgent_bugs = [bug for bug in open_priority_bugs if bug["priority"] == 1]
    high_bugs = [bug for bug in open_priority_bugs if bug["priority"] == 2]

    # Urgent bugs are due after one day. Mark them at risk immediately and
    # overdue if not fixed within a day. High priority bugs retain the
    # existing week-long window.
    at_risk = [bug for bug in urgent_bugs if bug["daysOpen"] <= 1] + [
        bug for bug in high_bugs if bug["daysOpen"] > 4 and bug["daysOpen"] <= 7
    ]
    overdue = [bug for bug in urgent_bugs if bug["daysOpen"] > 1] + [
        bug for bug in high_bugs if bug["daysOpen"] > 7
    ]

    markdown = ""
    if unassigned:
        markdown += "*Unassigned Priority Bugs*\n\n"
        markdown += "\n".join(
            [
                format_bug_line(bug)
                for bug in sorted(
                    unassigned,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
        assigned = {
            bug["assignee"]["displayName"]
            for bug in open_priority_bugs
            if bug["assignee"]
        }
        platforms = {bug["platform"] for bug in unassigned if bug["platform"]}
        notified_slack_ids: set[str] = set()
        slug_by_slack_id: dict[str, str] = {}
        lead_platforms_by_slack_id: dict[str, set[str]] = {}
        support_slugs = get_support_slugs()
        for platform in platforms:
            platform_slug = platform.lower().replace(" ", "-")
            platform_config = config["platforms"].get(platform_slug, {})

            participant_roles = []
            lead_slug = platform_config.get("lead")
            if lead_slug:
                participant_roles.append((lead_slug, True))
            for developer_slug in platform_config.get("developers", []):
                participant_roles.append((developer_slug, False))

            for slug, is_lead in participant_roles:
                person = config["people"].get(slug)
                if not person:
                    continue
                if slug not in support_slugs:
                    continue
                if person["linear_username"] in assigned:
                    continue
                slack_id = person.get("slack_id")
                if not slack_id:
                    continue
                notified_slack_ids.add(slack_id)
                slug_by_slack_id.setdefault(slack_id, slug)
                if is_lead:
                    lead_platforms_by_slack_id.setdefault(slack_id, set()).add(platform)

        if notified_slack_ids:
            notified_lines = []
            for slack_id in sorted(
                notified_slack_ids,
                key=lambda sid: slug_by_slack_id.get(sid, ""),
            ):
                mention = f"<@{slack_id}>"
                lead_platforms = sorted(lead_platforms_by_slack_id.get(slack_id, set()))
                if lead_platforms:
                    if len(lead_platforms) == 1:
                        mention = f"{mention} ({lead_platforms[0]} Lead)"
                    else:
                        lead_text = ", ".join(f"{p} Lead" for p in lead_platforms)
                        mention = f"{mention} ({lead_text})"
                notified_lines.append(mention)

            notified_text = "\n".join(notified_lines)
            markdown += f"attn:\n\n{notified_text}"
    if at_risk:
        markdown += "\n\n*At Risk*\n\n"
        markdown += "\n".join(
            [
                format_bug_line(bug)
                for bug in sorted(
                    at_risk,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
    if overdue:
        markdown += "\n\n*Overdue*\n\n"
        markdown += "\n".join(
            [
                format_bug_line(bug)
                for bug in sorted(
                    overdue,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
    if markdown:
        markdown += f"\n\n<{os.getenv('APP_URL')}|View Bug Board>"
        post_to_slack(markdown)


@with_retries
def post_leaderboard():
    days = 7
    config = load_config()
    people_config = config.get("people", {})
    apollos_team_slugs = {
        slug
        for slug, info in people_config.items()
        if info.get("team") == "apollos_engineering"
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
        if alias_to_slug.get(normalize_identity(assignee)) in apollos_team_slugs
    }
    leaderboard = dict(sorted(filtered_leaderboard.items(), key=lambda x: x[1], reverse=True))
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
    apollos_team_members = get_team_members("apollos_engineering")
    people_by_github_username = {
        person.get("github_username"): person
        for person in apollos_team_members.values()
        if person.get("github_username")
    }
    apollos_linear_usernames = {
        person.get("linear_username")
        for person in apollos_team_members.values()
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
        markdown += "*PRs - Checks Passing, Waiting for Review (+12h, <200 lines added)*\n"
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
                    if created.endswith("Z"):
                        created = created[:-1]
                    dt = datetime.fromisoformat(created)
                    days_waiting = (datetime.now() - dt).days
                else:
                    days_waiting = 0
                pr_days.append((days_waiting, pr))

            for days_waiting, pr in sorted(pr_days, key=lambda x: x[0], reverse=True):
                markdown += f"- <{pr['url']}|{pr['title']}> (+{days_waiting}d)\n"
        markdown += "\n\n"

    filtered_stale_issues = {
        assignee: issues
        for assignee, issues in stale_issues.items()
        if assignee in apollos_linear_usernames and issues
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
    apollos_team_members = get_team_members("apollos_engineering")
    inactive = []
    base_url = os.getenv("APP_URL", "")
    for person_key, person in apollos_team_members.items():
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
    upcoming = []
    today = datetime.now(timezone.utc).date()
    canceled_statuses = {"canceled", "cancelled"}
    for project in projects:
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
def post_friday_deadlines():
    """Notify leads about projects ending on Friday."""
    projects = get_projects()
    config = load_config()
    people_config = config.get("people", {})
    apollos_slugs = {
        slug for slug, info in people_config.items() if info.get("team") == "apollos_engineering"
    }

    def normalize(name: str) -> str:
        return name.replace(".", " ").replace("-", " ").title()

    name_to_slug = {}
    for slug, info in people_config.items():
        username = info.get("linear_username") or slug
        full = normalize(username)
        name_to_slug[full] = slug
        first = full.split()[0]
        name_to_slug.setdefault(first, slug)

    def is_apollos_lead_project(project: dict) -> bool:
        lead = (project.get("lead") or {}).get("displayName")
        if not lead:
            return False
        normalized = normalize(lead)
        slug = name_to_slug.get(normalized) or name_to_slug.get(normalized.split()[0])
        return slug in apollos_slugs

    projects = [p for p in projects if is_apollos_lead_project(p)]

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


if os.getenv("DEBUG") == "true":
    post_inactive_engineers()
    post_priority_bugs()
    post_leaderboard()
    post_weekly_changelog()
    post_stale()
    post_upcoming_projects()
    post_friday_deadlines()
else:
    schedule.every().friday.at("13:00").do(post_inactive_engineers)
    schedule.every(1).days.at("12:00").do(post_priority_bugs)
    schedule.every().friday.at("20:00").do(post_leaderboard)
    schedule.every().thursday.at("19:00").do(post_weekly_changelog)
    schedule.every(1).days.at("14:00").do(post_stale)
    schedule.every().friday.at("12:00").do(post_upcoming_projects)
    schedule.every().monday.at("12:00").do(post_friday_deadlines)

    while True:
        schedule.run_pending()
        time.sleep(1)
