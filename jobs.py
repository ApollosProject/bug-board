import logging
import os
import time
from datetime import datetime

import requests
import schedule
from dotenv import load_dotenv

from config import load_config
from constants import PRIORITY_TO_SCORE
from github import (
    get_prs_waiting_for_review_by_reviewer,
    get_prs_with_changes_requested_by_reviewer,
)
from linear import (
    get_completed_issues,
    get_open_issues,
    get_projects,
    get_stale_issues_by_assignee,
)
from openai_client import get_chat_function_call

load_dotenv()


def format_bug_line(bug):
    """Return a formatted Slack message line for a bug."""
    reviewer = (
        get_slack_markdown_by_linear_username(bug["assignee"]["displayName"])
        if bug["assignee"]
        else ""
    )
    platform_text = f", {bug['platform']}" if bug["platform"] else ""
    reviewer_text = f", {reviewer}" if reviewer else ""
    return (
        f"- <{bug['url']}|{bug['title']}> "
        f"(+{bug['daysOpen']}d{platform_text}{reviewer_text})"
    )


def with_retries(func):
    def wrapper(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Function {func.__name__} failed: {e}")
                if i == 2:
                    raise
                time.sleep(5)

    return wrapper


def get_slack_markdown_by_linear_username(username):
    config = load_config()
    for person in config["people"]:
        if config["people"][person]["linear_username"] == username:
            return f"<@{config['people'][person]['slack_id']}>"
    return "No Assignee"


@with_retries
def post_priority_bugs():
    config = load_config()
    open_priority_bugs = get_open_issues(2, "Bug")
    unassigned = [bug for bug in open_priority_bugs if bug["assignee"] is None]
    at_risk = [
        bug
        for bug in open_priority_bugs
        if bug["daysOpen"] > 4 and bug["daysOpen"] <= 7
    ]
    overdue = [bug for bug in open_priority_bugs if bug["daysOpen"] > 7]

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
        notified = set()
        for platform in platforms:
            platform_slug = platform.lower().replace(" ", "-")
            lead = config["platforms"][platform_slug]["lead"]
            lead_info = config["people"][lead]
            notified.add(f"<@{lead_info['slack_id']}> ({platform} Lead)")
            for developer in config["platforms"][platform_slug]["developers"]:
                person = config["people"][developer]
                if person["linear_username"] not in assigned and person.get(
                    "on_call_support", False
                ):
                    notified.add(f"<@{person['slack_id']}>")
        if notified:
            notified_text = "\n".join(notified)
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
        url = os.getenv("SLACK_WEBHOOK_URL")
        requests.post(url, json={"text": markdown})


@with_retries
def post_leaderboard():
    items = (
        get_completed_issues(5, "Bug", 7)
        + get_completed_issues(5, "New Feature", 7)
        + get_completed_issues(5, "Technical Change", 7)
    )
    priority_to_score = PRIORITY_TO_SCORE
    leaderboard = {}
    for item in items:
        assignee = item["assignee"]
        if not assignee:
            continue
        assignee_name = assignee["displayName"]
        if assignee_name not in leaderboard:
            leaderboard[assignee_name] = 0
        score = priority_to_score.get(item["priority"], 0)
        leaderboard[assignee_name] += score
    leaderboard = dict(sorted(leaderboard.items(), key=lambda x: x[1], reverse=True))
    medals = ["🥇", "🥈", "🥉"]
    markdown = "*Weekly Leaderboard*\n\n"
    for i, (assignee, score) in enumerate(leaderboard.items()):
        if i >= 3:
            break
        slack_markdown = get_slack_markdown_by_linear_username(assignee)
        markdown += f"{medals[i]} {slack_markdown}: {score}\n"
    markdown += "\n\n"
    markdown += "_scores - 10pts for high, 5pts for medium, 1pt for low_\n\n"
    markdown += f"<{os.getenv('APP_URL')}?days=7|View Bug Board>"
    url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(url, json={"text": markdown})


@with_retries
def post_stale():
    config = load_config()
    people_by_github_username = {
        person["github_username"]: person for person in config["people"].values()
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
        markdown += "*PRs - Checks Passing, Waiting for Review*\n"
        for reviewer, pr_list in prs.items():
            if not pr_list:
                continue
            unique_prs = {pr["url"]: pr for pr in pr_list}.values()
            reviwer_slack_id = people_by_github_username.get(reviewer, {}).get(
                "slack_id"
            )
            if reviwer_slack_id:
                reviewer_slack_markdown = f"<@{reviwer_slack_id}>"
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

    if any(issues for issues in stale_issues.values()):
        markdown += "*Stale Open Issues*\n"
        for assignee, issues in stale_issues.items():
            if not issues:
                continue
            assignee_slack_markdown = get_slack_markdown_by_linear_username(assignee)
            markdown += f"\n{assignee_slack_markdown}:\n\n"
            for issue in issues:
                markdown += (
                    f"- <{issue['url']}|{issue['title']}>" f" ({issue['daysStale']}d)\n"
                )
        markdown += "\n\n"
    markdown += f"<{os.getenv('APP_URL')}|View Bug Board>"

    url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(url, json={"text": markdown})


@with_retries
def post_upcoming_projects():
    """Notify leads about projects starting on Monday."""
    projects = get_projects()
    upcoming = []
    today = datetime.utcnow().date()
    for project in projects:
        start = project.get("startDate")
        if not start:
            continue
        try:
            start_dt = datetime.fromisoformat(start).date()
        except ValueError:
            continue
        days_until = (start_dt - today).days
        if start_dt.weekday() == 0 and 0 <= days_until <= 5:
            lead = project.get("lead", {}).get("displayName")
            lead_md = get_slack_markdown_by_linear_username(lead) if lead else "No Lead"
            upcoming.append(f"- <{project['url']}|{project['name']}> - Lead: {lead_md}")
    if upcoming:
        markdown = "*Projects Starting Monday*\n\n" + "\n".join(upcoming)
        url = os.getenv("SLACK_WEBHOOK_URL")
        requests.post(url, json={"text": markdown})


@with_retries
def post_friday_deadlines():
    """Notify leads about projects ending on Friday."""
    projects = get_projects()
    upcoming = []
    today = datetime.utcnow().date()
    for project in projects:
        target = project.get("targetDate")
        if not target:
            continue
        try:
            target_dt = datetime.fromisoformat(target).date()
        except ValueError:
            continue
        days_until = (target_dt - today).days
        if target_dt.weekday() == 4 and 0 <= days_until <= 5:
            lead = project.get("lead", {}).get("displayName")
            lead_md = get_slack_markdown_by_linear_username(lead) if lead else "No Lead"
            upcoming.append(f"- <{project['url']}|{project['name']}> - Lead: {lead_md}")
    if upcoming:
        markdown = "*Projects Due Friday*\n\n" + "\n".join(upcoming)
        url = os.getenv("SLACK_WEBHOOK_URL")
        requests.post(url, json={"text": markdown})


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

    # Include only issues without a project or from the configured cycle initiative projects
    config = load_config()
    cycle_init = config.get("cycle_initiative")
    if cycle_init:
        projects = get_projects()
        cycle_projects = [
            p["name"]
            for p in projects
            if any(
                node.get("name") == cycle_init
                for node in p.get("initiatives", {}).get("nodes", [])
            )
        ]
        issues = [
            issue
            for issue in issues
            if not issue.get("project") or issue.get("project") in cycle_projects
        ]
    else:
        issues = [issue for issue in issues if not issue.get("project")]

    if not issues:
        return

    chunks = []
    for issue in issues:
        desc = issue.get("description") or ""
        comments = " ".join(
            c.get("body", "") for c in issue.get("comments", {}).get("nodes", [])
        )
        chunks.append(
            f"ID: {issue['id']}\n"
            f"Title: {issue['title']}\n"
            f"Platform: {issue.get('platform', '')}\n"
            f"Description: {desc}\n"
            f"Comments: {comments}"
        )

    instructions = (
        "Create a short customer-facing changelog from the provided issues. "
        "Each issue chunk begins with 'ID: <issue id>'. "
        "Group items under 'New Features', 'Bug Fixes', and 'Improvements'. "
        "List each change as a short sentence with no markdown or bullet characters. "
        "Ignore technical tasks, internal changes, and unfinished work. "
        "Ensure each change appears only once in the changelog. "
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
            input_text,
            function_spec,
            "generate_changelog",
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
    requests.post(os.getenv("SLACK_WEBHOOK_URL"), json={"text": changelog_text})


if os.getenv("DEBUG") == "true":
    post_priority_bugs()
    post_leaderboard()
    post_weekly_changelog()
    post_stale()
    post_upcoming_projects()
    post_friday_deadlines()
else:
    schedule.every(1).days.at("12:00").do(post_priority_bugs)
    schedule.every().friday.at("20:00").do(post_leaderboard)
    schedule.every().thursday.at("19:00").do(post_weekly_changelog)
    schedule.every(1).days.at("14:00").do(post_stale)
    schedule.every().thursday.at("12:00").do(post_upcoming_projects)
    schedule.every().monday.at("12:00").do(post_friday_deadlines)

    while True:
        schedule.run_pending()
        time.sleep(1)
