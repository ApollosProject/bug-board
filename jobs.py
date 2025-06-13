import logging
import os
import time

import requests
import schedule
import yaml
from dotenv import load_dotenv

from github import get_prs_waiting_for_review_by_reviewer
from linear import get_completed_issues, get_open_issues, get_stale_issues_by_assignee

load_dotenv()


def with_retries(func):
    def wrapper(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Function {func.__name__} failed: {e}")
                time.sleep(5)

    return wrapper


def get_slack_markdown_by_linear_username(username):
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    for person in config["people"]:
        if config["people"][person]["linear_username"] == username:
            return f"<@{config['people'][person]['slack_id']}>"
    return "No Assignee"


# @with_retries
def post_priority_bugs():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
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
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['platform'] if bug['platform'] else ''})"
                for bug in sorted(unassigned, key=lambda x: x["daysOpen"], reverse=True)
            ]
        )
        markdown += "\n\n"
        assigned = set(
            [
                bug["assignee"]["displayName"]
                for bug in open_priority_bugs
                if bug["assignee"]
            ]
        )
        platforms = set([bug["platform"] for bug in unassigned if bug["platform"]])
        notified = set()
        for platform in platforms:
            platform_slug = platform.lower().replace(" ", "-")
            lead = config["platforms"][platform_slug]["lead"]
            notified.add(f"<@{config['people'][lead]['slack_id']}> ({platform} Lead)")
            for developer in config["platforms"][platform_slug]["developers"]:
                person = config["people"][developer]
                if person["linear_username"] not in assigned:
                    notified.add(f"<@{person['slack_id']}>")
        if notified:
            notified_text = "\n".join(notified)
            markdown += f"attn:\n\n{notified_text}"
    if at_risk:
        markdown += "\n\n*At Risk*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['platform'] if bug['platform'] else ''}{', ' + get_slack_markdown_by_linear_username(bug['assignee']['displayName']) if bug['assignee'] else ''})"
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
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['platform'] if bug['platform'] else ''}{', ' + get_slack_markdown_by_linear_username(bug['assignee']['displayName']) if bug['assignee'] else ''})"
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


# @with_retries
def post_leaderboard():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    items = get_completed_issues(5, "Bug", 7) + get_completed_issues(
        5, "New Feature", 7
    )
    priority_to_score = {1: 4, 2: 4, 3: 2, 4: 1, 5: 1}
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
    markdown += "_scores - 4pts for high, 2pts for medium, 1pt for low_\n\n"
    markdown += f"<{os.getenv('APP_URL')}/7|View Bug Board>"
    url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(url, json={"text": markdown})


# @with_retries
def post_stale():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    people_by_github_username = {
        person["github_username"]: person for person in config["people"].values()
    }
    prs = get_prs_waiting_for_review_by_reviewer()
    stale_issues = get_stale_issues_by_assignee(
        get_open_issues(5, "Bug") + get_open_issues(5, "New Feature"), 7
    )
    if not prs and not stale_issues:
        return

    markdown = ""
    if prs:
        markdown += "*PRs Waiting for Review*\n"
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
            for pr in unique_prs:
                markdown += f"- <{pr['url']}|{pr['title']}>\n"
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
                    f"- <{issue['url']}|{issue['title']}> ({issue['daysStale']}d)\n"
                )
        markdown += "\n\n"
    markdown += f"<{os.getenv('APP_URL')}|View Bug Board>"

    url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(url, json={"text": markdown})


if os.getenv("DEBUG") == "true":
    post_priority_bugs()
    post_leaderboard()
    post_stale()
else:
    schedule.every(1).days.at("12:00").do(post_priority_bugs)
    schedule.every().friday.at("20:00").do(post_leaderboard)
    schedule.every(1).days.at("14:00").do(post_stale)

    while True:
        schedule.run_pending()
        time.sleep(1)
