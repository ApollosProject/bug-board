import logging
import os
import time

import requests
import schedule
import yaml
from dotenv import load_dotenv

from linear import get_open_issues

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


@with_retries
def post_priority_bugs():
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
    if at_risk:
        markdown += "*At Risk*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['platform'] if bug['platform'] else ''}{', ' + bug['assignee']['name'] if bug['assignee'] else ''})"
                for bug in sorted(
                    at_risk,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
    if overdue:
        markdown += "*Overdue*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['platform'] if bug['platform'] else ''}{', ' + bug['assignee']['name'] if bug['assignee'] else ''})"
                for bug in sorted(
                    overdue,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
    if markdown:
        with open("config.yml", "r") as file:
            config = yaml.safe_load(file)
        all_issues = unassigned + at_risk + overdue
        assigned = set(
            [
                bug["assignee"]["displayName"]
                for bug in open_priority_bugs
                if bug["assignee"]
            ]
        )
        platforms = set([bug["platform"] for bug in all_issues if bug["platform"]])
        notified = set()
        for platform in platforms:
            lead = config["platforms"][platform.lower()]["lead"]
            notified.add(f"<@{config['people'][lead]['slack_id']}> ({platform} Lead)")
            for developer in config["platforms"][platform.lower()]["developers"]:
                person = config["people"][developer]
                if person["linear_username"] not in assigned:
                    notified.add(f"<@{person['slack_id']}>")
        if notified:
            notified_text = "\n".join(notified)
            markdown += f"attn:\n\n{notified_text}"
        markdown += f"\n\n<{os.getenv('APP_URL')}|View Bug Board>"
        url = os.getenv("SLACK_WEBHOOK_URL")
        requests.post(url, json={"text": markdown})


if os.getenv("DEBUG") == "true":
    post_priority_bugs()
else:
    schedule.every(1).days.at("12:00").do(post_priority_bugs)
    while True:
        schedule.run_pending()
        time.sleep(1)
