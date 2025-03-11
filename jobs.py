import logging
import os
import time

import requests
import schedule
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
        markdown += "*At Risk Issues*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['assignee']['name'] if bug['assignee'] else ''})"
                for bug in sorted(
                    at_risk,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
    if overdue:
        markdown += "*Overdue Issues*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['assignee']['name'] if bug['assignee'] else ''})"
                for bug in sorted(
                    overdue,
                    key=lambda x: x["daysOpen"],
                    reverse=True,
                )
            ]
        )
        markdown += "\n\n"
    if markdown:
        markdown += f"attn: @cleaners\n\n<{os.getenv('APP_URL')}|View Dashboard>"
        url = os.getenv("SLACK_WEBHOOK_URL")
        requests.post(url, json={"text": markdown})


schedule.every(1).days.at("12:00").do(post_priority_bugs)

while True:
    schedule.run_pending()
    time.sleep(1)
