import logging
import os
import time

import requests
import schedule
from dotenv import load_dotenv

from linear import get_completed_issues, get_open_issues, get_time_data

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
    completed_priority_bugs = get_completed_issues(2, "Bug")
    completed_lead_time_data = get_time_data(completed_priority_bugs)["lead"]
    unassigned_priority_bugs = [
        bug for bug in open_priority_bugs if bug["assignee"] is None
    ]
    open_issues_over_avg_lead_time = [
        bug
        for bug in open_priority_bugs
        if bug["daysOpen"] > completed_lead_time_data["avg"]
    ]

    markdown = ""
    if unassigned_priority_bugs:
        markdown += "*Unassigned Priority Bugs*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['platform'] if bug['platform'] else ''})"
                for bug in sorted(
                    unassigned_priority_bugs, key=lambda x: x["daysOpen"], reverse=True
                )
            ]
        )
        markdown += "\n\n"
    if open_issues_over_avg_lead_time:
        markdown += "*At Risk Issues*\n\n"
        markdown += "\n".join(
            [
                f"- <{bug['url']}|{bug['title']}>{' (' + '+' + str(bug['daysOpen']) + 'd'}{', ' + bug['assignee']['name'] if bug['assignee'] else ''})"
                for bug in sorted(
                    open_issues_over_avg_lead_time,
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
