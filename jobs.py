import logging
import os
import time

import requests
import schedule
from dotenv import load_dotenv

from bb import get_open_issues

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
def post_unassigned_priority_bugs():
    open_priority_bugs = get_open_issues(2, "Bug")
    unassigned_priority_bugs = [
        bug for bug in open_priority_bugs if bug["assignee"] is None
    ]

    if not unassigned_priority_bugs:
        return

    markdown = "*Unassigned Priority Bugs*\n\n"
    markdown += "\n".join(
        [
            f"- <{bug['url']}|{bug['title']}>{' - ' + bug['platform'] if bug['platform'] else ''}"
            for bug in unassigned_priority_bugs
        ]
    )
    markdown += "\n\n"
    markdown += f"<{os.getenv('APP_URL')}|View Dashboard>"

    url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(url, json={"text": markdown})


schedule.every(1).days.at("12:00").do(post_unassigned_priority_bugs)

while True:
    schedule.run_pending()
    time.sleep(1)
