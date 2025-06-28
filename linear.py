import os
from datetime import datetime

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from config import get_platforms

load_dotenv()


headers = {"Authorization": os.getenv("LINEAR_API_KEY")}
transport = AIOHTTPTransport(
    url="https://api.linear.app/graphql",
    headers=headers,
)
client = Client(transport=transport, fetch_schema_from_transport=True)


def get_open_issues(priority, label):

    params = {"priority": priority, "label": label}
    query = gql(
        """
        query PriorityIssues ($priority: Float, $label: String) {
          issues(
            filter: {
              labels: { name: { eq: $label } }
              project: { name: { eq: "Customer Success" } }
              priority: { lte: $priority }
              state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }
            }
            orderBy: createdAt
          ) {
            nodes {
              id
              title
              assignee {
                displayName
                name
              }
              url
              labels {
                nodes {
                  name
                }
              }
              createdAt
              updatedAt
              priority
            }
          }
        }
    """
    )

    # Execute the query on the transport
    data = client.execute(query, variable_values=params)
    issues = data["issues"]["nodes"]
    # add in platform (its the labels minus the label param above)
    for issue in issues:
        platforms = [
            tag["name"]
            for tag in issue["labels"]["nodes"]
            if tag["name"] != label
            and tag["name"].lower().replace(" ", "-") in get_platforms()
        ]
        issue["platform"] = platforms[0] if platforms else None
        issue["daysOpen"] = (
            datetime.utcnow()
            - datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        ).days
    return issues


def get_completed_issues(priority, label, days=30):

    query = gql(
        """
        query CompletedIssues (
            $priority: Float,
            $label: String,
            $days: DateTimeOrDuration,
            $cursor: String
        ) {
          issues(
            first: 50
            after: $cursor
            filter: {
              labels: { name: { eq: $label } }
              project: { name: { eq: "Customer Success" } }
              priority: { lte: $priority }
              state: { name: { in: ["Done"] } }
              completedAt:{gt: $days}
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              assignee {
                name
                displayName
                email
              }
              url
              labels {
                nodes {
                  name
                }
              }
              priority
              completedAt
              createdAt
              startedAt
              attachments {
                nodes {
                  metadata
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
    )

    cursor = None
    issues = []
    while True:
        params = {
            "priority": priority,
            "label": label,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = client.execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]
    return issues


def get_created_issues(priority, label, days=30):

    query = gql(
        """
        query CreatedIssues (
            $priority: Float,
            $label: String,
            $days: DateTimeOrDuration,
            $cursor: String
        ) {
            issues(
                first: 50
                after: $cursor
                filter: {
                    labels: { name: { eq: $label } }
                    project: { name: { eq: "Customer Success" } }
                    priority: { lte: $priority }
                    createdAt:{gt: $days}
                }
                orderBy: createdAt
            ) {
                nodes {
                    id
                    title
                    labels {
                        nodes {
                        name
                        }
                    }
                    createdAt
                }
                pageInfo {
                  hasNextPage
                  endCursor
                }
            }
        }
        """
    )

    cursor = None
    issues = []
    while True:
        params = {
            "priority": priority,
            "label": label,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = client.execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]
    for issue in issues:
        platforms = [
            tag["name"]
            for tag in issue["labels"]["nodes"]
            if tag["name"] != label
            and tag["name"].lower().replace(" ", "-") in get_platforms()
        ]
        issue["platform"] = platforms[0] if platforms else None
    return issues


def by_assignee(issues):
    assignee_issues = {}
    for issue in issues:
        if not issue["assignee"]:
            continue
        assignee = issue["assignee"]["name"]
        if assignee not in assignee_issues:
            assignee_issues[assignee] = {"score": 0, "issues": []}
        assignee_issues[assignee]["issues"].append(issue)
        # high - 4, medium - 2, everything else - 1
        priority_to_score = {1: 4, 2: 4, 3: 2, 4: 1, 5: 1}
        score = priority_to_score.get(issue["priority"], 1)
        assignee_issues[assignee]["score"] += score
    # sort by the score
    return dict(
        sorted(
            assignee_issues.items(),
            key=lambda x: x[1]["score"],
            reverse=True,
        )
    )


# TODO maybe use this one day for adding PR reviews to the leaderboard
def by_reviewer(issues):
    issues_by_approver = {}
    for issue in issues:
        for attachment in issue["attachments"]["nodes"]:
            metadata = attachment["metadata"]
            if metadata.get("reviews"):
                for review in metadata["reviews"]:
                    if review["state"] == "approved":
                        author = review["reviewerLogin"]
                        if author not in issues_by_approver:
                            issues_by_approver[author] = []
                        issues_by_approver[author].append(issue)
    return dict(
        sorted(
            issues_by_approver.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )
    )


def get_stale_issues_by_assignee(issues, days=30):
    """Return issues not updated in `days` days, grouped by assignee."""
    stale_issues = {}
    for issue in issues:
        if not issue["assignee"]:
            continue
        assignee = issue["assignee"]["displayName"]
        if assignee not in stale_issues:
            stale_issues[assignee] = []
        last_updated = datetime.strptime(
            issue["updatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        if (datetime.utcnow() - last_updated).days > days:
            days_stale = (datetime.utcnow() - last_updated).days
            stale_issues[assignee].append(
                {
                    "title": issue["title"],
                    "url": issue["url"],
                    "daysStale": days_stale,
                    "priority": issue["priority"],
                    "platform": issue.get("platform"),
                }
            )
    return stale_issues


def by_platform(issues):
    platform_issues = {}
    for issue in issues:
        if not issue["platform"]:
            continue
        platform = issue["platform"]
        if platform not in platform_issues:
            platform_issues[platform] = []
        platform_issues[platform].append(issue)
    return dict(
        sorted(
            platform_issues.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )
    )


def get_time_data(issues):
    lead_times = []
    queue_times = []
    work_times = []
    for issue in issues:
        completed_at = datetime.strptime(
            issue["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        created_at = datetime.strptime(
            issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        lead_time = (completed_at - created_at).days
        lead_times.append(lead_time)
        if issue["startedAt"]:
            started_at = datetime.strptime(
                issue["startedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            queue_time = (started_at - created_at).days
            queue_times.append(queue_time)
            work_time = (completed_at - started_at).days
            work_times.append(work_time)
    data = {
        "lead": {
            "avg": int(sum(lead_times) / len(lead_times)),
            "p95": int(sorted(lead_times)[int(len(lead_times) * 0.95)]),
        },
        "queue": {
            "avg": int(sum(queue_times) / len(queue_times)),
            "p95": int(sorted(queue_times)[int(len(queue_times) * 0.95)]),
        },
        "work": {
            "avg": int(sum(work_times) / len(work_times)),
            "p95": int(sorted(work_times)[int(len(work_times) * 0.95)]),
        },
    }
    return data


def get_user_id(username):
    """Return a Linear user ID for the given username."""

    query = gql(
        """
        query ($name: String!) {
          users(filter: { name: { eq: $name } }) {
            nodes { id }
          }
        }
        """
    )
    data = client.execute(query, variable_values={"name": username})
    users = data.get("users", {}).get("nodes", [])
    return users[0]["id"] if users else None


def get_open_issues_for_person(user_id):
    """Return open issues assigned to a user across all projects by Linear ID."""

    query = gql(
        """
        query OpenIssues($assignee: ID!, $cursor: String) {
          issues(
            first: 50
            after: $cursor
            filter: {
              assignee: { id: { eq: $assignee } }
              state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }
            }
            orderBy: createdAt
          ) {
            nodes {
              id
              title
              url
              createdAt
              project { name }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
    )

    cursor = None
    issues = []
    while True:
        params = {"assignee": user_id, "cursor": cursor}
        data = client.execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]

    for issue in issues:
        issue["daysOpen"] = (
            datetime.utcnow()
            - datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        ).days
    return issues


def get_completed_issues_for_person(user_id, days=30):
    """Return completed issues for a user over the last `days` days by ID."""

    query = gql(
        """
        query CompletedIssues($assignee: ID!, $days: DateTimeOrDuration, $cursor: String) {
          issues(
            first: 50
            after: $cursor
            filter: {
              assignee: { id: { eq: $assignee } }
              state: { name: { in: ["Done"] } }
              completedAt: { gt: $days }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              url
              project { name }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
    )

    cursor = None
    issues = []
    while True:
        params = {
            "assignee": user_id,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = client.execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]
    return issues


def by_project(issues):
    """Group issues by project name."""
    project_issues = {}
    for issue in issues:
        project = (
            issue.get("project", {}).get("name") if issue.get("project") else "No Project"
        )
        if project not in project_issues:
            project_issues[project] = []
        project_issues[project].append(issue)
    return project_issues
