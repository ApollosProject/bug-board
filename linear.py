import os
from datetime import datetime
from functools import lru_cache

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from config import get_platforms

load_dotenv()


def _compute_assignee_time_to_fix(issue, assignee_name):
    """Return days between last assignment to `assignee_name` and completion."""
    history = issue.get("history", {}).get("edges", [])
    last_assigned = None
    for edge in history:
        node = edge.get("node", {})
        to_assignee = node.get("toAssignee")
        if not to_assignee:
            continue
        if to_assignee.get("displayName") == assignee_name:
            updated = node.get("updatedAt")
            if updated and (last_assigned is None or updated > last_assigned):
                last_assigned = updated
    if not last_assigned:
        return None
    completed_at = issue.get("completedAt")
    if not completed_at:
        return None
    try:
        completed_dt = datetime.strptime(completed_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        assigned_dt = datetime.strptime(last_assigned, "%Y-%m-%dT%H:%M:%S.%fZ")
        return (completed_dt - assigned_dt).days
    except ValueError:
        return None


@lru_cache(maxsize=1)
def _get_client():
    headers = {"Authorization": os.getenv("LINEAR_API_KEY")}
    transport = AIOHTTPTransport(
        url="https://api.linear.app/graphql",
        headers=headers,
    )
    return Client(transport=transport, fetch_schema_from_transport=True)


def get_open_issues(priority, label):

    params = {"priority": priority, "label": label}
    query = gql(
        """
        query PriorityIssues ($priority: Float, $label: String) {
          issues(
            filter: {
              labels: { name: { eq: $label } }
              priority: { lte: $priority }
              state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }
              or: [
                { project: { name: { eq: "Customer Success" } } },
                { project: null }
              ]
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
    data = _get_client().execute(query, variable_values=params)
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
              priority: { lte: $priority }
              state: { name: { in: ["Done"] } }
              completedAt:{gt: $days}
              or: [
                { project: { name: { eq: "Customer Success" } } },
                { project: null }
              ]
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              description
              comments {
                nodes {
                  body
                }
              }
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
              history {
                edges {
                  node {
                    toAssignee {
                      displayName
                    }
                    updatedAt
                  }
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
        data = _get_client().execute(query, variable_values=params)
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
                    priority: { lte: $priority }
                    createdAt:{gt: $days}
                    or: [
                        { project: { name: { eq: "Customer Success" } } },
                        { project: null }
                    ]
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
        data = _get_client().execute(query, variable_values=params)
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
        assignee = issue.get("assignee", {}).get("displayName") if issue.get("assignee") else None
        if assignee:
            issue["assignee_time_to_fix"] = _compute_assignee_time_to_fix(issue, assignee)
        else:
            issue["assignee_time_to_fix"] = None
    return issues


def by_assignee(issues):
    assignee_issues = {}
    for issue in issues:
        if not issue["assignee"]:
            continue
        assignee = issue["assignee"]["displayName"]
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
        last_updated = datetime.strptime(issue["updatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
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
        completed_at = datetime.strptime(issue["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_at = datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        lead_time = (completed_at - created_at).days
        lead_times.append(lead_time)
        if issue["startedAt"]:
            started_at = datetime.strptime(issue["startedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
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


def get_open_issues_for_person(login: str):
    """Return open issues assigned to a given Linear username across all projects."""

    query = gql(
        """
        query OpenIssues($login: String!, $cursor: String) {
          issues(
            first: 50
            after: $cursor
            filter: {
              assignee: { displayName: { eq: $login } }
              state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              url
              updatedAt
              createdAt
              project { name }
              labels {
                nodes {
                  name
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
        params = {"login": login, "cursor": cursor}
        data = _get_client().execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]

    for issue in issues:
        platforms = [
            tag["name"]
            for tag in issue.get("labels", {}).get("nodes", [])
            if tag["name"].lower().replace(" ", "-") in get_platforms()
        ]
        issue["platform"] = platforms[0] if platforms else None
        issue["daysOpen"] = (
            datetime.utcnow()
            - datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        ).days
        issue["daysUpdated"] = (
            datetime.utcnow()
            - datetime.strptime(issue["updatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        ).days
    return issues


def get_completed_issues_for_person(login: str, days=30):
    """Return completed issues for a user over the last `days` days, filtered by Linear username."""

    query = gql(
        """
        query CompletedIssues($login: String!, $days: DateTimeOrDuration, $cursor: String) {
          issues(
            first: 50
            after: $cursor
            filter: {
              assignee: { displayName: { eq: $login } }
              state: { name: { in: ["Done"] } }
              completedAt: { gt: $days }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              url
              completedAt
              project { name }
              labels {
                nodes {
                  name
                }
              }
              priority
              history {
                edges {
                  node {
                    toAssignee {
                      displayName
                    }
                    updatedAt
                  }
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
        params = {"login": login, "days": f"-P{days}D", "cursor": cursor}
        data = _get_client().execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]
    for issue in issues:
        platforms = [
            tag["name"]
            for tag in issue.get("labels", {}).get("nodes", [])
            if tag["name"].lower().replace(" ", "-") in get_platforms()
        ]
        issue["platform"] = platforms[0] if platforms else None
        issue["daysCompleted"] = (
            datetime.utcnow()
            - datetime.strptime(issue["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        ).days
        issue["assignee_time_to_fix"] = _compute_assignee_time_to_fix(issue, login)
    return issues


def by_project(issues):
    """Group issues by project name."""
    project_issues = {}
    for issue in issues:
        if issue.get("project"):
            project = issue["project"].get("name")
        else:
            project = "No Project"
        if project not in project_issues:
            project_issues[project] = []
        project_issues[project].append(issue)
    return project_issues


def get_projects():
    """Return all Linear projects under the Apollos team, ordered by name."""
    query = gql(
        """
        query {
          teams(filter: { name: { eq: "Apollos" } }, first: 1) {
            nodes {
              projects(first: 50) {
                nodes {
                  id
                  name
                  url
                  health
                  startDate
                  targetDate
                  lead {
                    displayName
                  }
                  initiatives(first: 50) {
                    nodes {
                      id
                      name
                    }
                  }
                  members(first: 50) {
                    nodes {
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
        """
    )
    data = _get_client().execute(query)
    teams = data.get("teams", {}).get("nodes", []) or []
    if not teams:
        return []
    projects = teams[0].get("projects", {}).get("nodes", []) or []
    sorted_projects = sorted(projects, key=lambda project: project.get("name", ""))
    # flatten built-in members
    for project in sorted_projects:
        nodes = project.get("members", {}).get("nodes", [])
        project["members"] = [m["displayName"] for m in nodes if m.get("displayName")]
    return sorted_projects
