from datetime import datetime

from gql import gql

from config import get_linear_team_key, get_platforms
from constants import PRIORITY_TO_SCORE
from .client import _compute_assignee_time_to_fix, _execute


def get_open_issues(priority, label):
    team_key = get_linear_team_key()
    params = {"priority": priority, "label": label, "team_key": team_key}
    query = gql(
        """
        query PriorityIssues ($priority: Float, $label: String, $team_key: String!) {
          issues(
            filter: {
              team: { key: { eq: $team_key } }
              labels: { name: { eq: $label } }
              priority: { lte: $priority, gte: 1 }
              state: { name: { nin: ["Done", "Canceled", "Duplicate"] } }
              project: { null: true }
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
    """,
    )

    data = _execute(query, variable_values=params)
    issues = [
        issue for issue in data["issues"]["nodes"] if issue.get("priority", 0) > 0
    ]
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
    team_key = get_linear_team_key()
    query = gql(
        """
        query CompletedIssues (
            $priority: Float,
            $label: String,
            $team_key: String!,
            $days: DateTimeOrDuration,
            $cursor: String
        ) {
          issues(
            first: 50
            after: $cursor
        filter: {
              team: { key: { eq: $team_key } }
              labels: { name: { eq: $label } }
              priority: { lte: $priority, gte: 1 }
              state: { name: { in: ["Done"] } }
              completedAt: { gt: $days }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              project {
                name
              }
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
        """,
    )

    cursor = None
    issues = []
    while True:
        params = {
            "priority": priority,
            "label": label,
            "team_key": team_key,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = _execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]

    issues = [issue for issue in issues if issue.get("priority", 0) > 0]

    for issue in issues:
        proj = issue.get("project", {}).get("name") if issue.get("project") else None
        issue["project"] = proj
    return issues


def get_completed_issues_summary(priority, label, days=30):
    team_key = get_linear_team_key()
    query = gql(
        """
        query CompletedIssuesSummary (
            $priority: Float,
            $label: String,
            $team_key: String!,
            $days: DateTimeOrDuration,
            $cursor: String
        ) {
          issues(
            first: 50
            after: $cursor
            filter: {
              team: { key: { eq: $team_key } }
              labels: { name: { eq: $label } }
              priority: { lte: $priority, gte: 1 }
              state: { name: { in: ["Done"] } }
              completedAt: { gt: $days }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              project {
                name
              }
              assignee {
                name
                displayName
              }
              priority
              completedAt
              createdAt
              startedAt
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """,
    )

    cursor = None
    issues = []
    while True:
        params = {
            "priority": priority,
            "label": label,
            "team_key": team_key,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = _execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]

    issues = [issue for issue in issues if issue.get("priority", 0) > 0]

    for issue in issues:
        proj = issue.get("project", {}).get("name") if issue.get("project") else None
        issue["project"] = proj
    return issues


def get_created_issues(priority, label, days=30):
    team_key = get_linear_team_key()
    query = gql(
        """
        query CreatedIssues (
            $priority: Float,
            $label: String,
            $team_key: String!,
            $days: DateTimeOrDuration,
            $cursor: String
        ) {
            issues(
                first: 50
                after: $cursor
                filter: {
                    team: { key: { eq: $team_key } }
                    labels: { name: { eq: $label } }
                    priority: { lte: $priority, gte: 1 }
                    createdAt:{gt: $days}
                    project: { null: true }
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
                    priority
                }
                pageInfo {
                  hasNextPage
                  endCursor
                }
            }
        }
        """,
    )

    cursor = None
    issues = []
    while True:
        params = {
            "priority": priority,
            "label": label,
            "team_key": team_key,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = _execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]
    issues = [issue for issue in issues if issue.get("priority", 0) > 0]
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
        score = PRIORITY_TO_SCORE.get(issue["priority"], 1)
        assignee_issues[assignee]["score"] += score
    return dict(
        sorted(
            assignee_issues.items(),
            key=lambda x: x[1]["score"],
            reverse=True,
        )
    )


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


def _parse_linear_datetime(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def get_time_data(issues):
    def summarize_times(values):
        if not values:
            return {"avg": 0, "p95": 0}
        values_sorted = sorted(values)
        count = len(values_sorted)
        if count == 0:
            return {"avg": 0, "p95": 0}
        p95_index = min(int(count * 0.95), count - 1)
        return {
            "avg": int(sum(values_sorted) / count),
            "p95": int(values_sorted[p95_index]),
        }

    lead_times = []
    queue_times = []
    work_times = []
    for issue in issues:
        completed_at = _parse_linear_datetime(issue.get("completedAt"))
        created_at = _parse_linear_datetime(issue.get("createdAt"))
        if not completed_at or not created_at:
            continue
        try:
            lead_time = (completed_at - created_at).days
        except ZeroDivisionError:
            continue
        lead_times.append(lead_time)
        started_at = _parse_linear_datetime(issue.get("startedAt"))
        if started_at:
            queue_time = (started_at - created_at).days
            queue_times.append(queue_time)
            work_time = (completed_at - started_at).days
            work_times.append(work_time)
    data = {
        "lead": summarize_times(lead_times),
        "queue": summarize_times(queue_times),
        "work": summarize_times(work_times),
    }
    return data


def get_open_issues_for_person(login: str):
    """Return open issues assigned to a given Linear username across all projects."""
    team_key = get_linear_team_key()
    query = gql(
        """
        query OpenIssues($login: String!, $team_key: String!, $cursor: String) {
          issues(
            first: 50
            after: $cursor
            filter: {
              team: { key: { eq: $team_key } }
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
        """,
    )

    cursor = None
    issues = []
    while True:
        params = {"login": login, "team_key": team_key, "cursor": cursor}
        data = _execute(query, variable_values=params)
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


def get_open_issues_in_projects(project_names):
    """Return open issues across the team limited to specified Linear project names.

    project_names: iterable of project name strings to include.
    """

    # Ensure we work with a list for GraphQL variables
    project_names = list(project_names)
    team_key = get_linear_team_key()

    query = gql(
        """
        query OpenIssuesInProjects(
          $projectNames: [String!],
          $team_key: String!,
          $cursor: String
        ) {
          issues(
            first: 50
            after: $cursor
            filter: {
              team: { key: { eq: $team_key } }
              state: { type: { nin: [\"completed\", \"canceled\"] } }
              project: { name: { in: $projectNames } }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              identifier
              title
              url
              updatedAt
              createdAt
              dueDate
              slaType
              slaStartedAt
              slaMediumRiskAt
              slaHighRiskAt
              slaBreachesAt
              state { name type }
              assignee { displayName }
              project { name }
              parent { id }
              children(first: 50) {
                nodes {
                  id
                  identifier
                  title
                  url
                  state { name type }
                  assignee { displayName }
                  dueDate
                  slaType
                  slaStartedAt
                  slaMediumRiskAt
                  slaHighRiskAt
                  slaBreachesAt
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """,
    )

    cursor = None
    issues = []
    while True:
        params = {
            "projectNames": project_names,
            "team_key": team_key,
            "cursor": cursor,
        }
        data = _execute(query, variable_values=params)
        issues += data["issues"]["nodes"]
        if not data["issues"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["issues"]["pageInfo"]["endCursor"]
    return issues


def get_recently_resolved_parent_issues_in_project(project_name: str, limit: int = 50):
    """Return recently resolved *parent* issues for a project.

    This is used for computing 'days since last open issue' when there are
    currently no open issues. We consider Done/Canceled/Duplicate resolved.
    """
    team_key = get_linear_team_key()
    query = gql(
        """
        query ResolvedIssuesInProject(
          $projectName: String!,
          $team_key: String!,
          $cursor: String
        ) {
          issues(
            first: 50
            after: $cursor
            filter: {
              team: { key: { eq: $team_key } }
              project: { name: { eq: $projectName } }
              parent: { null: true }
              state: { name: { in: ["Done", "Canceled", "Duplicate"] } }
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              identifier
              title
              url
              updatedAt
              completedAt
              canceledAt
              state { name }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """,
    )

    cursor = None
    issues = []
    while True:
        data = _execute(
            query,
            variable_values={
                "projectName": project_name,
                "team_key": team_key,
                "cursor": cursor,
            },
        )
        payload = data["issues"]
        issues.extend(payload["nodes"])
        if len(issues) >= limit:
            return issues[:limit]
        if not payload["pageInfo"]["hasNextPage"]:
            break
        cursor = payload["pageInfo"]["endCursor"]
    return issues


def get_completed_issues_for_person(login: str, days=30):
    """Return completed issues for a user over the last `days` days, filtered by Linear username."""
    team_key = get_linear_team_key()
    query = gql(
        """
        query CompletedIssues(
          $login: String!,
          $team_key: String!,
          $days: DateTimeOrDuration,
          $cursor: String
        ) {
          issues(
            first: 50
            after: $cursor
            filter: {
              team: { key: { eq: $team_key } }
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
        """,
    )

    cursor = None
    issues = []
    while True:
        params = {
            "login": login,
            "team_key": team_key,
            "days": f"-P{days}D",
            "cursor": cursor,
        }
        data = _execute(query, variable_values=params)
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
