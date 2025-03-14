import os
from datetime import datetime

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


headers = {"Authorization": os.getenv("LINEAR_API_KEY")}
transport = AIOHTTPTransport(url="https://api.linear.app/graphql", headers=headers)
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
            tag["name"] for tag in issue["labels"]["nodes"] if tag["name"] != label
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
        query CompletedIssues ($priority: Float, $label: String, $days: DateTimeOrDuration, $cursor: String) {
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
                email
              }
              url
              labels {
                nodes {
                  name
                }
              }
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
        query CreatedIssues ($priority: Float, $label: String, $days: DateTimeOrDuration, $cursor: String) {
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
            tag["name"] for tag in issue["labels"]["nodes"] if tag["name"] != label
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
            assignee_issues[assignee] = []
        assignee_issues[assignee].append(issue)
    # sort by the number of issues
    return dict(sorted(assignee_issues.items(), key=lambda x: len(x[1]), reverse=True))


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
        sorted(issues_by_approver.items(), key=lambda x: len(x[1]), reverse=True)
    )


def get_time_data(issues):
    lead_times = []
    queue_times = []
    for issue in issues:
        completed_at = datetime.strptime(issue["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_at = datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        lead_time = (completed_at - created_at).days
        lead_times.append(lead_time)
        if issue["startedAt"]:
            started_at = datetime.strptime(issue["startedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
            queue_time = (started_at - created_at).days
            queue_times.append(queue_time)
    data = {
        "lead": {
            "avg": int(sum(lead_times) / len(lead_times)),
            "p95": int(sorted(lead_times)[int(len(lead_times) * 0.95)]),
        },
        "queue": {
            "avg": int(sum(queue_times) / len(queue_times)),
            "p95": int(sorted(queue_times)[int(len(queue_times) * 0.95)]),
        },
    }
    return data
