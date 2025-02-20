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
                name
              }
              url
              labels {
                nodes {
                  name
                }
              }
              createdAt
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
            datetime.now()
            - datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        ).days
    return issues


def get_completed_issues(priority, label):

    params = {"priority": priority, "label": label}
    query = gql(
        """
        query CompletedIssues ($priority: Float, $label: String) {
          issues(
            filter: {
              labels: { name: { eq: $label } }
              project: { name: { eq: "Customer Success" } }
              priority: { lte: $priority }
              state: { name: { in: ["Done"] } }
              completedAt:{lt:"P1M"}
            }
            orderBy: updatedAt
          ) {
            nodes {
              id
              title
              assignee {
                name
              }
              url
              labels {
                nodes {
                  name
                }
              }
              completedAt
              createdAt
              attachments {
                nodes {
                  metadata
                }
              }
            }
          }
        }
        """
    )

    # Execute the query on the transport
    data = client.execute(query, variable_values=params)
    issues = data["issues"]["nodes"]
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


def get_lead_time_data(issues):
    lead_times = []
    for issue in issues:
        completed_at = datetime.strptime(issue["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_at = datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        lead_time = (completed_at - created_at).days
        lead_times.append(lead_time)
    data = {
        "avg": int(sum(lead_times) / len(lead_times)),
        "p95": int(sorted(lead_times)[int(len(lead_times) * 0.95)]),
    }
    return data
