import os
from datetime import datetime

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


headers = {"Authorization": os.getenv("LINEAR_API_KEY")}
transport = AIOHTTPTransport(url="https://api.linear.app/graphql", headers=headers)
client = Client(transport=transport, fetch_schema_from_transport=True)


def get_open_priority_issues():

    query = gql(
        """
        query PriorityIssues {
          issues(
            filter: {
              labels: { name: { eq: "Bug" } }
              project: { name: { eq: "Customer Success" } }
              priority: { lte: 2 }
              state: { name: { nin: ["Done", "Canceled"] } }
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
            }
          }
        }
    """
    )

    # Execute the query on the transport
    return client.execute(query)


def get_completed_priority_issues():

    query = gql(
        """
        query CompletedIssues {
          issues(
            filter: {
              labels: { name: { eq: "Bug" } }
              project: { name: { eq: "Customer Success" } }
              priority: { lte: 2 }
              state: { name: { in: ["Done", "Canceled"] } }
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
            }
          }
        }
        """
    )

    # Execute the query on the transport
    return client.execute(query)


def get_lead_time_data(issues):
    lead_times = []
    for issue in issues["issues"]["nodes"]:
        completed_at = datetime.strptime(issue["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_at = datetime.strptime(issue["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        lead_time = (completed_at - created_at).days
        lead_times.append(lead_time)
    data = {
        "avg": int(sum(lead_times) / len(lead_times)),
        "p95": int(sorted(lead_times)[int(len(lead_times) * 0.95)]),
    }
    return data
