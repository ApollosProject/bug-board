import os

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


headers = {"Authorization": os.getenv("LINEAR_API_KEY")}
transport = AIOHTTPTransport(url="https://api.linear.app/graphql", headers=headers)
client = Client(transport=transport, fetch_schema_from_transport=True)


def get_priority_issues():

    query = gql(
        """
        query Issues {
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
