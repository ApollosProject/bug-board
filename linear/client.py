import os
from datetime import datetime
from functools import lru_cache

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport



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

