import os
from datetime import datetime
import threading

from dotenv import load_dotenv
from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


def _compute_assignee_time_to_fix(issue, assignee_name):
    """Return days between last assignment to `assignee_name` and completion."""
    completed_at = issue.get("completedAt")
    if not completed_at:
        return None
    try:
        completed_dt = datetime.strptime(completed_at, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return None

    history = issue.get("history", {}).get("edges", [])
    last_assigned = None
    for edge in history:
        node = edge.get("node", {})
        to_assignee = node.get("toAssignee")
        if not to_assignee or to_assignee.get("displayName") != assignee_name:
            continue
        updated = node.get("updatedAt")
        if not updated:
            continue
        try:
            updated_dt = datetime.strptime(updated, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            continue
        if updated_dt > completed_dt:
            # Ignore assignment changes that happened after the issue was closed.
            continue
        if last_assigned is None or updated_dt > last_assigned:
            last_assigned = updated_dt

    if last_assigned is None:
        return None

    delta_days = (completed_dt - last_assigned).days
    return max(delta_days, 0)


_thread_local = threading.local()


def _get_client():
    client = getattr(_thread_local, "client", None)
    if client is None:
        headers = {"Authorization": os.getenv("LINEAR_API_KEY")}
        transport = AIOHTTPTransport(
            url="https://api.linear.app/graphql",
            headers=headers,
        )
        client = Client(transport=transport, fetch_schema_from_transport=False)
        _thread_local.client = client
    return client


def _execute(query, variable_values=None):
    client = _get_client()
    if variable_values is None:
        return client.execute(query)
    return client.execute(query, variable_values=variable_values)
