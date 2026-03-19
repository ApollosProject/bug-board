from gql import gql

from config import get_linear_team_key
from .client import _execute


def _normalize_project_members(projects: list[dict]) -> list[dict]:
    for project in projects:
        nodes = project.get("members", {}).get("nodes", [])
        project["members"] = [m["displayName"] for m in nodes if m.get("displayName")]
    return projects


def get_projects():
    """Return all Linear projects under the Apollos team, ordered by name."""
    team_key = get_linear_team_key()
    query = gql(
        """
        query Projects($team_key: String!, $after: String) {
          teams(filter: { key: { eq: $team_key } }, first: 1) {
            nodes {
              projects(first: 50, after: $after) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  id
                  name
                  url
                  health
                  status {
                    name
                  }
                  completedAt
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
    projects = []
    after = None
    while True:
        data = _execute(query, variable_values={"team_key": team_key, "after": after})
        teams = data.get("teams", {}).get("nodes", []) or []
        if not teams:
            return []
        project_connection = teams[0].get("projects", {}) or {}
        projects.extend(project_connection.get("nodes", []) or [])
        page_info = project_connection.get("pageInfo", {}) or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break
    sorted_projects = sorted(projects, key=lambda project: project.get("name", ""))
    return _normalize_project_members(sorted_projects)


def update_project_description(project_id: str, description: str) -> None:
    """Update a Linear project's description."""
    mutation = gql(
        """
        mutation ProjectUpdate($id: String!, $input: ProjectUpdateInput!) {
          projectUpdate(id: $id, input: $input) {
            success
          }
        }
        """
    )
    _execute(
        mutation,
        variable_values={"id": project_id, "input": {"description": description}},
    )
