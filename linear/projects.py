from gql import gql

from config import get_linear_team_key
from .client import _execute


def get_projects():
    """Return all Linear projects under the Apollos team, ordered by name."""
    team_key = get_linear_team_key()
    query = gql(
        """
        query Projects($team_key: String!) {
          teams(filter: { key: { eq: $team_key } }, first: 1) {
            nodes {
              projects(first: 50) {
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
    data = _execute(query, variable_values={"team_key": team_key})
    teams = data.get("teams", {}).get("nodes", []) or []
    if not teams:
        return []
    projects = teams[0].get("projects", {}).get("nodes", []) or []
    sorted_projects = sorted(projects, key=lambda project: project.get("name", ""))
    for project in sorted_projects:
        nodes = project.get("members", {}).get("nodes", [])
        project["members"] = [m["displayName"] for m in nodes if m.get("displayName")]
    return sorted_projects


def get_project_by_name(name: str) -> dict | None:
    """Return a Linear project by exact name match, including description."""
    team_key = get_linear_team_key()
    query = gql(
        """
        query Projects($team_key: String!) {
          teams(filter: { key: { eq: $team_key } }, first: 1) {
            nodes {
              projects(first: 250) {
                nodes {
                  id
                  name
                  url
                  description
                }
              }
            }
          }
        }
        """
    )
    data = _execute(query, variable_values={"team_key": team_key})
    teams = data.get("teams", {}).get("nodes", []) or []
    if not teams:
        return None
    projects = teams[0].get("projects", {}).get("nodes", []) or []
    for project in projects:
        if project.get("name") == name:
            return project
    return None


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
