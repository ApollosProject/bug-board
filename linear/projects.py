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
