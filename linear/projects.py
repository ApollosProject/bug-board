from gql import gql

from config import get_linear_team_key

from .client import _execute


def get_completed_project_issue_assignees(project_id: str) -> list[str]:
    """Return sorted unique assignee display names for a project's completed issues."""
    query = gql(
        """
        query CompletedProjectIssueAssignees($project_id: String!, $after: String) {
          issues(
            first: 50
            after: $after
            filter: {
              project: { id: { eq: $project_id } }
              state: { type: { in: ["completed"] } }
            }
          ) {
            nodes {
              assignee {
                displayName
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

    assignees: set[str] = set()
    after = None
    while True:
        data = _execute(query, variable_values={"project_id": project_id, "after": after})
        issue_connection = data.get("issues", {}) or {}
        for issue in issue_connection.get("nodes", []) or []:
            assignee = issue.get("assignee") or {}
            display_name = assignee.get("displayName")
            if display_name:
                assignees.add(display_name)
        page_info = issue_connection.get("pageInfo", {}) or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break
    return sorted(assignees)


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
                    type
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
    projects: list[dict] = []
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
