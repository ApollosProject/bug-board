from gql import gql

from config import get_linear_team_key

from .client import _execute


def _completed_issue_assignee_names(issue_nodes: list[dict]) -> list[str]:
    completed_issue_assignees = {
        issue["assignee"]["displayName"]
        for issue in issue_nodes
        if issue.get("assignee") and issue["assignee"].get("displayName")
    }
    return sorted(completed_issue_assignees)


def _get_completed_project_issue_assignees(project_id: str) -> list[str]:
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

    issue_nodes: list[dict] = []
    after = None
    while True:
        data = _execute(query, variable_values={"project_id": project_id, "after": after})
        issue_connection = data.get("issues", {}) or {}
        issue_nodes.extend(issue_connection.get("nodes", []) or [])
        page_info = issue_connection.get("pageInfo", {}) or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break
    return _completed_issue_assignee_names(issue_nodes)


def _is_completed_project(project: dict) -> bool:
    status_type = ((project.get("status") or {}).get("type") or "").strip().lower()
    return status_type == "completed"


def _normalize_project_participants(
    projects: list[dict], *, include_completed_issue_assignees: bool = False
) -> list[dict]:
    for project in projects:
        member_nodes = project.get("members", {}).get("nodes", [])
        project["members"] = [m["displayName"] for m in member_nodes if m.get("displayName")]

        if include_completed_issue_assignees:
            project_id = project.get("id")
            if project_id and _is_completed_project(project):
                project["completedIssueAssignees"] = _get_completed_project_issue_assignees(
                    project_id
                )
            else:
                issue_nodes = project.get("issues", {}).get("nodes", [])
                project["completedIssueAssignees"] = _completed_issue_assignee_names(issue_nodes)
        project.pop("issues", None)
    return projects


def get_projects(*, include_completed_issue_assignees: bool = False):
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
    return _normalize_project_participants(
        sorted_projects,
        include_completed_issue_assignees=include_completed_issue_assignees,
    )
