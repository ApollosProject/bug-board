from gql import gql

from config import get_linear_team_key

from .client import _execute

PROJECT_ID_FILTER_CHUNK_SIZE = 50


def get_completed_project_issue_assignees(project_id: str) -> list[str]:
    """Return sorted unique assignee display names for a project's completed issues."""
    return get_completed_project_issue_assignees_by_project([project_id]).get(project_id, [])


def get_completed_project_issue_assignees_by_project(
    project_ids: list[str],
) -> dict[str, list[str]]:
    """Return completed-issue assignees grouped by project id."""
    unique_project_ids = []
    seen_ids = set()
    for project_id in project_ids:
        if not project_id or project_id in seen_ids:
            continue
        seen_ids.add(project_id)
        unique_project_ids.append(project_id)
    if not unique_project_ids:
        return {}

    query = gql(
        """
        query CompletedProjectIssueAssignees($project_ids: [ID!], $after: String) {
          issues(
            first: 250
            after: $after
            filter: {
              project: { id: { in: $project_ids } }
              state: { type: { in: ["completed"] } }
            }
          ) {
            nodes {
              assignee {
                displayName
              }
              project {
                id
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

    assignees_by_project: dict[str, set[str]] = {
        project_id: set() for project_id in unique_project_ids
    }
    for index in range(0, len(unique_project_ids), PROJECT_ID_FILTER_CHUNK_SIZE):
        chunk = unique_project_ids[index : index + PROJECT_ID_FILTER_CHUNK_SIZE]
        after = None
        while True:
            data = _execute(query, variable_values={"project_ids": chunk, "after": after})
            issue_connection = data.get("issues", {}) or {}
            for issue in issue_connection.get("nodes", []) or []:
                project = issue.get("project") or {}
                issue_project_id = project.get("id")
                if issue_project_id not in assignees_by_project:
                    continue
                assignee = issue.get("assignee") or {}
                display_name = assignee.get("displayName")
                if display_name:
                    assignees_by_project[issue_project_id].add(display_name)
            page_info = issue_connection.get("pageInfo", {}) or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
            if not after:
                break

    return {project_id: sorted(assignees) for project_id, assignees in assignees_by_project.items()}


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
                  lastUpdate {
                    createdAt
                  }
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
