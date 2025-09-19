from datetime import datetime
from typing import Dict, Set

from config import load_config
from linear.issues import get_open_issues_in_projects
from linear.projects import get_projects


def _normalize(name: str) -> str:
    """Normalize a Linear display name or username for comparison."""
    return name.replace(".", " ").replace("-", " ").title()


def _name_to_slug_map(config: Dict) -> Dict[str, str]:
    """Build a mapping from normalized display/first names to person slug."""
    mapping: Dict[str, str] = {}
    for slug, info in config.get("people", {}).items():
        username = info.get("linear_username", slug)
        full = _normalize(username)
        mapping[full] = slug
        first = full.split()[0]
        mapping.setdefault(first, slug)
    return mapping


def _is_cycle_initiative_project(project: Dict, cycle_init: str | None) -> bool:
    """Return True if project is part of the configured cycle initiative."""
    if not cycle_init:
        return False
    nodes = project.get("initiatives", {}).get("nodes", []) or []
    return any((node.get("name") or "") == cycle_init for node in nodes)


def _is_active_today(project: Dict) -> bool:
    """Return True if today is within the project's start/target date window."""
    status = (project.get("status") or {}).get("name")
    if status in {"Completed", "Incomplete"}:
        return False
    start = project.get("startDate")
    target = project.get("targetDate")
    if not start:
        return False
    try:
        start_dt = datetime.fromisoformat(start).date()
    except Exception:
        return False
    today = datetime.utcnow().date()
    if start_dt > today:
        return False

    if target:
        try:
            target_dt = datetime.fromisoformat(target).date()
        except Exception:
            # If target can't be parsed, assume work is ongoing as long as it has
            # started.
            return True
        if today <= target_dt:
            return True
        # The target date has passed but the project is still marked active.
        # Treat it as ongoing so the assignees remain off the support rotation
        # until the project is completed.
        return True

    return True


def get_support_slugs() -> Set[str]:
    """
    Compute the set of people slugs who are on support today.

    Anyone not assigned to an initiative project active today (lead or member)
    is considered on support. If assigned but today's date is outside the
    project's start/target window, they are on support.
    """
    config = load_config()
    projects = get_projects()
    name_to_slug = _name_to_slug_map(config)
    cycle_init = config.get("cycle_initiative")

    assigned_active_slugs: Set[str] = set()
    for project in projects:
        # Only consider projects that belong to the configured cycle initiative
        if not _is_cycle_initiative_project(project, cycle_init):
            continue
        if not _is_active_today(project):
            continue
        participants = []
        lead = (project.get("lead") or {}).get("displayName")
        if lead:
            participants.append(lead)
        participants.extend(project.get("members", []))
        for name in participants:
            slug = name_to_slug.get(_normalize(name)) or name_to_slug.get(
                _normalize(name).split()[0]
            )
            if slug:
                assigned_active_slugs.add(slug)

    people_cfg = config.get("people", {})
    all_people_slugs: Set[str] = set(people_cfg.keys())
    available_slugs: Set[str] = {
        slug
        for slug, info in people_cfg.items()
        if info.get("available_for_support", False)
    }

    # Additional filter: exclude anyone currently assigned to issues
    # in an Onboarding Churches project and who is also a member of that project.
    onboarding_projects = [
        p
        for p in projects
        if any(
            (node.get("name") or "") == "Onboarding Churches"
            for node in (p.get("initiatives", {}) or {}).get("nodes", []) or []
        )
    ]
    onboarding_project_names = {p.get("name") for p in onboarding_projects}
    # Build membership map for onboarding projects
    onboarding_members_by_project: Dict[str, Set[str]] = {}
    for p in onboarding_projects:
        members: Set[str] = set(p.get("members", []) or [])
        lead_name = (p.get("lead") or {}).get("displayName")
        if lead_name:
            members.add(lead_name)
        onboarding_members_by_project[p.get("name")] = {_normalize(n) for n in members}

    if onboarding_project_names:
        issues = get_open_issues_in_projects(onboarding_project_names)
    else:
        issues = []
    assigned_onboarding_slugs: Set[str] = set()
    for issue in issues:
        assignee = (issue.get("assignee") or {}).get("displayName")
        project_name = (issue.get("project") or {}).get("name")
        if not assignee or not project_name:
            continue
        # Only exclude if assignee is also a member of that project
        normalized = _normalize(assignee)
        proj_members = onboarding_members_by_project.get(project_name, set())
        if normalized not in proj_members:
            continue
        slug = name_to_slug.get(normalized) or name_to_slug.get(normalized.split()[0])
        if slug:
            assigned_onboarding_slugs.add(slug)

    # On support = available AND not assigned to an active cycle project
    # AND not assigned to onboarding project issues while being project member
    return (
        (all_people_slugs & available_slugs)
        - assigned_active_slugs
        - assigned_onboarding_slugs
    )
