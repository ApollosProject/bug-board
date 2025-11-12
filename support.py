from datetime import datetime
from typing import Dict, Set

from config import load_config
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
        if info.get("team") == "apollos_engineering"
    }

    # On support = available AND not assigned to an active cycle project
    return (all_people_slugs & available_slugs) - assigned_active_slugs
