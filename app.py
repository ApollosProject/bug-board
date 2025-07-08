from flask import Flask, render_template, request, abort

from config import load_config
import re
from datetime import datetime

from linear import (
    by_assignee,
    by_platform,
    by_project,
    get_completed_issues,
    get_completed_issues_for_person,
    get_created_issues,
    get_open_issues,
    get_open_issues_for_person,
    get_time_data,
    get_projects,
)
from github import merged_prs_by_author, merged_prs_by_reviewer

app = Flask(__name__)


@app.template_filter('first_name')
def first_name_filter(name: str) -> str:
    parts = re.split(r'[.\-\s]+', name)
    if parts and parts[0]:
        return parts[0].title()
    return name.title()


@app.template_filter('mmdd')
def mmdd_filter(date_str: str) -> str:
    """Format an ISO date string as MM/DD."""
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str).date()
        return dt.strftime("%m/%d")
    except ValueError:
        return date_str


# use a query string parameter for days on the index route
@app.route("/")
def index():
    days = request.args.get("days", default=30, type=int)
    created_priority_bugs = get_created_issues(2, "Bug", days)
    open_priority_bugs = get_open_issues(2, "Bug")
    completed_priority_bugs = get_completed_issues(2, "Bug", days)
    completed_bugs = get_completed_issues(5, "Bug", days)
    completed_new_features = get_completed_issues(
        5,
        "New Feature",
        days,
    )
    completed_technical_changes = get_completed_issues(
        5,
        "Technical Change",
        days,
    )
    open_work = (
        get_open_issues(5, "Bug")
        + get_open_issues(5, "New Feature")
        + get_open_issues(5, "Technical Change")
    )
    time_data = get_time_data(completed_priority_bugs)
    fixes_per_day = len(
        completed_bugs + completed_new_features + completed_technical_changes
    ) / days

    config_data = load_config()
    username_to_slug = {
        info.get("linear_username"): slug
        for slug, info in config_data.get("people", {}).items()
    }

    return render_template(
        "index.html",
        days=days,
        priority_issues=sorted(open_priority_bugs, key=lambda x: x["createdAt"]),
        issue_count=len(created_priority_bugs),
        priority_percentage=int(
            len(completed_priority_bugs)
            / len(
                completed_bugs
                + completed_new_features
                + completed_technical_changes
            )
            * 100
        ),
        completed_issues_by_assignee=by_assignee(
            completed_bugs
            + completed_new_features
            + completed_technical_changes
        ),
        all_issues=created_priority_bugs + open_priority_bugs,
        issues_by_platform=by_platform(created_priority_bugs),
        lead_time_data=time_data["lead"],
        queue_time_data=time_data["queue"],
        open_assigned_work=sorted(
            [
                issue
                for issue in open_work
                if issue["assignee"] is not None and issue["priority"] > 2
            ],
            key=lambda x: x["createdAt"],
            reverse=True,
        ),
        fixes_per_day=fixes_per_day,
        username_to_slug=username_to_slug,
    )


@app.route("/team/<slug>")
def team_slug(slug):
    """Display open and completed work for a team member."""
    days = request.args.get("days", default=30, type=int)
    config = load_config()
    person_cfg = config.get("people", {}).get(slug)
    if not person_cfg:
        abort(404)
    login = person_cfg.get("linear_username", slug)
    person_name = login.replace(".", " ").replace("-", " ").title()
    open_items = sorted(
        get_open_issues_for_person(login),
        key=lambda x: x["updatedAt"],
        reverse=True,
    )
    completed_items = sorted(
        get_completed_issues_for_person(login, days),
        key=lambda x: x["completedAt"],
        reverse=True,
    )

    # Group open and completed items by project
    open_by_project = by_project(open_items)
    completed_by_project = by_project(completed_items)

    for issues in open_by_project.values():
        issues.sort(key=lambda x: x["updatedAt"], reverse=True)
    for issues in completed_by_project.values():
        issues.sort(key=lambda x: x["completedAt"], reverse=True)

    # Determine current cycle initiative projects
    cycle_initiative = config.get("cycle_initiative")
    cycle_projects = get_projects()
    # attach start/target date info and compute days left
    for proj in cycle_projects:
        target = proj.get("targetDate")
        start = proj.get("startDate")
        days_left = None
        starts_in = None
        if target:
            try:
                target_dt = datetime.fromisoformat(target).date()
                days_left = (target_dt - datetime.utcnow().date()).days
            except ValueError:
                pass
        if start:
            try:
                start_dt = datetime.fromisoformat(start).date()
                starts_in = (start_dt - datetime.utcnow().date()).days
            except ValueError:
                pass
        proj["days_left"] = days_left
        proj["starts_in"] = starts_in
    projects_by_initiative = {}
    for project in cycle_projects:
        nodes = project.get("initiatives", {}).get("nodes", [])
        if nodes:
            for init in nodes:
                name = init.get("name") or "Unnamed Initiative"
                projects_by_initiative.setdefault(name, []).append(project)
        else:
            projects_by_initiative.setdefault("No Initiative", []).append(project)
    # Sort initiatives alphabetically
    projects_by_initiative = dict(sorted(projects_by_initiative.items(), key=lambda x: x[0]))
    current_projects = projects_by_initiative.get(cycle_initiative, []) if cycle_initiative else []
    current_names = [proj.get("name") for proj in current_projects]

    if person_cfg.get("on_call_support"):
        open_current_cycle = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj == "Customer Success"
        }
        open_other = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj != "Customer Success"
        }
    else:
        open_current_cycle = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj in current_names
        }
        open_other = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj not in current_names
        }

    work_by_platform = by_platform(open_items + completed_items)
    github_username = person_cfg.get("github_username")
    prs_merged = 0
    prs_reviewed = 0
    if github_username:
        prs_merged = len(merged_prs_by_author(days).get(github_username, []))
        prs_reviewed = len(merged_prs_by_reviewer(days).get(github_username, []))

    return render_template(
        "person.html",
        person_slug=slug,
        person_name=person_name,
        days=days,
        open_current_cycle=open_current_cycle,
        open_other=open_other,
        completed_by_project=completed_by_project,
        on_call_support=person_cfg.get("on_call_support"),
        work_by_platform=work_by_platform,
        prs_merged=prs_merged,
        prs_reviewed=prs_reviewed,
    )


@app.route("/team")
def team():
    config = load_config()

    def format_name(key):
        data = config["people"].get(key, {})
        name = data.get("linear_username", key)
        return name.replace(".", " ").replace("-", " ").title()

    platform_teams = {}
    for slug, info in config.get("platforms", {}).items():
        lead = info.get("lead")
        developers = [dev for dev in info.get("developers", []) if dev != lead]
        developers = sorted(developers, key=lambda d: format_name(d))
        members = [{"name": format_name(lead), "lead": True}] + [
            {"name": format_name(dev), "lead": False} for dev in developers
        ]
        platform_teams[slug] = members
    developers = sorted(
        [
            {"slug": slug, "name": format_name(slug)}
            for slug in config.get("people", {})
        ],
        key=lambda d: d["name"],
    )

    open_priority_bugs = get_open_issues(2, "Bug")
    bugs_by_assignee = by_assignee(open_priority_bugs)

    cycle_projects = get_projects()
    current_init = config.get("cycle_initiative")
    cycle_project_names = []
    project_url_map = {}
    for project in cycle_projects:
        nodes = project.get("initiatives", {}).get("nodes", [])
        if current_init:
            if any(init.get("name") == current_init for init in nodes):
                cycle_project_names.append(project.get("name"))
        else:
            cycle_project_names.append(project.get("name"))
        project_url_map[project.get("name")] = project.get("url")
    # attach start/target date info and compute days left
    for proj in cycle_projects:
        target = proj.get("targetDate")
        start = proj.get("startDate")
        days_left = None
        starts_in = None
        if target:
            try:
                target_dt = datetime.fromisoformat(target).date()
                days_left = (target_dt - datetime.utcnow().date()).days
            except ValueError:
                pass
        if start:
            try:
                start_dt = datetime.fromisoformat(start).date()
                starts_in = (start_dt - datetime.utcnow().date()).days
            except ValueError:
                pass
        proj["days_left"] = days_left
        proj["starts_in"] = starts_in

    # group cycle projects by initiatives
    projects_by_initiative = {}
    for project in cycle_projects:
        nodes = project.get("initiatives", {}).get("nodes", [])
        if nodes:
            for init in nodes:
                name = init.get("name") or "Unnamed Initiative"
                projects_by_initiative.setdefault(name, []).append(project)
        else:
            projects_by_initiative.setdefault("No Initiative", []).append(project)
    # sort initiatives alphabetically
    projects_by_initiative = dict(
        sorted(projects_by_initiative.items(), key=lambda x: x[0])
    )
    # filter to only the cycle initiative (from config.yml)
    if current_init:
        projects_by_initiative = {
            name: projects
            for name, projects in projects_by_initiative.items()
            if name == current_init
        }

    current_focus = []
    for dev in developers:
        login = config["people"].get(dev["slug"], {}).get("linear_username", dev["slug"])
        open_items = get_open_issues_for_person(login)
        by_proj = by_project(open_items)
        cycle_for_dev = [
            {"name": proj, "url": project_url_map.get(proj)}
            for proj in by_proj
            if proj in cycle_project_names
        ]
        support_raw = bugs_by_assignee.get(dev["name"], {}).get("issues", [])
        support_list = [{"title": i["title"], "url": i["url"]} for i in support_raw]
        current_focus.append(
            {
                "slug": dev["slug"],
                "name": dev["name"],
                "cycle": cycle_for_dev,
                "support": support_list,
            }
        )

    return render_template(
        "team.html",
        platform_teams=platform_teams,
        cycle_projects_by_initiative=projects_by_initiative,
        current_focus=current_focus,
    )


if __name__ == "__main__":
    app.run(debug=True)
