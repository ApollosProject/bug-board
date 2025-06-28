from flask import Flask, render_template, request, abort
import yaml
import re

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

app = Flask(__name__)

@app.template_filter('first_name')
def first_name_filter(name: str) -> str:
    parts = re.split(r'[.\-\s]+', name)
    if parts and parts[0]:
        return parts[0].title()
    return name.title()


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
        completed_bugs_by_assignee=by_assignee(completed_bugs),
        completed_features_by_assignee=by_assignee(
            completed_new_features + completed_technical_changes
        ),
        completed_issues_by_assignee=by_assignee(
            completed_bugs
            + completed_new_features
            + completed_technical_changes
        ),
        completed_issues=(
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
    )


@app.route("/team/<slug>")
def team_slug(slug):
    """Display open and completed work for a team member."""
    days = request.args.get("days", default=30, type=int)
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    person_cfg = config.get("people", {}).get(slug)
    if not person_cfg:
        abort(404)
    login = person_cfg.get("linear_username", slug)
    person_name = login.replace(".", " ").replace("-", " ").title()
    open_items = get_open_issues_for_person(login)
    completed_items = get_completed_issues_for_person(login, days)

    # Group open and completed items by project
    open_by_project = by_project(open_items)
    completed_by_project = by_project(completed_items)

    # Determine current cycle initiative projects
    cycle_initiative = config.get("cycle_initiative")
    cycle_projects = get_projects()
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

    return render_template(
        "person.html",
        person_slug=slug,
        person_name=person_name,
        days=days,
        open_current_cycle=open_current_cycle,
        open_other=open_other,
        completed_by_project=completed_by_project,
    )


@app.route("/team")
def team():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)

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
    on_call_support = [
        format_name(name)
        for name, person in config.get("people", {}).items()
        if person.get("on_call_support")
    ]
    cycle_projects = get_projects()

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
    current_init = config.get("cycle_initiative")
    if current_init:
        projects_by_initiative = {
            name: projects
            for name, projects in projects_by_initiative.items()
            if name == current_init
        }

    return render_template(
        "team.html",
        platform_teams=platform_teams,
        developers=developers,
        cycle_projects_by_initiative=projects_by_initiative,
        on_call_support=on_call_support,
    )


if __name__ == "__main__":
    app.run(debug=True)
