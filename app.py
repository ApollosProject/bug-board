import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from flask import Flask, abort, render_template, request

from config import load_config
from constants import PRIORITY_TO_SCORE
from github import merged_prs_by_author, merged_prs_by_reviewer
from linear.issues import (
    by_assignee,
    by_platform,
    by_project,
    get_completed_issues,
    get_completed_issues_for_person,
    get_created_issues,
    get_open_issues,
    get_open_issues_for_person,
    get_time_data,
)
from linear.projects import get_projects
from support import get_support_slugs

app = Flask(__name__)


@app.template_filter("first_name")
def first_name_filter(name: str) -> str:
    parts = re.split(r"[.\-\s]+", name)
    if parts and parts[0]:
        return parts[0].title()
    return name.title()


@app.template_filter("mmdd")
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
    # Only include non-project issues in the index summary
    completed_priority_bugs = [
        issue
        for issue in get_completed_issues(2, "Bug", days)
        if not issue.get("project")
    ]
    completed_bugs = [
        issue
        for issue in get_completed_issues(5, "Bug", days)
        if not issue.get("project")
    ]
    completed_new_features = [
        issue
        for issue in get_completed_issues(
            5,
            "New Feature",
            days,
        )
        if not issue.get("project")
    ]
    completed_technical_changes = [
        issue
        for issue in get_completed_issues(
            5,
            "Technical Change",
            days,
        )
        if not issue.get("project")
    ]
    open_work = (
        get_open_issues(5, "Bug")
        + get_open_issues(5, "New Feature")
        + get_open_issues(5, "Technical Change")
    )
    time_data = get_time_data(completed_priority_bugs)
    fixes_per_day = (
        len(completed_bugs + completed_new_features + completed_technical_changes)
        / days
    )

    config_data = load_config()
    people_config = config_data.get("people", {})

    def format_display_name(linear_username: str) -> str:
        return re.sub(r"[._-]+", " ", linear_username).title()

    username_to_slug = {}
    github_to_linear = {}
    leaderboard_entries = {}

    for slug, info in people_config.items():
        linear_username = info.get("linear_username") or slug
        username_to_slug[linear_username] = slug
        github_username = info.get("github_username")
        if github_username:
            github_to_linear[github_username] = linear_username

    completed_work = (
        completed_bugs + completed_new_features + completed_technical_changes
    )

    for issue in completed_work:
        assignee = issue.get("assignee")
        if not assignee:
            continue
        linear_username = assignee.get("name") or assignee.get("displayName")
        if not linear_username:
            continue
        display_name = assignee.get("displayName") or format_display_name(
            linear_username
        )
        entry = leaderboard_entries.setdefault(
            linear_username,
            {
                "display_name": display_name,
                "linear_username": linear_username,
                "score": 0,
                "issues": [],
                "reviews": 0,
            },
        )
        entry["issues"].append(issue)
        entry["score"] += PRIORITY_TO_SCORE.get(issue.get("priority"), 1)

    for reviewer, prs in merged_prs_by_reviewer(days).items():
        linear_username = github_to_linear.get(reviewer)
        if linear_username:
            entry = leaderboard_entries.setdefault(
                linear_username,
                {
                    "display_name": format_display_name(linear_username),
                    "linear_username": linear_username,
                    "score": 0,
                    "issues": [],
                    "reviews": 0,
                },
            )
        else:
            entry = leaderboard_entries.setdefault(
                reviewer,
                {
                    "display_name": format_display_name(reviewer),
                    "linear_username": None,
                    "score": 0,
                    "issues": [],
                    "reviews": 0,
                },
            )
        review_points = len(prs)
        entry["reviews"] += review_points
        entry["score"] += review_points

    completed_issues_by_assignee = sorted(
        leaderboard_entries.values(),
        key=lambda item: item["score"],
        reverse=True,
    )

    return render_template(
        "index.html",
        days=days,
        priority_issues=sorted(open_priority_bugs, key=lambda x: x["createdAt"]),
        issue_count=len(created_priority_bugs),
        priority_percentage=int(
            len(completed_priority_bugs)
            / len(completed_bugs + completed_new_features + completed_technical_changes)
            * 100
        ),
        completed_issues_by_assignee=completed_issues_by_assignee,
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
    github_username = person_cfg.get("github_username")
    with ThreadPoolExecutor(max_workers=3) as executor:
        open_future = executor.submit(get_open_issues_for_person, login)
        completed_future = executor.submit(get_completed_issues_for_person, login, days)
        github_future = None
        if github_username:
            github_future = executor.submit(
                lambda: (
                    merged_prs_by_author(days),
                    merged_prs_by_reviewer(days),
                )
            )
        open_items = sorted(
            open_future.result(timeout=30),
            key=lambda x: x["updatedAt"],
            reverse=True,
        )
        completed_items = sorted(
            completed_future.result(timeout=30),
            key=lambda x: x["completedAt"],
            reverse=True,
        )
        if github_future:
            author_map, reviewer_map = github_future.result(timeout=30)
            prs_merged = len(author_map.get(github_username, []))
            prs_reviewed = len(reviewer_map.get(github_username, []))
        else:
            prs_merged = prs_reviewed = 0

    priority_fix_times = []
    priority_bugs_fixed = 0
    for issue in completed_items:
        is_priority_bug = issue.get("priority", 5) <= 2 and any(
            lbl.get("name") == "Bug" for lbl in issue.get("labels", {}).get("nodes", [])
        )
        if not is_priority_bug:
            continue
        priority_bugs_fixed += 1
        if issue.get("assignee_time_to_fix") is not None:
            fix_time = issue["assignee_time_to_fix"]
            priority_fix_times.append(fix_time)

    if priority_fix_times:
        avg_priority_bug_fix = int(sum(priority_fix_times) / len(priority_fix_times))
    else:
        avg_priority_bug_fix = None

    # Compute metrics for all completed work
    all_work_done = len(completed_items)
    all_fix_times = [
        issue["assignee_time_to_fix"]
        for issue in completed_items
        if issue.get("assignee_time_to_fix") is not None
    ]
    if all_fix_times:
        avg_all_time_to_fix = int(sum(all_fix_times) / len(all_fix_times))
    else:
        avg_all_time_to_fix = None

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
    projects_by_initiative = dict(
        sorted(projects_by_initiative.items(), key=lambda x: x[0])
    )
    current_projects = (
        projects_by_initiative.get(cycle_initiative, []) if cycle_initiative else []
    )
    current_names = [proj.get("name") for proj in current_projects]

    on_support = slug in get_support_slugs()
    if on_support:
        open_current_cycle = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj in ["Customer Success", "No Project"]
        }
        open_other = {
            proj: issues
            for proj, issues in open_by_project.items()
            if proj not in ["Customer Success", "No Project"]
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

    return render_template(
        "person.html",
        person_slug=slug,
        person_name=person_name,
        days=days,
        open_current_cycle=open_current_cycle,
        open_other=open_other,
        completed_by_project=completed_by_project,
        on_call_support=on_support,
        work_by_platform=work_by_platform,
        prs_merged=prs_merged,
        prs_reviewed=prs_reviewed,
        priority_bug_avg_time_to_fix=avg_priority_bug_fix,
        priority_bugs_fixed=priority_bugs_fixed,
        all_work_done=all_work_done,
        avg_all_time_to_fix=avg_all_time_to_fix,
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

    # Extract projects for the Onboarding Churches initiative (active only)
    onboarding_initiative_name = "Onboarding Churches"
    inactive_project_statuses = {"Completed", "Incomplete", "Canceled"}
    onboarding_churches_projects = [
        p
        for p in projects_by_initiative.get(onboarding_initiative_name, [])
        if p.get("status", {}).get("name") not in inactive_project_statuses
    ]

    # filter to only the cycle initiative (from config.yml)
    current_init = config.get("cycle_initiative")
    if current_init:
        projects_by_initiative = {
            name: projects
            for name, projects in projects_by_initiative.items()
            if name == current_init
        }

    # Separate completed or incomplete projects from the cycle initiatives
    completed_projects = []
    for name, projects in list(projects_by_initiative.items()):
        remaining = []
        for project in projects:
            if project.get("status", {}).get("name") in inactive_project_statuses:
                completed_projects.append(project)
            else:
                remaining.append(project)
        if remaining:
            projects_by_initiative[name] = remaining
        else:
            del projects_by_initiative[name]

    # Determine which team members are participating in cycle projects
    cycle_projects_filtered = [
        p for projs in projects_by_initiative.values() for p in projs
    ]

    def normalize(name: str) -> str:
        """Normalize a Linear display name or username for comparison."""
        return name.replace(".", " ").replace("-", " ").title()

    name_to_slug = {}
    for slug, info in config.get("people", {}).items():
        username = info.get("linear_username", slug)
        full = normalize(username)
        # Map the full normalized name to the slug
        name_to_slug[full] = slug
        first = full.split()[0]
        # Also map first name if unique (don't overwrite existing mapping)
        name_to_slug.setdefault(first, slug)

    cycle_member_slugs = set()
    member_projects = {}
    for project in cycle_projects_filtered:
        # Only include projects that have started (start date today or earlier)
        starts_in = project.get("starts_in")
        if starts_in is not None and starts_in > 0:
            continue
        lead = (project.get("lead") or {}).get("displayName")
        participants = []
        if lead:
            participants.append(lead)
        participants.extend(project.get("members", []))
        for name in participants:
            slug = name_to_slug.get(normalize(name)) or name_to_slug.get(
                normalize(name).split()[0]
            )
            if slug:
                cycle_member_slugs.add(slug)
                member_projects.setdefault(slug, set()).add(
                    (project.get("name"), project.get("url"))
                )

    # Convert sets back to sorted lists of dicts
    member_projects = {
        slug: [
            {"name": name, "url": url}
            for name, url in sorted(projects, key=lambda x: x[0])
        ]
        for slug, projects in member_projects.items()
    }

    developers = sorted(
        [{"slug": slug, "name": format_name(slug)} for slug in cycle_member_slugs],
        key=lambda d: d["name"],
    )

    # Build onboarding members and their projects for Current Focus section
    onboarding_member_slugs = set()
    onboarding_member_projects = {}
    for project in onboarding_churches_projects:
        lead = (project.get("lead") or {}).get("displayName")
        participants = []
        if lead:
            participants.append(lead)
        participants.extend(project.get("members", []))
        for name in participants:
            slug = name_to_slug.get(normalize(name)) or name_to_slug.get(
                normalize(name).split()[0]
            )
            if slug:
                onboarding_member_slugs.add(slug)
                onboarding_member_projects.setdefault(slug, set()).add(
                    (project.get("name"), project.get("url"))
                )

    onboarding_member_projects = {
        slug: [
            {"name": name, "url": url}
            for name, url in sorted(projects, key=lambda x: x[0])
        ]
        for slug, projects in onboarding_member_projects.items()
    }
    onboarding_developers = sorted(
        [{"slug": slug, "name": format_name(slug)} for slug in onboarding_member_slugs],
        key=lambda d: d["name"],
    )

    support_slugs = get_support_slugs()
    on_call_support = sorted(
        [{"slug": name, "name": format_name(name)} for name in support_slugs],
        key=lambda d: d["name"],
    )

    # Map open priority bug issues to on-call support members
    priority_bugs = get_open_issues(2, "Bug")
    bugs_by_assignee = by_assignee(priority_bugs)
    support_issues = {}
    for assignee, data in bugs_by_assignee.items():
        slug = name_to_slug.get(normalize(assignee)) or name_to_slug.get(
            normalize(assignee).split()[0]
        )
        if slug:
            support_issues[slug] = [
                {"title": issue["title"], "url": issue["url"]}
                for issue in data["issues"]
            ]

    return render_template(
        "team.html",
        platform_teams=platform_teams,
        developers=developers,
        developer_projects=member_projects,
        onboarding_developers=onboarding_developers,
        onboarding_developer_projects=onboarding_member_projects,
        cycle_projects_by_initiative=projects_by_initiative,
        completed_cycle_projects=completed_projects,
        on_call_support=on_call_support,
        support_issues=support_issues,
        onboarding_churches_projects=onboarding_churches_projects,
    )


if __name__ == "__main__":
    app.run(debug=True)
