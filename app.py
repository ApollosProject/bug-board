from flask import Flask, render_template, request, abort

from config import get_people, get_platforms, get_person

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
    get_user_id,
)

app = Flask(__name__)


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
        priority_issues=open_priority_bugs,
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


@app.route("/person/<slug>")
def person(slug):
    """Display open and completed work for a person."""
    info = get_person(slug)
    if not info:
        abort(404)
    user_id = info.get("linear_id") or get_user_id(info.get("linear_username", slug))

    days = request.args.get("days", default=30, type=int)
    open_items = get_open_issues_for_person(user_id)
    completed_items = get_completed_issues_for_person(user_id, days)
    return render_template(
        "person.html",
        slug=slug,
        days=days,
        open_by_project=by_project(open_items),
        completed_by_project=by_project(completed_items),
    )


@app.route("/team")
def team():
    people = get_people()
    platforms = get_platforms()

    def format_name(key):
        data = people.get(key, {})
        name = data.get("linear_username", key)
        return name.replace(".", " ").replace("-", " ").title()

    platform_teams = {}
    for slug, info in platforms.items():
        lead = info.get("lead")
        developers = [dev for dev in info.get("developers", []) if dev != lead]
        developers = sorted(developers, key=lambda d: format_name(d))
        members = [{"name": format_name(lead), "lead": True}] + [
            {"name": format_name(dev), "lead": False} for dev in developers
        ]
        platform_teams[slug] = members

    developers = [
        {"name": format_name(person), "slug": person}
        for person in people
    ]
    developers = sorted(developers, key=lambda d: d["name"])
    on_call_support = [
        format_name(name)
        for name, person in people.items()
        if person.get("on_call_support")
    ]

    return render_template(
        "team.html",
        platform_teams=platform_teams,
        developers=developers,
        on_call_support=on_call_support,
    )


if __name__ == "__main__":
    app.run(debug=True)
