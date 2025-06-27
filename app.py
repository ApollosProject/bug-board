from flask import Flask, render_template, request

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


@app.route("/person/<person_id>")
def person(person_id):
    """Display open and completed work for a person."""
    days = request.args.get("days", default=30, type=int)
    open_items = get_open_issues_for_person(person_id)
    completed_items = get_completed_issues_for_person(person_id, days)
    return render_template(
        "person.html",
        person_id=person_id,
        days=days,
        open_by_project=by_project(open_items),
        completed_by_project=by_project(completed_items),
    )


if __name__ == "__main__":
    app.run(debug=True)
