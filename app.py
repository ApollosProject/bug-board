from flask import Flask, render_template, request
import yaml

from linear import (
    by_assignee,
    by_platform,
    get_completed_issues,
    get_created_issues,
    get_open_issues,
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


@app.route("/team")
def team():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)

    def format_name(key):
        data = config["people"].get(key, {})
        name = data.get("linear_username", key)
        return name.replace(".", " ").replace("-", " ").title()

    leads = {
        slug: format_name(info.get("lead"))
        for slug, info in config.get("platforms", {}).items()
    }
    developers = sorted(
        {format_name(person) for person in config.get("people", {})}
    )
    on_call_support = [
        format_name(name)
        for name, person in config.get("people", {}).items()
        if person.get("on_call_support")
    ]

    return render_template(
        "team.html",
        leads=leads,
        developers=developers,
        on_call_support=on_call_support,
    )


if __name__ == "__main__":
    app.run(debug=True)
