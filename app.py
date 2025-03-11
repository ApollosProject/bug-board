from flask import Flask, render_template

from linear import (
    by_assignee,
    by_reviewer,
    get_completed_issues,
    get_created_issues,
    get_open_issues,
    get_time_data,
)

app = Flask(__name__)


@app.route("/")
def index():
    created_priority_bugs = get_created_issues(2, "Bug")
    open_priority_bugs = get_open_issues(2, "Bug")
    completed_priority_bugs = get_completed_issues(2, "Bug")
    completed_bugs = get_completed_issues(5, "Bug")
    completed_new_features = get_completed_issues(5, "New Feature")
    time_data = get_time_data(completed_priority_bugs)
    fixes_per_day = len(completed_priority_bugs) / 30
    return render_template(
        "index.html",
        priority_issues=open_priority_bugs,
        issue_count=len(created_priority_bugs),
        completed_priority_bugs_by_assignee=by_assignee(completed_priority_bugs),
        completed_bugs_by_assignee=by_assignee(completed_bugs),
        completed_features_by_assignee=by_assignee(completed_new_features),
        issues_by_reviewer=by_reviewer(completed_bugs + completed_new_features),
        lead_time_data=time_data["lead"],
        queue_time_data=time_data["queue"],
        fixes_per_day=fixes_per_day,
    )


if __name__ == "__main__":
    app.run(debug=True)
