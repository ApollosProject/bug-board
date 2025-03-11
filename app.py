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


# add a param for days to the index route
@app.route("/")
@app.route("/<int:days>")
def index(days=30):
    created_priority_bugs = get_created_issues(2, "Bug", days)
    open_priority_bugs = get_open_issues(2, "Bug")
    completed_priority_bugs = get_completed_issues(2, "Bug", days)
    completed_bugs = get_completed_issues(5, "Bug", days)
    completed_new_features = get_completed_issues(5, "New Feature", days)
    time_data = get_time_data(completed_priority_bugs)
    fixes_per_day = len(completed_priority_bugs) / days
    return render_template(
        "index.html",
        days=days,
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
