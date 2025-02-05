from flask import Flask, render_template

from bb import by_assignee, get_completed_issues, get_lead_time_data, get_open_issues

app = Flask(__name__)


@app.route("/")
def index():
    open_priority_bugs = get_open_issues(2, "Bug")
    completed_priority_bugs = get_completed_issues(2, "Bug")
    completed_bugs = get_completed_issues(5, "Bug")
    completed_new_features = get_completed_issues(5, "New Feature")
    lead_time_data = get_lead_time_data(completed_priority_bugs)
    return render_template(
        "index.html",
        priority_issues=open_priority_bugs,
        completed_issue_count=len(completed_priority_bugs),
        completed_priority_bugs_by_assignee=by_assignee(completed_priority_bugs),
        completed_bugs_by_assignee=by_assignee(completed_bugs),
        completed_features_by_assignee=by_assignee(completed_new_features),
        lead_time_data=lead_time_data,
    )


if __name__ == "__main__":
    app.run(debug=True)
