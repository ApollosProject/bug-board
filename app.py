from flask import Flask, render_template

from bb import (
    get_completed_priority_issues,
    get_lead_time_data,
    get_open_priority_issues,
)

app = Flask(__name__)


@app.route("/")
def index():
    open_issues = get_open_priority_issues()
    completed_issues = get_completed_priority_issues()
    lead_time_data = get_lead_time_data(completed_issues)
    print(lead_time_data)
    return render_template(
        "index.html",
        priority_issues=open_issues["issues"]["nodes"],
        completed_issue_count=len(completed_issues["issues"]["nodes"]),
        lead_time_data=lead_time_data,
    )


if __name__ == "__main__":
    app.run(debug=True)
