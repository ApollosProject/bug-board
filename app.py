from flask import Flask, render_template

from bb import get_priority_issues

app = Flask(__name__)


@app.route("/")
def index():
    issues = get_priority_issues()
    return render_template("index.html", priority_issues=issues["issues"]["nodes"])


if __name__ == "__main__":
    app.run(debug=True)
