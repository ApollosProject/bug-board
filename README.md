# Bug Board

A small Flask application that displays Linear issues and GitHub pull request stats. It also includes a worker process that posts daily summaries to Slack.

## Setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # includes flake8 for linting
```

To lint and type check your code before committing:

```bash
flake8 *.py
mypy .
```

2. Provide the required environment variables. The application expects the following values:

- `LINEAR_API_KEY` – API token for Linear
- `GITHUB_TOKEN` – GitHub token used for pull‑request data
- `SLACK_WEBHOOK_URL` – Webhook URL used by the worker to post messages
- `MANAGER_SLACK_WEBHOOK_URL` – Webhook URL used for manager-facing summaries
- `APP_URL` – Public URL where the app is hosted
- `DEBUG` – set to `true` to run the scheduled jobs immediately
- `OPENAI_API_KEY` – API key used to generate weekly changelogs

These can be placed in a `.env` file or exported in your shell.

3. Edit `config.yml` to configure team members and platform ownership.

## RECON Daily Post

The worker posts a daily RECON Issues summary to Slack at **9:00am ET** by default.

Optional environment variables:

- `RECON_PROJECT_NAME` – Linear project name to report on (default: `RECON Issues`)
- `RECON_TIMEZONE` – Time zone used for scheduling + day counters (default: `America/New_York`)
- `RECON_CC_SLUGS` – Comma-separated `config.yml` people slugs to CC on SLA breach (default: `gerry,tyler`)
- `RECON_CC_SLACK_IDS` – Comma-separated Slack user IDs to CC (overrides `RECON_CC_SLUGS`)
- `RECON_DRY_RUN` – If `true`, do not post to Slack (logs the message instead)

State notes:

- `Days since last open issue` is derived from the most recently resolved *parent* issue in the RECON project (Done/Canceled/Duplicate) and is only displayed when there are currently 0 open parent issues.

## Running

Start the web server with:

```bash
gunicorn app:app
```

To run the scheduled jobs locally, start the worker:

```bash
python jobs.py
```


The `Procfile` defines both commands for platforms such as Heroku.
