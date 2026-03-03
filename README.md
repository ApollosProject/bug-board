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
- `AIRFLOW_API_BASE_URL` – Base URL for Airflow REST API (for example: `https://airflow.example.com`)
- `AIRFLOW_API_TOKEN` – Bearer token for Airflow API
- `AIRFLOW_FLEET_MONITOR_TOKEN` – Optional token required by `/airflow-fleet-health`

These can be placed in a `.env` file or exported in your shell.

3. Edit `config.yml` to configure team members and platform ownership.

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

## Airflow fleet outage monitor

This app exposes `GET /airflow-fleet-health` for Better Stack to detect broad DAG failures
without relying on Airflow DAG execution itself.

The endpoint:

- Calls the Airflow REST API and inspects each active DAG's latest run state
- Computes failed/evaluated ratio across active DAGs (not time-window based)
- Returns `503` when failure ratio is `>= 0.35` (with at least 20 DAGs evaluated), otherwise `200`

This checker is intentionally not highly configurable. It uses fixed settings:

- failure threshold ratio: `0.35`
- minimum evaluated DAGs: `20`

If `AIRFLOW_FLEET_MONITOR_TOKEN` is set, Better Stack must send either:

- `Authorization: Bearer <token>` header, or
- `?token=<token>` query param
