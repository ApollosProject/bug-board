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
- `AIRFLOW_API_TOKEN` – Bearer token for Airflow API (or use username/password below)
- `AIRFLOW_API_USERNAME` – Optional Airflow API username when not using token auth
- `AIRFLOW_API_PASSWORD` – Optional Airflow API password when not using token auth
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
- Applies stateful transitions (`2` bad windows to trigger degraded, `3` good windows to resolve)
- Returns `503` while degraded, otherwise `200`

Optional tuning env vars:

- `AIRFLOW_FLEET_FAILURE_THRESHOLD_RATIO` (default: `0.35`)
- `AIRFLOW_FLEET_MIN_TERMINAL_RUNS` (default: `20`)
- `AIRFLOW_FLEET_TRIGGER_BAD_WINDOWS` (default: `2`)
- `AIRFLOW_FLEET_RESOLVE_GOOD_WINDOWS` (default: `3`)
- `AIRFLOW_FLEET_EXCLUDED_DAGS` (comma-separated DAG IDs)
- `AIRFLOW_FLEET_STATE_FILE` (default: `/tmp/airflow_fleet_state.json`)
- `AIRFLOW_FLEET_REQUEST_TIMEOUT_SECONDS` (default: `20`)
- `AIRFLOW_FLEET_DAG_RUNS_PAGE_SIZE` (default: `200`)
- `AIRFLOW_FLEET_DAG_QUERY_WORKERS` (default: `30`)

If `AIRFLOW_FLEET_MONITOR_TOKEN` is set, Better Stack must send either:

- `Authorization: Bearer <token>` header, or
- `?token=<token>` query param
