# Bug Board

A small Flask application that displays Linear issues and GitHub pull request stats. It also includes a worker process that posts daily summaries to Slack.

## Setup

1. Create a virtual environment and install dependencies.
   Before creating the venv, make sure your shell is using the interpreter
   selected by `.python-version` (for example via `pyenv`):

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
- `REDIS_URL` – Optional Redis connection string for cached `/airflow-fleet-health` responses
- `REDIS_SSL_CERT_REQS` – Optional TLS cert verification mode for `rediss://` (`none`, `optional`, `required`; default for `rediss://` is `none` unless `REDIS_URL` already sets `ssl_cert_reqs`)
- `AIRFLOW_FLEET_HEALTH_REFRESH_SECONDS` – Optional worker refresh interval for cached fleet health (default: `60`)
- `AIRFLOW_FLEET_HEALTH_MAX_STALE_SECONDS` – Optional max age accepted by the web endpoint when reading cached data (default: `180`)
- `AIRFLOW_FLEET_HEALTH_REDIS_TTL_SECONDS` – Optional Redis TTL for cached fleet health record (default: `900`)

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
- Returns `503` when failure ratio is `>= 0.10` (with at least 20 DAGs evaluated), otherwise `200`
- Includes both `top_failed_dags` and the full `failed_dags` list in the JSON payload
- When `REDIS_URL` is configured, reads fleet health from Redis for fast responses
- When `REDIS_URL` is not configured, bypasses Redis and evaluates directly per request
- With `REDIS_URL` configured, cache miss/stale returns `{"status":"unknown"}` with `503` until worker refresh succeeds

For humans, `GET /failing-dags` renders the same fleet-health data as an internal dashboard page
and links back to the Astro filtered DAG view. When `AIRFLOW_FLEET_MONITOR_TOKEN` is
configured, the dashboard only serves cached fleet-health data so unauthenticated
requests cannot trigger a live Airflow scan.

This checker is intentionally not highly configurable. It uses fixed settings:

- failure threshold ratio: `0.10`
- minimum evaluated DAGs: `20`

When Redis caching is enabled, run the worker process (`python jobs.py`) so it refreshes
the cached fleet health value on the configured interval.

If `AIRFLOW_FLEET_MONITOR_TOKEN` is set, Better Stack must send either of these on
`GET /airflow-fleet-health`:

- `Authorization: Bearer <token>` header, or
- `?token=<token>` query param
