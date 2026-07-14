# Bug Board

A small Flask application that displays Linear issues and GitHub pull request stats. It also includes a worker process that posts daily summaries to Slack.

## Setup

1. Create a virtual environment and install dependencies.
   Before creating the venv, make sure your shell is using the interpreter
   selected by `.python-version` (for example via `pyenv`):

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # includes ruff and vulture for static analysis
```

To lint, format, and type check your code before committing:

```bash
ruff check .
ruff check . --fix
ruff format .
vulture . --config pyproject.toml
mypy .
```

To run unit tests locally:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

2. Provide the required environment variables. The application expects the following values:

- `LINEAR_API_KEY` – API token for Linear
- `GITHUB_TOKEN` – GitHub token used for pull-request data and Platforms release/commit lookups
- `SLACK_WEBHOOK_URL` – Webhook URL used by the worker to post messages
- `MANAGER_SLACK_WEBHOOK_URL` – Webhook URL used for manager-facing summaries
- `APP_URL` – Public URL where the app is hosted
- `DEBUG` – set to `true` to run the scheduled jobs immediately
- `OPENAI_API_KEY` – API key used to generate weekly changelogs
- `AIRFLOW_API_BASE_URL` – Base URL for Airflow REST API (for example: `https://airflow.example.com`)
- `AIRFLOW_API_TOKEN` – Bearer token for Airflow API
- `AIRFLOW_FLEET_HEARTBEAT_URL` – Optional Better Stack heartbeat URL for worker-reported Airflow fleet health
- `REDIS_URL` – Optional Redis connection string for cached Airflow fleet-health responses
- `REDIS_SSL_CERT_REQS` – Optional TLS cert verification mode for `rediss://` (`none`, `optional`, `required`; default for `rediss://` is `none` unless `REDIS_URL` already sets `ssl_cert_reqs`)
- `AIRFLOW_FLEET_HEALTH_REFRESH_SECONDS` – Optional worker refresh interval for cached fleet health (default: `60`)
- `AIRFLOW_FLEET_HEALTH_MAX_STALE_SECONDS` – Optional max age accepted by the web endpoint when reading cached data (default: `180`)
- `AIRFLOW_FLEET_HEALTH_REDIS_TTL_SECONDS` – Optional Redis TTL for cached fleet health record (default: `900`)
- `BIGQUERY_ANALYTICS_PROJECT_ID` – Optional Google Cloud project that contains the Segment BigQuery export (default: `apollos-project`)
- `BIGQUERY_ANALYTICS_DATASETS` – Optional comma-separated BigQuery datasets containing Segment export tables (default: `apollos,apollos_tv,apollos_roku`)
- `BIGQUERY_ANALYTICS_TABLES` – Optional comma-separated Segment tables to inspect for app runtime versions (default: `identifies,screens,app_became_active,app_became_backgrounded,app_became_inactive`)
- `BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64` – Base64-encoded Google service account JSON for BigQuery access
- `APP_VERSIONS_LOOKBACK_DAYS` – Optional lookback window for `/apps` (default: `30`)
- `APP_VERSIONS_LIMIT` – Optional maximum app rows rendered by `/apps` (default: `1000`)

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

## Airflow fleet outage heartbeat

The worker can report Airflow fleet health to Better Stack using a heartbeat, which avoids
Better Stack polling this app as an uptime monitor. Configure a Better Stack heartbeat and set
`AIRFLOW_FLEET_HEARTBEAT_URL` to its secret URL.

On each worker refresh, the app:

- Evaluates the Airflow REST API and inspects each active DAG's latest run state
- Refreshes the Redis-backed fleet-health cache when Redis is configured
- Sends the base heartbeat URL when fleet health is healthy
- Sends the heartbeat URL with `/fail` appended when fleet health is degraded
- Suppresses one-off `unknown` evaluations and only sends `/fail` after 3 consecutive unknowns

The health calculation:

- Computes failed/evaluated ratio across active DAGs (not time-window based)
- Returns `503` when failure ratio is `>= 0.10` (with at least 20 DAGs evaluated), otherwise `200`
- Includes both `top_failed_dags` and the full `failed_dags` list in the JSON payload
- When `REDIS_URL` is configured, reads fleet health from Redis for fast responses
- When `REDIS_URL` is not configured, bypasses Redis and evaluates directly per request
- With `REDIS_URL` configured, cache miss/stale returns `{"status":"unknown"}` with `503` until worker refresh succeeds

For humans, `GET /failing-dags` renders the same fleet-health data as an internal dashboard page
and links back to the Astro filtered DAG view. The dashboard always serves cached fleet-health
data and never performs a live full-fleet Airflow scan during a web request in deployed
environments. In local debug mode, if `REDIS_URL` is not configured, the dashboard falls back
to a live evaluation so the page can be validated without a worker/cache setup. Without a fresh
Redis-backed cache value outside local debug mode, it renders the unavailable/setup-required
state instead.

This checker is intentionally not highly configurable. It uses fixed settings:

- failure threshold ratio: `0.10`
- minimum evaluated DAGs: `20`

When Redis caching or the Better Stack heartbeat is enabled, run the worker process
(`python jobs.py`) so it refreshes fleet health on the configured interval.

The legacy `GET /airflow-fleet-health` Better Stack monitor endpoint has been removed.

## Apps dashboard

`GET /apps` reads the Segment BigQuery export and reports production freshness using the release
signal appropriate to each platform:

- iOS and Android compare the observed Expo runtime with the latest production runtime for that
  platform.
- tvOS, AndroidTV, and Amazon compare stable `apollos-platforms` release tags. An alpha observation
  is treated as a prerelease unless its source revision exactly matches a published stable tag, in
  which case the stable tag is displayed.
- Roku compares its embedded source revision with the latest commit on `master` that changed
  `templates/roku`. This reports source freshness without implying that the manual Roku deployment
  itself was automated.

New builds send `deployment_track` alongside `source_revision` and `source_version`, allowing the
dashboard to keep internal and prerelease builds out of production baselines. Older observations
without that field are classified from their release tag and revision. Public App Store versions
are also looked up by bundle ID for iOS, but are shown as informational app-version context rather
than used to determine Expo runtime freshness.

The page first inspects `INFORMATION_SCHEMA.COLUMNS` for the configured Segment tables and only
queries tables that expose a supported version signal, so Segment lifecycle-only app-store
`version` fields are not mistaken for Apollos runtime versions. If older clients are still active
after a release, the dashboard keeps the highest production observation for the app instead of
letting the most recent older-client event hide it. Mobile rows prefer the `apollos` Segment
dataset, TV rows prefer `apollos_tv`, and Roku rows prefer `apollos_roku` so the same app event is
not counted twice when Segment exports overlap. Rows show `TBD` until the metadata required by their
platform's freshness flow appears.

To make the dashboard query live data locally, in production, or in review apps, set
`BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64`. The value should be a base64-encoded Google service
account JSON with BigQuery read access to `apollos-project`. Application Default Credentials are
not used by this dashboard.

The legacy `/app-versions` URL renders the same dashboard for compatibility with existing links.
