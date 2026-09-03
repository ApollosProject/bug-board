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
- `GITHUB_TOKEN` – GitHub token used for pull‑request data
- `GITHUB_OAUTH_ENABLED` – Set to `true` to require GitHub sign-in; the app also enables the gate automatically when either OAuth credential is configured
- `GITHUB_OAUTH_CLIENT_ID` – Client ID for the GitHub OAuth app that gates dashboard access
- `GITHUB_OAUTH_CLIENT_SECRET` – Client secret for the GitHub OAuth app
- `GITHUB_OAUTH_CALLBACK_URL` – OAuth callback URL (for example, `https://your-app.example/auth/github/callback`); when omitted, the app uses `APP_URL` plus `/auth/github/callback`
- `GITHUB_OAUTH_ORG` – GitHub organization whose active members can sign in (default: `ApollosProject`)
- `FLASK_SECRET_KEY` – Random value of at least 32 characters used to sign login sessions
- `BUG_BOARD_API_KEY` – Static key that authenticates the JSON API (see [JSON API](#json-api)); leave unset to keep the API disabled
- `SLACK_WEBHOOK_URL` – Webhook URL used by the worker to post messages
- `MANAGER_SLACK_WEBHOOK_URL` – Webhook URL used for manager-facing summaries
- `APP_URL` – Public URL where the app is hosted
- `DEBUG` – set to `true` to run the scheduled jobs immediately
- `OPENAI_API_KEY` – API key used to generate weekly changelogs
- `AIRFLOW_API_BASE_URL` – Base URL for Airflow REST API (for example: `https://airflow.example.com`)
- `AIRFLOW_API_TOKEN` – Bearer token for Airflow API
- `AIRFLOW_FLEET_HEARTBEAT_URL` – Optional Better Stack heartbeat URL for worker-reported Airflow fleet health
- `REDIS_URL` – Optional Redis connection string for cached Airflow fleet-health and team metrics responses
- `REDIS_SSL_CERT_REQS` – Optional TLS cert verification mode for `rediss://` (`none`, `optional`, `required`; default for `rediss://` is `none` unless `REDIS_URL` already sets `ssl_cert_reqs`)
- `AIRFLOW_FLEET_HEALTH_REFRESH_SECONDS` – Optional worker refresh interval for cached fleet health and team metrics (default: `60`)
- `AIRFLOW_FLEET_HEALTH_MAX_STALE_SECONDS` – Optional max age accepted by the web endpoint when reading cached data (default: `180`)
- `AIRFLOW_FLEET_HEALTH_REDIS_TTL_SECONDS` – Optional Redis TTL for cached fleet health record (default: `900`)
- `LEADERBOARD_REDIS_TTL_SECONDS` – Optional Redis TTL for cached team metrics (default: `900`)
- `REGRESSION_REDIS_TTL_SECONDS` – Optional Redis TTL for cached regression reports (default: `86400`)
- `BIGQUERY_ANALYTICS_PROJECT_ID` – Optional Google Cloud project that contains the Segment BigQuery export (default: `apollos-project`)
- `BIGQUERY_ANALYTICS_DATASETS` – Optional comma-separated BigQuery datasets containing Segment export tables (default: `apollos,apollos_tv,apollos_roku`)
- `BIGQUERY_ANALYTICS_TABLES` – Optional comma-separated Segment tables to inspect for app runtime versions (default: `identifies,screens,app_became_active,app_became_backgrounded,app_became_inactive`)
- `BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64` – Base64-encoded Google service account JSON for BigQuery access
- `APP_VERSIONS_LOOKBACK_DAYS` – Optional lookback window for `/apps` (default: `30`)
- `APP_VERSIONS_LIMIT` – Optional maximum app rows rendered by `/apps` (default: `1000`)
- `RIPPLING_PTO_CALENDAR_URL` – Optional private Rippling direct-reports calendar subscription URL used to add OOO bars to the project timeline; treat this value as a secret
- `RIPPLING_PTO_TIMEZONE` – Optional IANA time zone used to place timed PTO entries on calendar days (default: `America/New_York`)

These can be placed in a `.env` file or exported in your shell.

### GitHub access gate

Create an OAuth app owned by the `ApollosProject` GitHub organization and set its authorization
callback URL to the same value as `GITHUB_OAUTH_CALLBACK_URL`. The application requests only the
`read:org` scope, validates both the signed-in GitHub identity and active organization membership,
and keeps the resulting login session for at most eight hours. The temporary GitHub access token is
not stored in the session.

Production uses `https://engineering.apollos.app` as `APP_URL` and
`https://engineering.apollos.app/auth/github/callback` as `GITHUB_OAUTH_CALLBACK_URL`.

Set `GITHUB_OAUTH_ENABLED=true`, `GITHUB_OAUTH_CLIENT_ID`, `GITHUB_OAUTH_CLIENT_SECRET`, and
`FLASK_SECRET_KEY` in the deployed environment. Also set either `GITHUB_OAUTH_CALLBACK_URL` or
`APP_URL`. Once OAuth is enabled or partially configured, the dashboard fails closed with `503`
until every required value is present. `GET /healthz` remains public for platform health checks;
all dashboard and static-resource routes require a verified session.

For local OAuth testing, GitHub permits a loopback callback such as
`http://127.0.0.1:8000/auth/github/callback`. Generate a session key without committing it:

```bash
python -c 'import secrets; print(secrets.token_hex(32))'
```

3. Edit `config.yml` to configure team members and platform ownership.

`config.yml` also decides which Linear teams and labels feed the person metrics:

- `linear_team_key` – the primary Linear team (default `APO`). Team-wide pages (homepage stats,
  leaderboard, projects, regressions) query this team only.
- `linear_team_keys` – optional list of teams whose issues count toward each person's All Work
  Done, Time to Completion, and Priority Bugs Fixed cards, e.g. `[APO, SUP]`. The primary team is
  always included.
- `bug_labels` – label names that mark an issue as a bug for Priority Bugs Fixed (default
  `[Bug]`; `[Bug, Issue]` covers both the engineering and Support teams).

## Running

Start the web server with:

```bash
gunicorn app:app
```

When `REDIS_URL` is set, production web requests serve the 30-day team metrics table from
Redis. In local debug mode (`DEBUG=true`), a cache miss falls back to a live computation so the
page still works without `python jobs.py`.

To run the scheduled jobs locally, start the worker:

```bash
python jobs.py
```


The `Procfile` defines both commands for platforms such as Heroku.

## JSON API

The dashboard is gated by GitHub OAuth, which scripts cannot complete. Read-only JSON endpoints
authenticate with a static key instead. Generate one and set `BUG_BOARD_API_KEY`:

```bash
python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

While `BUG_BOARD_API_KEY` is unset the API answers `503`, so the endpoints stay closed by default.
Callers pass the key as either `Authorization: Bearer <key>` or `X-API-Key: <key>`; anything else
gets a `401`.

### `GET /api/team/<slug>`

The JSON form of the `/team/<slug>` page. It accepts the same window parameters as the page —
either `days=<n>` or `start=YYYY-MM-DD&end=YYYY-MM-DD` (defaults to the last 30 days).

```bash
curl -sS -H "Authorization: Bearer $BUG_BOARD_API_KEY" \
  "https://engineering.apollos.app/api/team/zach?start=2026-08-31&end=2026-09-25"
```

Each entry in `metrics` carries the raw `value`, the `display` string the dashboard renders, and a
`vs_team` comparison against the other engineers — `null` when there is no cohort to compare with.
`z` is oriented so positive is better than the engineering average even for metrics where a lower
raw value is better, and `eng_avg`/`eng_stdev` describe the (outlier-trimmed) cohort baseline.

```json
{
  "person": { "slug": "zach", "name": "Zach", "github_username": "solideo-gloria" },
  "window": { "start": "2026-08-31", "end": "2026-09-25", "days": 26, "preset_days": null },
  "metrics": {
    "prs_merged": {
      "label": "PRs Merged",
      "value": 32,
      "display": "32",
      "vs_team": { "z": 2.41, "label": "+2.4σ", "tone": "high", "eng_avg": 12.0, "eng_stdev": 8.3 }
    }
  },
  "regressions": { "status": "ready", "authored": 1, "authored_rate": 3.1 },
  "links": { "github_merged_prs": "https://github.com/pulls?q=..." }
}
```

Adding an endpoint under `/api/` does not by itself exempt it from the OAuth gate: only views
decorated with `require_api_key` (`api.py`) are exempted, and `tests/test_api.py` fails if an
`/api/` route skips the decorator.

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
- Includes the full active `dags` inventory plus `failed_dags` and `top_failed_dags`
- When `REDIS_URL` is configured, reads fleet health from Redis for fast responses
- When `REDIS_URL` is not configured, bypasses Redis and evaluates directly per request
- With `REDIS_URL` configured, cache miss/stale returns `{"status":"unknown"}` with `503` until worker refresh succeeds

For humans, `GET /dags` renders a searchable active-DAG inventory with each latest run state
and links into Astro. The legacy `GET /failing-dags` URL remains available. The dashboard serves cached fleet-health
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

`GET /apps` reads the Segment BigQuery export and shows the highest observed Apollos
version signal per church/app/platform. It uses the analytics metadata sent by the mobile and TV
apps, including the exported `apollos_version`, `app_version`, `app_update_id`, `bundle_id`,
`application_name`, `church`, `apollos_platform`, `source_revision`, and `source_version` fields.
Public App Store versions are also looked up by bundle ID when available so iOS rows can distinguish
the observed installed app version from the current production App Store version. Roku Segment
exports currently do not expose `apollos_version`, so Roku rows use the exported
`context_library_version` and are labelled as analytics library versions.

The page first inspects `INFORMATION_SCHEMA.COLUMNS` for the configured Segment tables and only
queries tables that expose a supported version signal, so Segment lifecycle-only app-store
`version` fields are not mistaken for Apollos runtime versions. Runtime rows are marked outdated
when their highest observed runtime is behind the highest runtime observed for the same platform in
the lookback window, or when the observed app version is behind an available App Store version. If
older clients are still active after a release, the dashboard keeps the highest observed runtime for
the app instead of letting the most recent older-client event hide it. Mobile rows prefer the
`apollos` Segment dataset, TV rows prefer `apollos_tv`, and Roku rows prefer `apollos_roku` so the
same app event is not counted twice when Segment exports overlap.
TV rows show `TBD` until source metadata appears in Segment exports. Once those fields are present,
TV freshness uses the highest observed `source_version` for each TV platform instead of the static
Expo runtime version.

To make the dashboard query live data locally, in production, or in review apps, set
`BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64`. The value should be a base64-encoded Google service
account JSON with BigQuery read access to `apollos-project`. Application Default Credentials are
not used by this dashboard.

The legacy `/app-versions` URL renders the same dashboard for compatibility with existing links.

## Regression metrics

The worker analyzes completed urgent/high-priority Linear bugs, blames lines removed by
their fixing PRs, and maps those commits back to likely inducing PR authors and approvers.
It refreshes one 30-day Redis summary every six hours.

The homepage shows team-level regression cards. Person pages show authored and approved
regression counts, rates, and direct links to the attributed GitHub PRs. Web requests only read
the cached summary and never run the attribution pipeline. Automated blame is directional rather
than proof of causality;
version-controlled corrections and exclusions can be added to
`regression_overrides.yml`.
