# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Bug Board is a Flask app that aggregates Linear issues, GitHub PR stats, and Airflow fleet health into an internal engineering dashboard. It also has a worker process (`jobs.py`) that posts scheduled summaries to Slack. There is no database; all data is fetched live from external APIs.

### Repo / module map

- `app.py` - Flask app, page routes, HTMX partial routes, `/healthz`, `/dags`, `/team`, `/team/<slug>`, and the dashboard's existing `/api/team/<slug>` route.
- `api.py` - JSON API authentication helpers. `require_api_key` registers endpoint names in `API_KEY_ENDPOINTS`; `github_oauth.py` uses that set to exempt API-key routes from the OAuth gate. `BUG_BOARD_API_KEY` may be sent with `Authorization: Bearer ...` or `X-API-Key`.
- `github_oauth.py` - GitHub OAuth dashboard gate. It fails closed with `503` when OAuth is enabled or partially configured but required settings are missing; `/healthz` stays public.
- `jobs.py` - Worker entrypoint for scheduled Slack posts, stale issue reminders, leaderboard posts, Airflow fleet-health heartbeats, and Redis-backed refresh jobs.
- `leaderboard.py`, `leaderboard_cache.py`, `leaderboard_export.py` - Team leaderboard scoring, optional Redis cache refresh/read paths, and CSV export support.
- `regressions.py`, `regression_cache.py`, `regression_overrides.yml` - Regression dashboard data, optional cached summary, and manual attribution/ignore overrides.
- `airflow_fleet_health.py`, `fleet_health_cache.py` - Airflow DAG health evaluation plus Redis cache storage/TTL/staleness handling.
- `app_versions.py` - App version dashboard context.
- `config.py`, `config.yml` - Team/member/project/support configuration. Prefer extending the existing YAML shape over adding new config sources.
- `templates/`, `templates/partials/` - Full pages and HTMX partials.
- `static/` - CSS and static image assets.
- `tests/` - Unit tests that document behavior. Start with the neighboring test file for the module you touch.

### Running the application

```bash
source venv/bin/activate
gunicorn app:app --bind 127.0.0.1:8000 --workers 1
```

The app starts and serves pages without any API keys configured. Routes like `/`, `/team`, `/healthz`, and `/dags` all return 200 even without `LINEAR_API_KEY` or `GITHUB_TOKEN` — the HTMX partials that fetch live data will fail gracefully. The `/healthz` endpoint always returns `{"status": "ok"}`.

### Local verify / QA flows

Activate the venv before running commands:

```bash
source venv/bin/activate
```

Lint, format, and type check:

```bash
ruff check .
ruff check . --fix
ruff format .
mypy .
```

Run the full unit suite:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

Target a single test file by changing the pattern, for example:

```bash
python -m unittest discover -s tests -p 'test_api.py'
python -m unittest discover -s tests -p 'test_github_oauth.py'
python -m unittest discover -s tests -p 'test_jobs.py'
```

Smoke the web app without secrets:

```bash
gunicorn app:app --bind 127.0.0.1:8000 --workers 1 &
server_pid=$!
trap 'kill "$server_pid"' EXIT
sleep 1
curl -fsS http://127.0.0.1:8000/healthz
curl -fsS http://127.0.0.1:8000/
```

The shell should show `/healthz` returning `{"status":"ok"}`. Pages can render without API keys; HTMX partials that need Linear, GitHub, or Airflow credentials may show empty/error states instead of live data.

For JSON API changes, use `tests/test_api.py` as the contract. JSON API routes decorated with `require_api_key` escape OAuth and use API-key authentication. With `BUG_BOARD_API_KEY` unset the API returns `503`; with a key configured, callers authenticate via `Authorization: Bearer <key>` or `X-API-Key: <key>`. When adding `/api/` routes, add `@require_api_key` or `tests/test_api.py` should fail.

For OAuth changes, use `README.md` and `tests/test_github_oauth.py` as the contract. When OAuth is enabled or partially configured, dashboard routes fail closed with `503` until all required settings are present. `/healthz` remains public for platform checks.

For worker changes, run or inspect `python jobs.py` paths and `tests/test_jobs.py`. Slack posting requires `SLACK_WEBHOOK_URL`; message links use `APP_URL`; Redis cache refresh jobs run only when `REDIS_URL` is set.

CI (`.github/workflows/ci.yml`) uses Python 3.12. The `.python-version` file says 3.13 but 3.12 works and is what CI uses.

### Environment variables

The app runs without any env vars for basic page rendering. External-API-dependent features (leaderboard data, team member views, Airflow fleet health) require `LINEAR_API_KEY`, `GITHUB_TOKEN`, `AIRFLOW_API_BASE_URL`, and `AIRFLOW_API_TOKEN`. The JSON API under `/api/` requires `BUG_BOARD_API_KEY` and returns `503` without it. The worker process (`python jobs.py`) requires `SLACK_WEBHOOK_URL` and `APP_URL`. When `REDIS_URL` is set, the worker also refreshes the homepage leaderboard cache. See `README.md` for the full list.

Do not paste secrets into docs, tests, logs, or PR descriptions. Use placeholder names such as `$BUG_BOARD_API_KEY`.

### Agent working conventions

- Keep PRs small and focused; prefer updating the nearest module and its neighboring tests over broad refactors.
- Do not invent second sources of truth for config, route allowlists, team mappings, or scoring constants. Extend `config.yml`, `API_KEY_ENDPOINTS` via `@require_api_key`, or existing constants/helpers as appropriate.
- When adding `/api/` routes, always use `require_api_key` and cover the behavior in `tests/test_api.py`.
- Preserve the app's no-secrets local rendering path. Dashboard pages should continue to start without Linear/GitHub/Airflow credentials and degrade gracefully where live data is unavailable.
- Follow patterns in adjacent modules before adding new abstractions.

### Gotchas

- The venv must be activated before running any commands (`source venv/bin/activate`).
- `python3.12-venv` system package is required to create the venv (installed via `sudo apt-get install -y python3.12-venv`).
- mypy produces advisory notes about untyped function bodies — these are informational, not errors.
