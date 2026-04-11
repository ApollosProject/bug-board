# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Bug Board is a Flask app that aggregates Linear issues, GitHub PR stats, and Airflow fleet health into an internal engineering dashboard. It also has a worker process (`jobs.py`) that posts scheduled summaries to Slack. There is no database; all data is fetched live from external APIs.

### Running the application

```bash
source venv/bin/activate
gunicorn app:app --bind 127.0.0.1:8000 --workers 1
```

The app starts and serves pages without any API keys configured. Routes like `/`, `/team`, `/healthz`, and `/failing-dags` all return 200 even without `LINEAR_API_KEY` or `GITHUB_TOKEN` — the HTMX partials that fetch live data will fail gracefully. The `/healthz` endpoint always returns `{"status": "ok"}`.

### Lint, type check, and test commands

All standard — see `README.md`. Quick reference:

- **Lint:** `flake8 *.py`
- **Type check:** `mypy .`
- **Unit tests:** `python -m unittest discover -s tests -p 'test_*.py'`

CI (`.github/workflows/ci.yml`) uses Python 3.12. The `.python-version` file says 3.13 but 3.12 works and is what CI uses.

### Environment variables

The app runs without any env vars for basic page rendering. External-API-dependent features (leaderboard data, team member views, Airflow fleet health) require `LINEAR_API_KEY`, `GITHUB_TOKEN`, `AIRFLOW_API_BASE_URL`, and `AIRFLOW_API_TOKEN`. The worker process (`python jobs.py`) requires `SLACK_WEBHOOK_URL` and `APP_URL`. See `README.md` for the full list.

### Gotchas

- The venv must be activated before running any commands (`source venv/bin/activate`).
- `python3.12-venv` system package is required to create the venv (installed via `sudo apt-get install -y python3.12-venv`).
- mypy produces advisory notes about untyped function bodies — these are informational, not errors.
