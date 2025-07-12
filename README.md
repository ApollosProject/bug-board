# Bug Board

A small Flask application that displays Linear issues and GitHub pull request stats. It also includes a worker process that posts daily summaries to Slack.

## Setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Development Setup

For development, also install the development dependencies which include linting tools:

```bash
pip install -r dev-requirements.txt
```

To lint your code before committing:

```bash
flake8 *.py
```

2. Provide the required environment variables. The application expects the following values:

- `LINEAR_API_KEY` – API token for Linear
- `GITHUB_TOKEN` – GitHub token used for pull‑request data
- `SLACK_WEBHOOK_URL` – Webhook URL used by the worker to post messages
- `APP_URL` – Public URL where the app is hosted
- `DEBUG` – set to `true` to run the scheduled jobs immediately

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


