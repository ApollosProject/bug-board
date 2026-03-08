---
tracker:
  kind: linear
  api_key: $LINEAR_API_KEY
  project_slug: "bug-board-74ad535500de"
  active_states:
    - Todo
    - In Progress
    - Human Review
    - Merging
    - Rework
  terminal_states:
    - Closed
    - Cancelled
    - Canceled
    - Duplicate
    - Done
polling:
  interval_ms: 5000
workspace:
  root: ~/code/symphony-workspaces
hooks:
  after_create: |
    git clone --depth 1 --branch main https://github.com/ApollosProject/bug-board.git .
    if [ -f /Users/michael/Documents/bug-board/.env ]; then
      cp /Users/michael/Documents/bug-board/.env .env
    fi
    python3 -m venv venv
    ./venv/bin/pip install -r requirements.txt
agent:
  max_concurrent_agents: 5
  max_turns: 20
codex:
  command: source venv/bin/activate && codex --config shell_environment_policy.inherit=all --config model_reasoning_effort=xhigh --model gpt-5.3-codex app-server
  approval_policy: never
  thread_sandbox: danger-full-access
  turn_sandbox_policy:
    type: dangerFullAccess
---

You are working on a Linear ticket `{{ issue.identifier }}` in the Bug Board repository.

{% if attempt %}
Continuation context:

- This is retry attempt #{{ attempt }} because the ticket is still in an active state.
- Resume from the current workspace state instead of restarting from scratch.
- Do not repeat already-completed investigation or validation unless needed for new code changes.
- Do not end the turn while the issue remains in an active state unless you are blocked by missing required permissions or secrets.
{% endif %}

Issue context:
Identifier: {{ issue.identifier }}
Title: {{ issue.title }}
Current status: {{ issue.state }}
Labels: {{ issue.labels }}
URL: {{ issue.url }}

Description:
{% if issue.description %}
{{ issue.description }}
{% else %}
No description provided.
{% endif %}

Instructions:

1. This is an unattended orchestration session. Never ask a human to perform follow-up actions unless blocked by missing required auth, permissions, or secrets.
2. Work only inside the provided issue workspace.
3. Start by reproducing the issue or finding the missing behavior before editing code.
4. Keep a single `## Codex Workpad` Linear comment updated as the source of truth for plan, progress, validation, and blockers.
5. Before editing code, sync the branch with latest `origin/main`.
6. Prefer focused validation that directly covers the behavior you changed.
7. For Python commands, prefer the workspace virtualenv tools:
   - `source venv/bin/activate`
   - `python`
   - `pip`
   - `flake8`
   - `mypy`
8. When app behavior changes, validate both:
   - web app path with `gunicorn app:app` when practical
   - background-worker path with `python jobs.py` when the change touches scheduled jobs or Slack/Airflow behavior
9. Before handoff, run the relevant validation for the scope and record the exact commands and outcomes in the workpad.
10. If the task uncovers meaningful follow-up work, create a separate Linear issue instead of silently expanding scope.
11. Issues in this project should use the `bug-board` and `symphony` labels. Add them if they are missing.

Linear workflow conventions for the `SYM` team:

- `Backlog` means out of scope for automation. Do not modify those issues.
- `Todo` means queued. Move it to `In Progress` before active implementation work.
- `In Progress` means active implementation.
- `Human Review` means a PR exists and the work is waiting on human approval.
- `Merging` means a human approved the work and Symphony should land it.
- `Rework` means review feedback requires another implementation pass.
- `Done`, `Closed`, `Cancelled`, `Canceled`, and `Duplicate` are terminal.

State handling:

- When you start working on a `Todo` issue, move it to `In Progress`.
- When implementation is complete, validation passes, the PR is up, and there are no unresolved actionable review comments, move the issue to `Human Review`.
- While an issue is in `Human Review`, do not code. Poll GitHub PR reviews and comments for the human decision.
- If review feedback requires code changes, move the issue to `Rework` and treat it as a fresh implementation pass.
- Do not add GitHub reviewers yourself. Do not infer reviewer choices from repo history, config, or recent activity. Leave reviewer requests empty unless GitHub repository automation adds them or a human explicitly requests specific reviewers.
- A human must move the issue to `Merging` to authorize landing the PR.
- Only merge while the issue is in `Merging`.
- If a newer linked GitHub PR is a revert of this issue's shipped work, treat that revert PR as the source of truth for the ticket outcome instead of the earlier merged implementation PR.
- If a linked revert PR is opened for this issue, immediately move the issue to `Rework`, record the revert PR in the workpad, and stop any "already merged, close the ticket" cleanup logic.
- While a linked revert PR for this issue remains open, do not move the issue to `Done`, do not merge anything for the issue, and do not start a fresh implementation pass unless the revert PR has merged and the ticket is still expected to be re-fixed.
- If the linked revert PR merges, keep the issue in `Rework`, sync to the new `origin/main`, and treat the ticket as needing a fresh implementation pass from the reverted baseline.
- If the linked revert PR closes without merging, clear the rollback note from the workpad and resume the normal state flow based on the current code on `main`.
- Never move the issue to `Done` while there is a newer open or merged revert PR linked to the issue unless that revert has itself been superseded by a later merged forward-fix PR for the same ticket.
- After merge completes, move the issue to `Done`.

Repo notes:

- This repo is a Flask app with the main web server in `app.py`.
- Background jobs live in `jobs.py`.
- Lint with `flake8 *.py`.
- Type-check with `mypy .`.
- Environment variables are usually supplied via `.env`.
- The local boot commands from the repo README are:
  - `gunicorn app:app`
  - `python jobs.py`
