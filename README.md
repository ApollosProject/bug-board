# Bug Board

Apollos Engineering's dashboard for Linear work, GitHub delivery and reviews, app releases, project timelines, and Airflow fleet health. Built with Next.js App Router, React Server Components, TypeScript, Vercel Cron, and Vercel Workflow. No Flask server or persistent worker is needed.

## Local development

Use Node.js **24.8+** (Vercel: Node 24.x).

```sh
npm ci
npm run dev
```

Open `http://localhost:3000`. Without credentials, pages render explicit unavailable states. They do not present missing data as zero activity or verified releases. Next.js loads `.env.local`. Never commit credentials.

```sh
npm run check
npm test
npm run build
npm run start
npx playwright-core install chromium
npx e2e run tests/dashboard.e2e.ts --no-cache
```

## Architecture

- `app/`: streamed server-rendered pages and explicit route handlers. Native forms keep filters in the URL. No HTMX, client data-fetching layer, or chart CDN.
- `lib/`: typed integrations, scoring, comparisons, attribution, and snapshot access. `config.yml` remains the single source for people, teams, platforms, and ownership. `regression_overrides.yml` retains manual attribution corrections.
- `lib/refresh.ts`: fleet, metrics, and app refreshes run inside authenticated Cron functions. Fleet lookups retain a limit of 120 simultaneous requests. Store lookups retain a limit of eight.
- `workflows/refresh.ts`: durable regression attribution and notifications. Each blame step receives one file and PR metadata, not every file patch. Fleet/store/blame work never runs in a deployed page request. Other delivery windows and review queues use Next.js Data Cache and streamed server rendering.
- Upstash Redis REST: expiring snapshots and compare-and-delete refresh locks. Production, local, and individual preview deployments have separate namespaces. Expired records cannot establish healthy/current status. Merged-PR date chunks that end before today retain complete approval data for one hour. The chunk that includes today always uses GitHub. Historical metadata corrections can take up to one hour to appear. Complete review timelines cache for one hour, keyed by the PR update timestamp and unfiltered timeline count. A changed marker requires a new individual timeline lookup. CI, approvals, and requested reviewers still use the queue's 60-second refresh policy.
- Vercel Cron: fleet, metrics, and apps return `200` with `status: refreshed` only after completion. Failures return `503`. Regressions and notifications return `202` with a Workflow run ID, not a claim of completion. Inspect durable jobs in Vercel Workflow observability (locally: `npx workflow inspect runs`).

The main views are `/`, `/team`, `/team/[slug]`, `/reviews`, `/projects`, `/apps`, and `/dags`. `/healthz` stays public and returns `{"status":"ok"}`. `/team.csv` exports the selected team/window with cohort z-scores. `/app-versions` and `/failing-dags` redirect to their current pages. Flask's internal `/partials/*` transport is retired. Streamed server components replace it.

### Refresh and notifications

`vercel.json` schedules fleet and team/work snapshots every minute, apps every three minutes, regression attribution every six hours, and notification scheduling hourly. **Minute-level Cron requires a Vercel plan that supports it (normally Pro). Hobby's daily Cron is not sufficient.** There is no second worker to deploy.

| Snapshot | Maximum accepted age |
| --- | --- |
| Fleet, team metrics, projects, open issues | 180 seconds |
| Apps | 300 seconds |
| Regression summary | 24 hours |
| Published store builds | 30 minutes (quota backoff: 1 hour) |

Cron only runs on production deployments. Preview snapshots require manually invoking the authenticated Cron route after configuring a preview-only Redis namespace. Notifications and Better Stack heartbeat delivery are disabled outside `VERCEL_ENV=production`.

Priority bug notifications run at 12:00 UTC. Stale work notifications run at 10:00 New York, and project updates at 14:00 New York. Manager performance outliers run on Friday at 13:00 UTC. New York schedules follow daylight saving time. Weekly leaderboard posting remains unscheduled, as it was previously.

Refresh locks prevent overlapping executions. Ordinary Cron functions have a 300-second limit and a 360-second lease. A timeout cannot block refreshes for eight hours. Failed polls retry on the next Cron tick. Durable jobs retain their eight-hour lease and Workflow retries.

Slack digests have a per-day delivery claim: since Slack webhooks have no idempotency key, an ambiguous network failure retains the claim to prevent duplicate posts. Check Slack delivery and Workflow logs before an operator retries it. Do not blindly clear the claim.

Ready lists include only unassigned projects without unfinished incoming dependencies. A milestone dependency uses milestone status, not the parent project's completion. Dependency pages are complete before a project can appear in Ready.

### Slack handoff (separate operator approval required)

Preview deployments never send notifications. The production delivery claims prevent duplicates between Vercel deployments, but not between Vercel and Heroku.

1. Deploy the production dashboard without `SLACK_WEBHOOK_URL` and `MANAGER_SLACK_WEBHOOK_URL`.
2. Verify the production dashboard, snapshots, and scheduled refreshes before the worker handoff. Budget this short verification overlap from the live GitHub quota, including cold fills. The sampled native/legacy busy-hour forecast exceeds the shared 5,000-point limit. Do not leave both regular refresh schedulers active. If headroom is insufficient, postpone cutover.
3. Choose a handoff time between notification schedules. Record the last Heroku deliveries in each channel.
4. Confirm that no Heroku notification is in flight. Stop the Heroku worker only after approval for the coordinated handoff.
5. Configure the current Slack webhooks on Vercel. Deploy these environment changes before the next notification schedule.
6. Verify one delivery in the intended channel and its completed Workflow step.

Do not run both notification systems concurrently. Do not use Heroku's `DEBUG=true` path or manually replay a digest during the handoff.

Environment changes do not cancel active Workflow runs. For rollback, disable Vercel notifications and confirm that no notification run remains in flight. Then restore the Heroku worker. If a delivery is uncertain, inspect Slack before any retry. Do not clear a delivery claim without this check.

### App release evidence

Mobile and Android TV runtime claims require published store build IDs matched uniquely against Segment native build observations. iOS also requires the exact native version. Queued events with conflicting app/native versions are excluded. Multiple published runtimes remain explicit. Absent credentials, conflicting matches, incomplete store responses, or missing source targets remain unverified. Android uses the read-only release-lifecycle API, **not** `edits.tracks` or a store edit.

Analytics reads retain the event-time lookback and additionally prune `_PARTITIONTIME` only on ingestion-partitioned tables, using the discovered schema (main PR #577). `APP_VERSIONS_PARTITION_BUFFER_DAYS` defaults to 3 extra days to cover device-clock skew. It is bound only when a branch can prune. Mixed unions and non-ingestion-partitioned tables remain supported.

Non-mobile platforms show neutral **Observed** comparisons, never verified store publication. Cluster's directory resolves deployment identity by platform/bundle. Ambiguous identities are not deployable.

Deployment POSTs require a verified GitHub session, same-origin request, a fresh uniquely identified app row, and `GITHUB_ACTIONS_TOKEN`. They dispatch the latest stable Platforms tag. The existing workflow still checks production readiness. A dispatch is not evidence of a published build.

### Regression evidence

The durable attribution refresh traces removed lines in fixing PRs to likely inducing PRs, ranks them by recency-weighted line counts, and records human author/approval attribution. Work is bounded at 50 files and 500 deleted lines per file. Exceeding either marks evidence incomplete. Manual overrides can ignore a Linear issue or select an inducing PR. Attribution is directional, not proof of causality. Regression cards always use their labeled 30-day window, independently of the page's selected delivery window.

## Environment

See `.env.example` for names and defaults. Set server credentials in the appropriate Vercel environment. None are `NEXT_PUBLIC_*`.

**Required for a production dashboard:**

- `APP_URL`: canonical public origin.
- `GITHUB_OAUTH_ENABLED=true`, `GITHUB_OAUTH_CLIENT_ID`, `GITHUB_OAUTH_CLIENT_SECRET`, `AUTH_SECRET` (at least 32 random characters).
- `GITHUB_OAUTH_CALLBACK_URL`: defaults to `APP_URL/auth/github/callback`.
- `GITHUB_OAUTH_ORG`: defaults to `ApollosProject`.
- `LINEAR_API_KEY`, `GITHUB_TOKEN`: read access for the configured team and tracked repositories.
- Redis REST credentials: Vercel Marketplace supplies `KV_REST_API_URL` / `KV_REST_API_TOKEN` with the `KV` prefix. Standalone Upstash supplies `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN`. The standalone pair takes precedence, and a partial pair fails closed rather than mixing credentials. Connect the preview database to **Preview only**, using sensitive variables.
- `CRON_SECRET`: bearer secret for `/api/cron/{fleet,metrics,apps,regressions,notifications}`. Vercel adds it to scheduled requests.

GitHub OAuth requests `read:org`, validates active membership, and uses PKCE with a ten-minute signed state cookie. It keeps the verified identity in a signed HTTP-only session for at most 30 days, not the access token.

Partially configured auth fails closed with `503`. Vercel production and preview deployments require auth even if `GITHUB_OAUTH_ENABLED` was omitted. Hosted callback URLs must use HTTPS. Sessions and authenticated responses are private/no-store. The no-auth local development path never enables deployment.

**Optional features:**

- `BUG_BOARD_API_KEY`: enables `/api/team/[slug]` independently of OAuth.
- `AIRFLOW_API_BASE_URL`, `AIRFLOW_API_TOKEN`, `AIRFLOW_FLEET_HEARTBEAT_URL`.
- `BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64`, `BIGQUERY_ANALYTICS_PROJECT_ID`, `BIGQUERY_ANALYTICS_DATASETS`, `BIGQUERY_ANALYTICS_TABLES`, `APP_VERSIONS_LOOKBACK_DAYS`, `APP_VERSIONS_PARTITION_BUFFER_DAYS`, `APP_VERSIONS_LIMIT`. Explicit service-account credentials are required. No ADC fallback.
- `APOLLOS_API_KEY`: read-only Cluster/store verification. Store credentials remain within the refresh function. Snapshots and browser responses never contain these credentials.
- `GITHUB_ACTIONS_TOKEN`, `GITHUB_DEPLOY_WORKFLOW_ID` (default `173574865`).
- `SLACK_WEBHOOK_URL`, `MANAGER_SLACK_WEBHOOK_URL`.
- `RIPPLING_PTO_CALENDAR_URL`, `RIPPLING_PTO_TIMEZONE` (default `America/New_York`). Only Rippling's HTTPS PTO feed is accepted. No redirects.

Legacy `FLASK_SECRET_KEY`, `REDIS_URL`, Redis TCP/TLS settings, and worker interval variables are not used. Generate a new `AUTH_SECRET`. Old Flask sessions intentionally do not survive migration.

## JSON API

```sh
curl -H "Authorization: Bearer $BUG_BOARD_API_KEY" \
  "$APP_URL/api/team/zach?start=2026-08-31&end=2026-09-25"
```

`X-API-Key` is also accepted. No configured key returns `503`. A bad or missing key returns `401`. An unknown person returns `404`. Unavailable integration data returns `503`.

The person, window, metrics (`label`, `value`, `display`, `vs_team`), regressions, and links retain their existing shape. Positive z-scores mean better than the engineering baseline even when lower values are better. API responses are no-store. Date ranges are inclusive UTC calendar days. Reversed dates are normalized, and windows are bounded to 366 days.

Only the named team API and Cron routes escape OAuth. Each has its own authentication. New `/api/` paths do not gain an automatic auth exemption.

## E2E with Luna and AI Gateway

`e2e.config.ts` uses **Vercel AI Gateway** and defaults to **`openai/gpt-6-luna`**, with no provider fallback. Deterministic tests do not call a model. AI tests make actual Gateway calls and fail when credentials are unavailable. They are not silently skipped.

```sh
# Put AI_GATEWAY_API_KEY in .env.local or export it without logging its value.
npm run build
npx e2e run --no-cache

# Deterministic surface only (also run in CI):
npx e2e run tests/dashboard.e2e.ts --no-cache

# Already-running, populated local surface:
E2E_BASE_URL=http://127.0.0.1:3000 E2E_DATA=1 \
  npx e2e run tests/data.e2e.ts --no-cache
```

`E2E_MODEL` can override the Gateway model ID. The config explicitly loads Next's env files. `E2E_BASE_URL` selects an existing surface instead of starting a local production build. Dashboard tests expect access to the dashboard. They do not bypass GitHub auth or Vercel deployment protection.

AI instructions prohibit deployment and authentication actions. Keep recordings private when testing internal data.

Tests, traces, screenshots, videos, reports, and local Workflow state stay outside Git. `.e2e/` is ignored. CI runs type/lint checks, domain/security tests, a production build, and deterministic browser tests.

The manual `run_ai` input runs the Luna suite with the repository's `AI_GATEWAY_API_KEY` secret. The `e2e` label enables Luna on same-repository PRs, with the same key and budget limit. TesterArmy's native `@e2e-dev/github` reporter posts results, model usage, and artifact links as PR comments. Reruns update those comments.

Manual runs write job summaries because they have no PR event context. Repository rules still require the legacy Python check names. The workflow retains these names as explicit gates on native validation, not as Python tool runs.

## Vercel cutover (operator approval required)

1. Create/link the **Apollos** Vercel project with the Next.js preset, Node 24.x, and a plan supporting minute-level Cron and Workflow. Vercel assigns a new project’s first deployment to **production**, even with CLI `--target preview`. Do not assume the flag isolates that first deployment. Obtain separate approval for a harmless protected static bootstrap with no domain promotion, app credentials, or Cron. Then create the real preview and verify its actual API target. Keep Git auto-deployment disconnected until production deployment is authorized. The Heroku deployment still uses Flask.
2. Configure server credentials and Upstash Redis REST access separately for preview and production. Do not copy a legacy Redis TCP URL into the REST variable.
3. Configure a GitHub OAuth app/callback for the preview host. Confirm fail-closed behavior, active-member login, API-key access, and logout on the preview.
4. Invoke authenticated preview refreshes for fleet, metrics, apps, and regressions. Confirm `200` responses for ordinary refreshes. Wait for the regression Workflow to complete. Compare real data against the current dashboard, including store build evidence, regression overrides, PTO, CSV, and person comparisons. Run the populated E2E suite.
5. Add `AI_GATEWAY_API_KEY` and run the uncached Luna tests. Review all CI checks and attached runtime proof.
6. Only after approval, set the production origin/OAuth callback and move `engineering.apollos.app` to Vercel. Verify the production dashboard and refresh jobs without Slack webhooks.
7. Follow the separate Slack handoff procedure above. Coordinate fleet heartbeat ownership with the worker handoff. Do not stop the legacy worker before the new dashboard and refresh jobs are ready.

This repository change does not itself provision infrastructure, modify DNS/OAuth apps, send Slack posts, or shut down production services.
