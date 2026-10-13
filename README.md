# Bug Board

Apollos Engineering's dashboard for Linear work, GitHub delivery and reviews, app releases, project timelines, and Airflow fleet health. Built with Next.js App Router, React Server Components, TypeScript, Vercel Cron, and Vercel Workflow. No persistent worker is needed.

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
- `lib/refresh.ts`: fleet, metrics, and app refreshes run inside authenticated Cron functions. Fleet lookups retain a limit of 120 simultaneous requests and retry each failed lookup once; every Airflow request, from inventory onward, must finish within 230 seconds of the job start, and lookups past that deadline count as unchecked; health is `unknown` when more than 10% of active DAGs cannot be checked. Store lookups retain a limit of eight.
- `workflows/refresh.ts`: durable regression attribution and notifications. Each blame step receives one file and PR metadata, not every file patch. Fleet/store/blame work never runs in a deployed page request. Other delivery windows use Redis snapshots when configured; review queues use Next.js Data Cache. Pages stream server-rendered sections.
- Upstash Redis REST: expiring snapshots and compare-and-delete refresh locks. Production, local, and individual preview deployments have separate namespaces. Expired records cannot establish healthy/current status. Merged-PR date chunks that end before today retain complete approval data for one hour. The chunk that includes today always uses GitHub. Historical metadata corrections can take up to one hour to appear. Complete review timelines cache for one hour, keyed by the PR update timestamp and unfiltered timeline count. A changed marker requires a new individual timeline lookup. CI, approvals, and requested reviewers still use the queue's 60-second refresh policy.
- Vercel Web Analytics: page views from signed-in people (URLs keep date, platform, and sort filters; person and church slugs become `[slug]`, and the `q`, `author`, and `reviewer` values become `[redacted]`), plus one server-side event, `Team CSV export` (window). Deploy requests show up as `/apps?deployed=1` page views. No names, issue titles, or church data are sent. Collection starts only after Web Analytics is enabled on the Vercel project. Pro bills $0.03 per 1,000 events.
- Vercel Cron: fleet, metrics, and apps return `200` with `status: refreshed` only after completion. Failures return `503`. Regressions and notifications return `202` with a Workflow run ID, not a claim of completion. Inspect durable jobs in Vercel Workflow observability (locally: `npx workflow inspect runs`).

The main views are `/`, `/team`, `/team/[slug]`, `/reviews`, `/projects`, `/apps`, and `/dags`.

`/team` also shows the shipping pipeline: elapsed calendar time in each Linear status (median/p85 for issues completed in the selected window, summing repeat visits per issue), current queue counts and the five oldest items per status, first submitted human-review wait for merged PRs, and the ten oldest approved-but-unmerged PRs. Linear includes project, support, and unassigned issues; assigned issues and PR authors follow the engineering/everyone cohort filter. Current queues are independent of the historical window. Missing or inconsistent histories remain unknown, and sample coverage is shown. Review time starts at the first ready-for-review event, or creation for initially non-draft PRs; author/bot reviews and ordinary conversation comments do not count. Approval aging starts at the latest current approval, not a review request. These are elapsed status times, not hands-on effort or proof of production shipping.

Complete Linear assignment/status histories cache for one hour per issue update timestamp, with separate open/completed caches. Cold histories load in batches of 25 with at most four concurrent requests; later history pages are fully fetched before caching. The default historical view reuses the metrics Cron snapshot; other reports use a 60-second Redis cache when configured (real reports exceed Next's 2 MB Data Cache limit). Reports retain compact status totals and assignment events; open snapshots retain the current status-entry timestamp instead of a full timeline, so ages keep advancing between refreshes. First-review pagination stops at the first human response and is only extended for configured authors. The current PR section uses the existing 60-second review-queue cache policy, searches only approved PRs, and skips review-request timeline lookups. Tables bound oldest-item drilldowns rather than rendering the entire backlog.

`/healthz` stays public and returns `{"status":"ok"}`. `/team.csv` exports the selected team/window with cohort z-scores. `/app-versions` and `/failing-dags` redirect to their current pages.

### Refresh and notifications

`vercel.json` schedules fleet and team/work snapshots every minute, apps every three minutes, regression attribution every six hours, and notification scheduling hourly. **Minute-level Cron requires a Vercel plan that supports it (normally Pro). Hobby's daily Cron is not sufficient.** There is no second worker to deploy.

| Snapshot | Maximum accepted age |
| --- | --- |
| Fleet, team metrics, projects, open issues | 180 seconds |
| Apps | 300 seconds |
| Regression summary | 24 hours |
| Published store builds | 30 minutes (quota backoff: 1 hour) |

Cron only runs on production deployments. Preview team metrics, open work, and projects load on demand through deployment-scoped 60-second caches when fresh snapshots are absent; their first load can take a few minutes. They never read production's Redis namespace. Fleet, apps, PTO, and regressions still require their authenticated refresh routes and fresh preview snapshots; fleet/store/blame work does not run in page requests. Notifications and Better Stack heartbeat delivery are disabled outside `VERCEL_ENV=production`.

Priority bug notifications run at 12:00 UTC. Stale work notifications run at 10:00 New York, and project updates at 14:00 New York. Manager performance outliers run on Friday at 13:00 UTC. New York schedules follow daylight saving time. Weekly leaderboard posting remains unscheduled, as it was previously.

Refresh locks prevent overlapping executions. Ordinary Cron functions have a 300-second limit and a 360-second lease. A timeout cannot block refreshes for eight hours. Failed polls retry on the next Cron tick. Durable jobs retain their eight-hour lease and Workflow retries.

Slack digests have a per-day delivery claim: since Slack webhooks have no idempotency key, an ambiguous network failure retains the claim to prevent duplicate posts. Check Slack delivery and Workflow logs before an operator retries it. Do not blindly clear the claim.

Ready lists include only unassigned projects without unfinished incoming dependencies. A milestone dependency uses milestone status, not the parent project's completion. Dependency pages are complete before a project can appear in Ready.

### Slack notifications

Only Production sends Slack posts. Preview deployments never do. Long digests split between lines into posts of at most 39,000 characters to stay below Slack's 40,000-character truncation limit, with at least one second between successive posts. The day's digest is frozen in Redis so refreshes cannot change retry boundaries. Each part has its own delivery claim to prevent duplicate posts across Production deployments; an explicitly rejected part can retry without resending delivered parts. If a delivery is uncertain, check Slack before any retry, and do not clear a delivery claim without that check.

### App release evidence

Mobile and Android TV runtime claims require published store build IDs matched uniquely against Segment native build observations. iOS also requires the exact native version. Queued events with conflicting app/native versions are excluded. Multiple published runtimes remain explicit. Absent credentials, conflicting matches, incomplete store responses, or missing source targets remain unverified. Android uses the read-only release-lifecycle API, **not** `edits.tracks` or a store edit.

Analytics reads retain the event-time lookback and additionally prune `_PARTITIONTIME` only on ingestion-partitioned tables, using the discovered schema (main PR #577). `APP_VERSIONS_PARTITION_BUFFER_DAYS` defaults to 3 extra days to cover device-clock skew. It is bound only when a branch can prune. Mixed unions and non-ingestion-partitioned tables remain supported.

Non-mobile platforms show neutral **Observed** comparisons, never verified store publication. Cluster's directory resolves deployment identity by platform/bundle. Ambiguous identities are not deployable.

Deployment POSTs require a verified GitHub session, same-origin request, a fresh uniquely identified app row, and `GITHUB_ACTIONS_TOKEN`. They dispatch the latest stable Platforms tag. The existing workflow still checks production readiness. A dispatch is not evidence of a published build.

### Regression evidence

The durable attribution refresh traces removed lines in fixing PRs to likely inducing PRs, ranks them by recency-weighted line counts, and records human author/approval attribution. Work is bounded at 50 files and 500 deleted lines per file. Exceeding either marks evidence incomplete. Each issue is then judged automatically (`lib/attribution.ts`), with no human labels. Luna (`openai/gpt-6-luna`) writes a mechanism hypothesis and up to three GitHub searches for suspects blame cannot see, such as dependency upgrades or cross-repo changes. JEV (`typesafe-ai/jev`) decides whether a merged change introduced the bug, from the issue, the fixing diff, and each suspect's title and the hunks where it added lines the fix later removed. A fix that JEV is at least 50% sure no change introduced leaves the counts. A PR is blamed only when JEV's pick and Luna's independent pick agree. Otherwise the bug counts with nobody blamed. Judgements are cached for 45 days per issue and evidence. A run judges at most 40 new issues, at about $0.002 each, through AI Gateway (OIDC on Vercel, `AI_GATEWAY_API_KEY` locally) with zero data retention and no prompt training. Any failure keeps the SZZ result, and the next run retries. Issues with incomplete blame evidence keep the SZZ result. On 98 agent-labeled cases, 38 of 58 blames were right (20 wrong), compared with SZZ's 36 of 87 (51 wrong). Manual overrides still win, but none are needed. Attribution is directional, not proof of causality. The dashboard calls these introduced bugs: every fixed priority bug counts, whether it broke working behavior or shipped with new code. Regression cards always use their labeled 30-day window, independently of the page's selected delivery window.

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

Partially configured auth fails closed with `503`. Vercel production/preview deployments and Heroku review apps require auth even if `GITHUB_OAUTH_ENABLED` was omitted. Hosted callback URLs must use HTTPS. Sessions and authenticated responses are private/no-store. The no-auth local development path never enables deployment.

**Optional features:**

- `BUG_BOARD_API_KEY`: enables `/api/team/[slug]` independently of OAuth.
- `AIRFLOW_API_BASE_URL`, `AIRFLOW_API_TOKEN`, `AIRFLOW_FLEET_HEARTBEAT_URL`.
- `BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64`, `BIGQUERY_ANALYTICS_PROJECT_ID`, `BIGQUERY_ANALYTICS_DATASETS`, `BIGQUERY_ANALYTICS_TABLES`, `APP_VERSIONS_LOOKBACK_DAYS`, `APP_VERSIONS_PARTITION_BUFFER_DAYS`, `APP_VERSIONS_LIMIT`. Explicit service-account credentials are required. No ADC fallback.
- `APOLLOS_API_KEY`: read-only Cluster/store verification. Store credentials remain within the refresh function. Snapshots and browser responses never contain these credentials.
- `GITHUB_ACTIONS_TOKEN`, `GITHUB_DEPLOY_WORKFLOW_ID` (default `173574865`).
- `SLACK_WEBHOOK_URL`, `MANAGER_SLACK_WEBHOOK_URL`.
- `RIPPLING_PTO_CALENDAR_URL`, `RIPPLING_PTO_TIMEZONE` (default `America/New_York`). Only Rippling's HTTPS PTO feed is accepted. No redirects.


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

Manual runs write job summaries because they have no PR event context.

## Deploys and rollback

Vercel's Git integration deploys `main` to Production (`engineering.apollos.app`), and every pull request gets a Preview deployment. Cron runs only on Production. Preview team views can read through their own caches; other snapshot views need an explicit refresh. Preview deployments use their own Redis namespace and never post to Slack.

To roll back, promote an earlier Production deployment in Vercel (Instant Rollback), then confirm that the Cron jobs point at it. The Heroku production app, worker, and Redis add-on were retired after the migration. Automatic Heroku review-app creation is disabled; old review apps are not a supported preview surface and require separate operator cleanup.
