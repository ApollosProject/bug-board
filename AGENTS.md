# Bug Board

Next.js App Router / TypeScript application for Vercel. Read README.md before changing integration behavior.

## Map

- `app/`: server-rendered dashboard pages; route handlers for OAuth, API-key metrics, Cron, CSV, and deployment controls.
- `proxy.ts` / `lib/auth.ts`: fail-closed GitHub membership gate, PKCE/session cookies, API-key validation.
- `lib/`: typed Linear/GitHub/Airflow/BigQuery/store clients, metrics, review queue, attribution, PTO, snapshots, notifications.
- `workflows/refresh.ts`: durable refresh steps and notification scheduling.
- `config.yml` / `regression_overrides.yml`: existing configuration and manual attribution corrections.
- `tests/*.test.ts`: Node's test runner via tsx. `tests/*.e2e.ts`: e2e browser tests, Luna via Vercel AI Gateway.

## Validation

Use Node 24.8+; `npm ci`, `npm run check`, `npm test`, `npm run build`.
Then `npx playwright-core install chromium` and `npx e2e run tests/dashboard.e2e.ts --no-cache`.
Full `npx e2e run --no-cache` requires `AI_GATEWAY_API_KEY`; don't silently replace the provider or skip failed AI steps.
Local no-secrets pages must render unavailable states, not fabricate successful or zero activity.
Proof artifacts, .e2e/, .next/, local Workflow state, service doubles, and credentials must not enter Git.

## Conventions

- Prefer native Server Components, Suspense, URL filters, and forms over client state/data fetching.
- Keep expensive fleet/store/blame work in Workflow steps; those views only read fresh snapshots. Historical delivery windows and reviews use Next.js Data Cache.
- Extend existing YAML/types/helpers. Don't create additional team/config/route sources of truth.
- Only explicitly named team API and Cron routes escape OAuth; their handlers must authenticate.
- Keep health public; all hosted deployments and partially configured OAuth fail closed.
- Store publication is not a successful upload, observed runtime, tag, or dispatch. Preserve native-build matching and unverified/partial evidence states.
- No external deployment, Slack posting, DNS/OAuth change, or production shutdown without explicit operator authorization.
- Read README's Vercel cutover checklist before claiming migration is production-ready.
