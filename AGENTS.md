# AGENTS.md

## Cursor Cloud specific instructions

Bug Board is the Next.js app described in `README.md`. That file is the source for architecture, routes, environment variables, auth, Cron, and deploys.

### Running the application

Use Node.js 24.8+ (`package.json` `engines`; CI uses Node 24).

```bash
npm ci
npm run dev
```

Open `http://localhost:3000`. Without credentials, pages render explicit unavailable states.

### Local verify

```bash
npm run check
npm test
npm run build
```

`npm test` runs `tsx --test tests/*.test.ts`. `/healthz` returns `{"status":"ok"}`.
