import { test } from "node:test";
import assert from "node:assert/strict";
import { NextRequest } from "next/server";
import {
  apiKeyError,
  appOrigin,
  authConfigured,
  COOKIE,
  FLOW_COOKIE,
  equal,
  pkce,
  safeNext,
  sameOrigin,
  session,
  sign,
  verify,
} from "../lib/auth";
import { proxy } from "../proxy";
import { GET as login } from "../app/login/route";
import { GET as callback } from "../app/auth/github/callback/route";
import { POST as logout } from "../app/logout/route";
test("API requires configuration and accepts both constant-time key headers", () => {
  const original = process.env.BUG_BOARD_API_KEY;
  try {
    delete process.env.BUG_BOARD_API_KEY;
    assert.equal(
      apiKeyError(new Request("http://localhost/api/team/zach"))?.status,
      503,
    );
    process.env.BUG_BOARD_API_KEY = "local-test-key";
    assert.equal(
      apiKeyError(new Request("http://localhost/api/team/zach"))?.status,
      401,
    );
    for (const headers of [
      new Headers({ Authorization: "Bearer local-test-key" }),
      new Headers({ "X-API-Key": "local-test-key" }),
    ])
      assert.equal(
        apiKeyError(new Request("http://localhost/api/team/zach", { headers })),
        undefined,
      );
    assert.equal(equal("same", "same"), true);
    assert.equal(equal("same", "diff"), false);
    assert.equal(equal("same", "longer"), false);
  } finally {
    if (original === undefined) delete process.env.BUG_BOARD_API_KEY;
    else process.env.BUG_BOARD_API_KEY = original;
  }
});
test("OAuth fails closed, protects unknown APIs and resources, and separates cookie audiences", async () => {
  const values = {
    VERCEL_ENV: "development",
    GITHUB_OAUTH_CALLBACK_URL: "",
    GITHUB_OAUTH_ENABLED: "true",
    GITHUB_OAUTH_CLIENT_ID: "local-client",
    GITHUB_OAUTH_CLIENT_SECRET: "local-secret",
    AUTH_SECRET: "a".repeat(40),
    APP_URL: "http://localhost:3000",
    GITHUB_OAUTH_ORG: "ApollosProject",
  };
  const old = Object.fromEntries(
    Object.keys(values).map((key) => [key, process.env[key]]),
  );
  try {
    Object.assign(process.env, values);
    assert.ok(authConfigured());
    assert.equal(
      sameOrigin(
        new Request("http://internal-host/team", {
          headers: { Origin: values.APP_URL },
        }),
      ),
      true,
    );
    assert.equal(
      sameOrigin(
        new Request("http://internal-host/team", {
          headers: { Origin: "https://evil.example" },
        }),
      ),
      false,
    );
    assert.equal(sameOrigin(new Request("http://internal-host/team")), false);
    const token = await sign(
      { login: "redreceipt", userId: 1, org: "ApollosProject" },
      60,
      "dashboard",
    );
    assert.ok(await session(token));
    assert.equal(await verify(token, "oauth"), null);
    assert.equal(await session(token + "tampered"), null);
    assert.equal(
      await session(
        await sign(
          { login: "redreceipt", userId: 1, org: "other" },
          60,
          "dashboard",
        ),
      ),
      null,
    );
    const authenticated = await proxy(
      new NextRequest("http://localhost:3000/team", {
        headers: { Cookie: `${COOKIE}=${token}` },
      }),
    );
    assert.equal(authenticated.headers.get("x-middleware-next"), "1");
    assert.equal(
      authenticated.headers.get("cache-control"),
      "private, no-store",
    );
    for (const path of [
      "/team",
      "/api/new-route",
      "/static/resources/engineering-expectations.pdf",
    ])
      assert.ok(
        (await proxy(new NextRequest(`http://localhost:3000${path}`))).headers
          .get("x-middleware-rewrite")
          ?.includes("/signin"),
      );
    for (const path of ["/api/team/zach", "/api/cron/metrics", "/healthz"])
      assert.equal(
        (
          await proxy(new NextRequest(`http://localhost:3000${path}`))
        ).headers.get("x-middleware-next"),
        "1",
      );
    process.env.VERCEL_ENV = "production";
    process.env.GITHUB_OAUTH_ENABLED = "false";
    assert.equal(authConfigured(), false);
    assert.equal(
      (await proxy(new NextRequest("http://localhost:3000/team"))).status,
      503,
    );
    process.env.VERCEL_ENV = "preview";
    assert.equal(
      (await proxy(new NextRequest("http://localhost:3000/team"))).status,
      503,
    );
    process.env.APP_URL = "https://engineering.example";
    assert.equal(authConfigured(), true);
    delete process.env.AUTH_SECRET;
    assert.equal(
      (await proxy(new NextRequest("http://localhost:3000/team"))).status,
      503,
    );
    assert.equal(
      (await proxy(new NextRequest("http://localhost:3000/healthz"))).status,
      200,
    );
  } finally {
    for (const [key, value] of Object.entries(old))
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
  }
});
test("Heroku review apps fail closed even when OAuth was not configured", async () => {
  const names = ["VERCEL_ENV", "HEROKU_APP_NAME", "DYNO", "GITHUB_OAUTH_ENABLED", "GITHUB_OAUTH_CLIENT_ID", "GITHUB_OAUTH_CLIENT_SECRET", "GITHUB_OAUTH_CALLBACK_URL", "APP_URL", "AUTH_SECRET"];
  const old = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  try {
    for (const name of names) delete process.env[name];
    process.env.HEROKU_APP_NAME = "review-app";
    assert.equal((await proxy(new NextRequest("https://review-app.herokuapp.com/team"))).status, 503);
    // DYNO is always present on Heroku; app-name metadata is optional.
    delete process.env.HEROKU_APP_NAME;
    process.env.DYNO = "web.1";
    assert.equal((await proxy(new NextRequest("https://review-app.herokuapp.com/team"))).status, 503);
    Object.assign(process.env, { GITHUB_OAUTH_CLIENT_ID: "test-client", GITHUB_OAUTH_CLIENT_SECRET: "test-secret", AUTH_SECRET: "a".repeat(40), APP_URL: "http://localhost:3000" });
    assert.equal(authConfigured(), false);
    process.env.APP_URL = "https://review-app.herokuapp.com";
    assert.equal(authConfigured(), true);
    assert.match((await proxy(new NextRequest("https://review-app.herokuapp.com/team"))).headers.get("x-middleware-rewrite") || "", /\/signin/);
  } finally {
    for (const [name, value] of Object.entries(old))
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
  }
});
test("preview callback relays only signed origins and exchanges codes with the initiating cookie", async () => {
  const values = { VERCEL_ENV: "preview", VERCEL_URL: "unique-preview.vercel.app", VERCEL_BRANCH_URL: "branch-preview.vercel.app", APP_URL: "https://preview.example", GITHUB_OAUTH_CALLBACK_URL: "https://preview.example/auth/github/callback", GITHUB_OAUTH_CLIENT_ID: "test-client", GITHUB_OAUTH_CLIENT_SECRET: "test-secret", AUTH_SECRET: "a".repeat(40), GITHUB_OAUTH_ORG: "ApollosProject" };
  const old = Object.fromEntries(Object.keys(values).map(key => [key, process.env[key]]));
  const oldFetch = globalThis.fetch;
  try {
    Object.assign(process.env, values);
    const origin = "https://unique-preview.vercel.app";
    const start = await login(new NextRequest(`${origin}/login?next=/team?days=7`));
    const auth = new URL(start.headers.get("location")!);
    const state = auth.searchParams.get("state")!;
    assert.equal((await verify(state, "oauth-preview"))?.origin, origin);
    assert.equal(auth.searchParams.get("redirect_uri"), values.GITHUB_OAUTH_CALLBACK_URL);
    const cookie = start.cookies.get(FLOW_COOKIE)!;
    const flow = (await verify(cookie.value, "oauth"))!;
    assert.equal(auth.searchParams.get("code_challenge"), pkce(flow.verifier as string));
    assert.equal(start.cookies.get(FLOW_COOKIE)?.domain, undefined);
    assert.equal(start.cookies.get(FLOW_COOKIE)?.secure, true);
    let calls = 0;
    globalThis.fetch = async (input, init) => {
      calls++;
      const url = String(input);
      if (url.endsWith("/access_token")) {
        const body = JSON.parse(String(init?.body));
        assert.equal(body.code_verifier, flow.verifier);
        assert.equal(body.redirect_uri, values.GITHUB_OAUTH_CALLBACK_URL);
        return Response.json({ access_token: "fixture-token" });
      }
      if (url.endsWith("/user")) return Response.json({ login: "fixture-user", id: 42 });
      if (url.includes("/memberships/")) return Response.json({ state: "active" });
      throw new Error(`Unexpected fetch ${url}`);
    };
    const broker = await callback(new NextRequest(`${values.GITHUB_OAUTH_CALLBACK_URL}?code=fixture-code&state=${state}`));
    const destination = broker.headers.get("location")!;
    assert.equal(new URL(destination).origin, origin);
    assert.equal(broker.cookies.get(COOKIE), undefined);
    assert.equal(broker.headers.get("cache-control"), "no-store");
    assert.equal(calls, 0);
    assert.equal((await callback(new NextRequest(destination))).status, 400);
    const finish = await callback(new NextRequest(destination, { headers: { Cookie: `${FLOW_COOKIE}=${cookie.value}` } }));
    assert.equal(finish.headers.get("location"), `${origin}/team?days=7`);
    assert.equal((await session(finish.cookies.get(COOKIE)?.value))?.userId, 42);
    assert.equal(calls, 3);
    for (const bad of [state + "tampered", await sign({ origin }, -1, "oauth-preview"), await sign({ origin }, 60, "dashboard"), await sign({ origin: "https://evil.example/path" }, 60, "oauth-preview")])
      assert.equal((await callback(new NextRequest(`${values.GITHUB_OAUTH_CALLBACK_URL}?code=fixture-code&state=${bad}`))).status, 400);
    assert.equal((await login(new NextRequest("https://evil.example/login"))).status, 400);
    assert.equal(appOrigin(new Request("https://branch-preview.vercel.app/team")), "https://branch-preview.vercel.app");
    assert.equal(sameOrigin(new Request(`${origin}/logout`, { headers: { Origin: "https://evil.example" } })), false);
    assert.equal((await logout(new NextRequest(`${origin}/logout`, { headers: { Origin: origin } }))).headers.get("location"), `${origin}/signin`);
    process.env.VERCEL_ENV = "production";
    assert.equal(appOrigin(new Request(`${origin}/team`)), values.APP_URL);
    assert.equal(await verify(new URL((await login(new NextRequest(`${origin}/login`))).headers.get("location")!).searchParams.get("state")!, "oauth-preview"), null);
  } finally {
    globalThis.fetch = oldFetch;
    for (const [key, value] of Object.entries(old))
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
  }
});
test("return URLs reject open redirects and PKCE uses SHA256", () => {
  for (const value of [
    "https://evil.example",
    "//evil.example",
    "/\\evil.example",
    "/\r\nevil",
  ])
    assert.equal(safeNext(value), "/");
  assert.equal(safeNext("/team?days=7#fragment"), "/team?days=7");
  assert.equal(
    pkce("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
    "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
  );
});
