import { test } from "node:test";
import assert from "node:assert/strict";
import { NextRequest } from "next/server";
import {
  apiKeyError,
  authConfigured,
  COOKIE,
  equal,
  pkce,
  safeNext,
  sameOrigin,
  session,
  sign,
  verify,
} from "../lib/auth";
import { proxy } from "../proxy";
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
