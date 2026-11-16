import { test } from "node:test";
import assert from "node:assert/strict";
import { redis, snapshotOnce, writeSnapshot } from "../lib/cache";

test("snapshotOnce retains the first writer and fails closed when storage is unavailable", async (t) => {
  const names = ["UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"];
  const previous = names.map((name) => process.env[name]);
  process.env.UPSTASH_REDIS_REST_URL = "https://redis.example.test";
  process.env.UPSTASH_REDIS_REST_TOKEN = "fixture-redis";
  let saved: string | null = null;
  t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    const command = ([op, , value, ...args]: string[]) => {
      if (op === "set") {
        assert.ok(args.includes("nx"));
        if (saved !== null) return null;
        saved = value;
        return "OK";
      }
      assert.equal(op, "get");
      return saved;
    };
    return Response.json(Array.isArray(body[0])
      ? body.map((c: string[]) => ({ result: command(c) }))
      : { result: command(body) });
  });
  try {
    assert.equal(await snapshotOnce("digest", "first", 60), "first");
    assert.equal(await snapshotOnce("digest", "second", 60), "first");
    t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      return Response.json(Array.isArray(body[0]) ? body.map(() => ({ result: null })) : { result: null });
    });
    await assert.rejects(snapshotOnce("missing", "value", 60), /Saved snapshot unavailable/);
  } finally {
    names.forEach((name, i) => {
      if (previous[i] === undefined) delete process.env[name];
      else process.env[name] = previous[i];
    });
  }
});

test("snapshots over Upstash's request limit are not sent, and smaller ones still cache", async (t) => {
  const names = ["UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"];
  const previous = names.map((name) => process.env[name]);
  process.env.UPSTASH_REDIS_REST_URL = "https://redis.example.test";
  process.env.UPSTASH_REDIS_REST_TOKEN = "fixture-redis";
  const bodies: number[] = [];
  const warnings: unknown[][] = [];
  t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
    bodies.push(new TextEncoder().encode(String(init?.body)).byteLength);
    const body = JSON.parse(String(init?.body));
    const commands = Array.isArray(body[0]) ? body : [body];
    return Response.json(
      Array.isArray(body[0])
        ? commands.map(() => ({ result: "OK" }))
        : { result: "OK" },
    );
  });
  t.mock.method(console, "warn", (...args: unknown[]) => {
    warnings.push(args);
  });
  const requestBytes = (data: unknown) => {
    const command = ["set", "key", JSON.stringify({ at: 0, data }), "ex", 60];
    return new TextEncoder().encode(JSON.stringify([command])).byteLength;
  };
  try {
    await writeSnapshot("metrics", { ok: true }, 60);
    const report = "x".repeat(2_500_000);
    await writeSnapshot("report-v4:days=30", report, 60);
    assert.equal(bodies.length, 2);
    assert.ok(bodies[1] > 2_097_152, "a report past Next's 2MB cache limit still goes to Redis");
    assert.ok(bodies.every((bytes) => bytes <= 9_000_000));
    const oversized = "x".repeat(9_000_000);
    assert.ok(requestBytes(oversized) > 9_000_000);
    await assert.rejects(
      writeSnapshot("linear-histories:open", oversized, 3600),
      /Redis snapshot exceeds the request size limit/,
    );
    await assert.rejects(
      snapshotOnce("notifications", oversized, 3600),
      /Redis snapshot exceeds the request size limit/,
    );
    assert.equal(bodies.length, 2, "an oversized snapshot is not sent to Redis");
    assert.equal(warnings.length, 2);
    for (const warning of warnings) {
      assert.match(String(warning[0]), /exceed Upstash's 10MB limit/);
      assert.ok(Number(warning[2]) > 9_000_000);
    }
  } finally {
    names.forEach((name, i) => {
      if (previous[i] === undefined) delete process.env[name];
      else process.env[name] = previous[i];
    });
  }
});

test("cache accepts Marketplace credentials without mixing partial standalone pairs", async () => {
  const names = [
    "UPSTASH_REDIS_REST_URL",
    "UPSTASH_REDIS_REST_TOKEN",
    "KV_REST_API_URL",
    "KV_REST_API_TOKEN",
  ];
  const env = Object.fromEntries(
    names.map((name) => [name, process.env[name]]),
  );
  const fetch = globalThis.fetch;
  const requests: { url: string; authorization: string | null }[] = [];
  try {
    for (const name of names) delete process.env[name];
    assert.equal(redis(), null);
    await assert.rejects(
      writeSnapshot("contract", {}, 60),
      /Configure Upstash/,
    );
    globalThis.fetch = async (url, init) => {
      requests.push({
        url: String(url),
        authorization: new Headers(init?.headers).get("authorization"),
      });
      const body = JSON.parse(String(init?.body));
      return Response.json(
        Array.isArray(body[0]) ? [{ result: "OK" }] : { result: "OK" },
      );
    };
    process.env.KV_REST_API_URL = "https://marketplace.example.test";
    process.env.KV_REST_API_TOKEN = "fixture-marketplace";
    await writeSnapshot("contract", {}, 60);
    assert.match(requests.at(-1)!.url, /^https:\/\/marketplace\.example\.test/);
    assert.equal(requests.at(-1)!.authorization, "Bearer fixture-marketplace");
    process.env.UPSTASH_REDIS_REST_URL = "https://standalone.example.test";
    assert.equal(
      redis(),
      null,
      "partial standalone configuration must not fall back to another pair",
    );
    await assert.rejects(
      writeSnapshot("contract", {}, 60),
      /Configure Upstash/,
    );
    process.env.UPSTASH_REDIS_REST_TOKEN = "fixture-standalone";
    await writeSnapshot("contract", {}, 60);
    assert.match(requests.at(-1)!.url, /^https:\/\/standalone\.example\.test/);
    assert.equal(requests.at(-1)!.authorization, "Bearer fixture-standalone");
    delete process.env.UPSTASH_REDIS_REST_URL;
    assert.equal(
      redis(),
      null,
      "token-only standalone configuration must also fail closed",
    );
  } finally {
    globalThis.fetch = fetch;
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
  }
});
