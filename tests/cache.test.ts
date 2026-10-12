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
