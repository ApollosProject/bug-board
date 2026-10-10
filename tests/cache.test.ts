import { test } from "node:test";
import assert from "node:assert/strict";
import { redis, writeSnapshot } from "../lib/cache";

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
