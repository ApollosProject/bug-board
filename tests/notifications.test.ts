import { test } from "node:test";
import assert from "node:assert/strict";
import { notify, staleMessage } from "../lib/notifications";
import type { Issue } from "../lib/types";

const issue = (index: number, title = `Stale issue ${index}`): Issue => ({
  id: String(index),
  identifier: `APO-${index}`,
  number: index,
  title,
  url: `https://linear.app/differential/issue/APO-${index}`,
  priority: 3,
  createdAt: "2026-01-01T00:00:00Z",
  updatedAt: "2026-01-01T00:00:00Z",
  assignee: { displayName: "zach", name: "zach" },
  project: null,
  state: { name: "Todo", type: "unstarted" },
  labels: { nodes: [] },
});

test("Slack digests preserve complete lines and claim delivery per part", async (t) => {
  const env = {
    VERCEL_ENV: "preview",
    UPSTASH_REDIS_REST_URL: "https://redis.example.test",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis",
    SLACK_WEBHOOK_URL: "https://slack.example.test",
    APP_URL: "https://board.example.test",
  };
  const previous = Object.fromEntries(Object.keys(env).map((key) => [key, process.env[key]]));
  Object.assign(process.env, env);
  const claims = new Map<string, string>();
  const snapshots = new Map<string, string>();
  const posts: string[] = [];
  const postedAt: number[] = [];
  const claimedAt: number[] = [];
  let issues: Issue[] = [];
  let failure: "rejected" | "ambiguous" | undefined;
  let failAt = 0;
  t.mock.method(globalThis, "fetch", async (url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (String(url) === env.SLACK_WEBHOOK_URL) {
      posts.push(body.text);
      postedAt.push(Date.now());
      if (posts.length === failAt) {
        if (failure === "ambiguous") throw new TypeError("fetch failed");
        if (failure === "rejected") return new Response("invalid_payload", { status: 400 });
      }
      return new Response("ok");
    }
    assert.ok(String(url).startsWith(env.UPSTASH_REDIS_REST_URL));
    const command = ([op, key, ...args]: string[]) => {
      const records = key.includes(":notification-message:") ? snapshots : claims;
      if (op === "get") return key.endsWith(":open-issues")
        ? JSON.stringify({ at: Date.now(), data: issues })
        : records.get(key) ?? null;
      if (op === "set") {
        if (records.has(key)) return null;
        records.set(key, args[0]);
        if (records === claims) claimedAt.push(Date.now());
        return "OK";
      }
      if (op === "eval") {
        if (claims.get(args[1]) === args[2]) return Number(claims.delete(args[1]));
        return 0;
      }
      throw new Error(`Unexpected Redis command: ${op}`);
    };
    return Response.json(Array.isArray(body[0])
      ? body.map((c: string[]) => ({ result: command(c) }))
      : { result: command(body) });
  });
  try {
    await t.test("empty and short digests retain existing behavior and claim key", async () => {
      assert.deepEqual(await notify("stale", "short"), { status: "empty" });
      assert.equal(claims.size, 0);
      issues = [issue(1, "Fix <tag> & pipes | with emoji 🚀")];
      const message = await staleMessage();
      assert.deepEqual(await notify("stale", "short"), { status: "sent" });
      assert.deepEqual(posts, [message]);
      assert.ok([...claims.keys()].some((key) => key.endsWith(":notification:stale:short")));
      assert.deepEqual(await notify("stale", "short"), { status: "already-claimed" });
      assert.equal(posts.length, 1);
    });
    await t.test("large digests deliver every issue and the footer without breaking links", async () => {
      posts.length = 0;
      postedAt.length = 0;
      claimedAt.length = 0;
      issues = Array.from({ length: 1000 }, (_, i) => issue(i, `Issue ${i}: ${"🚀 <tag> & long title ".repeat(4)}`));
      const message = (await staleMessage())!;
      assert.ok(message.length > 80_000);
      assert.deepEqual(await notify("stale", "large"), { status: "sent" });
      assert.ok(posts.length >= 3);
      assert.ok(posts.every((text) => text.length <= 39_000));
      assert.ok(postedAt.slice(1).every((at, i) => at - postedAt[i] >= 1_000), "Posts respect Slack's one-message-per-second limit");
      assert.ok(claimedAt.slice(1).every((at, i) => at - postedAt[i] >= 1_000), "Parts are claimed only after the pacing wait");
      assert.equal(posts.join("\n"), message);
      for (const text of posts)
        for (const line of text.split("\n").filter((line) => line.startsWith("- ")))
          assert.match(line, /^- <https:\/\/linear\.app\/[^|]+\|[^>]+> \(\d+d\)$/);
      assert.match(posts.at(-1)!, /<https:\/\/board\.example\.test\|View Bug Board>$/);
      const count = posts.length;
      assert.deepEqual(await notify("stale", "large"), { status: "already-claimed" });
      assert.equal(posts.length, count);
    });
    await t.test("rejection retries only the failed part from the frozen digest despite refreshed data", async () => {
      posts.length = 0;
      failure = "rejected";
      failAt = 2;
      const message = await staleMessage();
      await assert.rejects(notify("stale", "rejected"), /HTTP 400/);
      const first = posts[0], rejected = posts[1];
      assert.equal([...claims.keys()].filter((key) => key.includes(":stale:rejected")).length, 1);
      failure = undefined;
      posts.length = 0;
      const original = issues;
      issues = [];
      assert.deepEqual(await notify("stale", "rejected"), { status: "sent" });
      assert.equal(posts[0], rejected);
      assert.equal([first, ...posts].join("\n"), message);
      issues = original;
    });
    await t.test("ambiguous failure retains its claim and does not duplicate that part", async () => {
      posts.length = 0;
      failure = "ambiguous";
      failAt = 2;
      await assert.rejects(notify("stale", "ambiguous"), /fetch failed/);
      const attempted = [...posts];
      assert.equal([...claims.keys()].filter((key) => key.includes(":stale:ambiguous")).length, 2);
      failure = undefined;
      posts.length = 0;
      assert.deepEqual(await notify("stale", "ambiguous"), { status: "sent" });
      assert.equal([...attempted, ...posts].join("\n"), await staleMessage());
      assert.deepEqual(await notify("stale", "ambiguous"), { status: "already-claimed" });
    });
    await t.test("a single oversized line fails before any delivery or claim", async () => {
      posts.length = 0;
      issues = [issue(1, "x".repeat(40_000))];
      const count = claims.size;
      await assert.rejects(notify("stale", "oversized"), /line exceeds message limit/);
      assert.equal(posts.length, 0);
      assert.equal(claims.size, count);
    });
  } finally {
    for (const [key, value] of Object.entries(previous))
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
  }
});
