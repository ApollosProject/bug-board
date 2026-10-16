import { test } from "node:test";
import assert from "node:assert/strict";
import { GET, maxDuration } from "../app/api/cron/[job]/route";
import { evaluateFleet, fleetHeartbeat, fleetInventory } from "../lib/fleet";
import { cachedReviewQueue, reviewRows } from "../lib/reviews";
import type { PullRequest } from "../lib/types";

const request = (job: string, authorization = "Bearer fixture-cron") =>
  GET(new Request(`http://localhost/api/cron/${job}`, { headers: { authorization } }), {
    params: Promise.resolve({ job }),
  });

test("Cron validates job and bearer secret before any integration work", async () => {
  const secret = process.env.CRON_SECRET;
  try {
    assert.equal(maxDuration, 300);
    delete process.env.CRON_SECRET;
    assert.equal((await request("fleet")).status, 503);
    assert.equal((await request("unknown")).status, 404);
    process.env.CRON_SECRET = "fixture-cron";
    assert.equal((await request("fleet", "Bearer wrong")).status, 401);
    assert.equal((await request("reviews", "Bearer wrong")).status, 401);
  } finally {
    if (secret === undefined) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = secret;
  }
});

test("review Cron publishes one complete snapshot for all queue variants; page reads do no integration work", async (t) => {
  const env = { CRON_SECRET: "fixture-cron", GITHUB_TOKEN: "fixture-key",
    LINEAR_API_KEY: "fixture-linear", UPSTASH_REDIS_REST_URL: "http://redis.local",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis", VERCEL_ENV: "production" };
  const previous = Object.fromEntries(Object.keys(env).map((k) => [k, process.env[k]]));
  const records = new Map<string, string>();
  let clock = Date.parse("2026-10-15T00:00:00Z"), integrationCalls = 0, fail = false;
  t.mock.method(Date, "now", () => clock);
  const pr = (id: string, reviewDecision: string): PullRequest => ({
    id, number: Number(id), title: `PR ${id}`, url: `https://github.com/apollosproject/apollos-platforms/pull/${id}`,
    createdAt: "2026-10-10T00:00:00Z", updatedAt: "2026-10-11T00:00:00Z",
    author: { login: "solideo-gloria" }, readiness: { nodes: [] },
    baseRefName: "main", headRefName: "fix", additions: 1, deletions: 0,
    mergeable: "MERGEABLE", reviewDecision, statusCheckRollup: null,
    repository: { nameWithOwner: "apollosproject/apollos-platforms", defaultBranchRef: { name: "main" } },
    reviewRequests: { nodes: [] }, reviews: { nodes: [], pageInfo: { hasNextPage: false, endCursor: null } },
    timelineItems: { nodes: [] }, timelineCount: { totalCount: 0 },
  });
  const prs = [pr("1", "REVIEW_REQUIRED"), pr("2", "APPROVED")];
  t.mock.method(globalThis, "fetch", async (url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (String(url).startsWith(env.UPSTASH_REDIS_REST_URL)) {
      const command = ([op, key, ...args]: string[]) => {
        if (op === "get") return records.get(key) ?? null;
        if (op === "set") {
          if (args.includes("nx") && records.has(key)) return null;
          records.set(key, args[0]); return "OK";
        }
        if (op === "eval" && records.get(args[1]) === args[2]) return Number(records.delete(args[1]));
        throw new Error(`Unexpected Redis operation: ${op}`);
      };
      return Response.json(Array.isArray(body[0])
        ? body.map((c: string[]) => ({ result: command(c) })) : { result: command(body) });
    }
    integrationCalls++;
    if (fail) return new Response("unavailable", { status: 502 });
    if (String(url).includes("api.github.com")) {
      if (body.query.includes("query Timeline")) return Response.json({ data: { node: { timelineItems: {
        nodes: [], pageInfo: { hasPreviousPage: false, startCursor: null },
      } } } });
      assert.ok(!body.variables.query.includes("-review:approved"));
      return Response.json({ data: { search: { issueCount: prs.length,
        nodes: body.variables.query.includes("repo:apollosproject/apollos-platforms ") ? prs : [],
        pageInfo: { hasNextPage: false, endCursor: null } } } });
    }
    return Response.json({ data: { issues: { nodes: [], pageInfo: { hasNextPage: false, endCursor: null } } } });
  });
  try {
    Object.assign(process.env, env);
    assert.equal((await request("reviews")).status, 200);
    assert.ok(integrationCalls > 0);
    const before = integrationCalls;
    assert.equal((await cachedReviewQueue(true)).length, 2);
    assert.deepEqual((await cachedReviewQueue(false)).map((row) => row.pr.id), ["1"]);
    assert.deepEqual((await cachedReviewQueue("only")).map((row) => row.pr.id), ["2"]);
    const age = (await cachedReviewQueue(false))[0].seconds;
    clock += 60000;
    assert.equal((await cachedReviewQueue(false))[0].seconds, age + 60);
    assert.equal(integrationCalls, before);
    const key = [...records.keys()].find((key) => key.endsWith(":reviews"))!;
    const published = records.get(key);
    fail = true;
    assert.equal((await request("reviews")).status, 503);
    assert.equal(records.get(key), published, "Failed refresh must not overwrite a complete snapshot");
    clock += 181000;
    const after = integrationCalls;
    await assert.rejects(cachedReviewQueue(true), /Review queue is refreshing/);
    assert.equal(integrationCalls, after, "Missing production snapshots must not start request-time polling");
    records.set(key, JSON.stringify({ at: clock, data: reviewRows([], new Map()) }));
    assert.deepEqual(await cachedReviewQueue(true), []);
  } finally {
    for (const [key, value] of Object.entries(previous))
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
  }
});

test("ordinary fleet Cron completes without a Workflow, preserves evidence, concurrency and lock ownership", async () => {
  const env = {
    CRON_SECRET: "fixture-cron",
    UPSTASH_REDIS_REST_URL: "http://redis.local",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis",
    AIRFLOW_API_BASE_URL: "http://airflow.local",
    AIRFLOW_API_TOKEN: "fixture-airflow",
    AIRFLOW_FLEET_HEARTBEAT_URL: "http://heartbeat.local",
    VERCEL_ENV: "preview",
  };
  const previous = Object.fromEntries(Object.keys(env).map((k) => [k, process.env[k]]));
  const original = globalThis.fetch;
  const records = new Map<string, string>();
  let peak = 0, active = 0, lease = 0, duplicateChecked = false, resets = 0;
  try {
    Object.assign(process.env, env);
    globalThis.fetch = async (url, init) => {
      const target = new URL(String(url));
      if (target.hostname === "redis.local") {
        const body = JSON.parse(String(init?.body));
        const command = ([op, key, ...args]: string[]) => {
          if (op === "set") {
            if (args.includes("nx")) {
              lease = Number(args[args.indexOf("ex") + 1]);
              if (records.has(key)) return null;
            }
            records.set(key, args[0]);
            return "OK";
          }
          if (op === "eval") {
            if (records.get(args[1]) === args[2]) {
              records.delete(args[1]);
              return 1;
            }
            return 0;
          }
          throw new Error(`Unexpected Redis operation: ${op}`);
        };
        return Response.json(body[0] instanceof Array
          ? body.map((c: string[]) => ({ result: command(c) }))
          : { result: command(body) });
      }
      assert.equal(target.hostname, "airflow.local", "Preview never sends a heartbeat");
      if (target.pathname === "/dags") {
        if (!duplicateChecked) {
          duplicateChecked = true;
          assert.deepEqual(await (await request("fleet")).json(), { status: "already_running" });
        }
        const offset = Number(target.searchParams.get("offset"));
        return Response.json({
          dags: Array.from({ length: Math.min(100, 126 - offset) }, (_, i) => ({ dag_id: `dag-${offset + i}`, is_paused: offset + i === 125 })),
          total_entries: 126,
        });
      }
      active++;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setImmediate(resolve));
      active--;
      const n = Number(target.pathname.split("/")[2].split("-")[1]);
      // One transient connection reset: the retry pass must recover it.
      if (n === 7 && !resets++) throw new TypeError("fetch failed");
      return Response.json({ dag_runs: [
        { state: "running", dag_run_id: `active-${n}` },
        { state: n < 25 ? "failed" : "success", dag_run_id: `terminal-${n}` },
      ], total_entries: 2 });
    };
    const response = await request("fleet");
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { status: "refreshed", result: { status: "degraded", dags: 125 } });
    assert.equal(lease, maxDuration + 60);
    assert.equal(peak, 120);
    assert.equal(resets, 2);
    const snapshot = JSON.parse([...records.entries()].find(([k]) => k.endsWith(":fleet"))![1]);
    assert.equal(snapshot.data.evaluated_dags, 125);
    assert.equal(snapshot.data.failed_fetches, 0);
    assert.equal(snapshot.data.failed_runs, 25);
    assert.equal(snapshot.data.non_terminal_dags, 125);
    assert.equal(snapshot.data.failure_ratio, 0.2);
    assert.equal(snapshot.data.dags.length, 125);
    assert.ok(![...records.keys()].some((k) => k.endsWith(":refresh:fleet")));

    // A lease that expires during work must not let the old request delete its new owner.
    const fetch = globalThis.fetch;
    globalThis.fetch = async (url, init) => {
      if (new URL(String(url)).pathname.includes("/dagRuns")) {
        const key = [...records.keys()].find((k) => k.endsWith(":refresh:fleet"))!;
        records.set(key, "new-owner");
      }
      return fetch(url, init);
    };
    assert.equal((await request("fleet")).status, 200);
    assert.ok([...records.entries()].some(([k, v]) => k.endsWith(":refresh:fleet") && v === "new-owner"));
  } finally {
    globalThis.fetch = original;
    for (const [key, value] of Object.entries(previous))
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
  }
});

test("durable jobs retain long leases and failed polling releases its short lease without false freshness", async () => {
  const env = {
    CRON_SECRET: "fixture-cron", UPSTASH_REDIS_REST_URL: "http://redis.local",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis", AIRFLOW_API_BASE_URL: "http://airflow.local",
    AIRFLOW_API_TOKEN: "fixture-airflow", AIRFLOW_FLEET_HEARTBEAT_URL: "",
    VERCEL_ENV: "preview",
  };
  const previous = Object.fromEntries(Object.keys(env).map((k) => [k, process.env[k]]));
  const original = globalThis.fetch;
  let durable = true, lease = 0, releases = 0, unknowns = 0;
  try {
    Object.assign(process.env, env);
    globalThis.fetch = async (url, init) => {
      if (new URL(String(url)).hostname === "airflow.local") return new Response("private upstream error", { status: 503 });
      const body = JSON.parse(String(init?.body));
      const command = ([op, , ...args]: string[]) => {
        if (op === "set") {
          assert.ok(args.includes("nx"), "No successful snapshot or heartbeat reset after inventory failure");
          lease = Number(args[args.indexOf("ex") + 1]);
          return durable ? null : "OK";
        }
        if (op === "incr") return ++unknowns;
        if (op === "eval") { releases++; return 1; }
        throw new Error(`Unexpected operation: ${op}`);
      };
      return Response.json(body[0] instanceof Array ? body.map((c: string[]) => ({ result: command(c) })) : { result: command(body) });
    };
    for (const job of ["regressions", "notifications"]) {
      assert.deepEqual(await (await request(job)).json(), { status: "already_running" });
      assert.equal(lease, 8 * 3600);
    }
    durable = false;
    const response = await request("fleet");
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { error: "refresh_unavailable" });
    assert.equal(lease, 360);
    assert.equal(releases, 1);
    assert.equal(unknowns, 1);
  } finally {
    globalThis.fetch = original;
    for (const [key, value] of Object.entries(previous))
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
  }
});

test("fleet heartbeat retains unknown threshold, recovery, failure path and Preview suppression", async () => {
  const env = {
    UPSTASH_REDIS_REST_URL: "http://redis.local",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis",
    AIRFLOW_FLEET_HEARTBEAT_URL: "http://heartbeat.local/monitor",
    VERCEL_ENV: "production",
  };
  const previous = Object.fromEntries(Object.keys(env).map((k) => [k, process.env[k]]));
  const original = globalThis.fetch;
  const deliveries: string[] = [];
  let count = 0;
  try {
    Object.assign(process.env, env);
    globalThis.fetch = async (url, init) => {
      const target = new URL(String(url));
      if (target.hostname === "heartbeat.local") {
        deliveries.push(target.pathname);
        assert.equal(init?.redirect, "error");
        return new Response("ok");
      }
      const body = JSON.parse(String(init?.body));
      const command = ([op]: string[]) => {
        if (op === "incr") return ++count;
        count = 0;
        return "OK";
      };
      return Response.json(body[0] instanceof Array ? body.map((c: string[]) => ({ result: command(c) })) : { result: command(body) });
    };
    await fleetHeartbeat("unknown");
    await fleetHeartbeat("unknown");
    assert.deepEqual(deliveries, []);
    await fleetHeartbeat("unknown");
    await fleetHeartbeat("healthy");
    assert.equal(count, 0);
    await fleetHeartbeat("degraded");
    assert.deepEqual(deliveries, ["/monitor/fail", "/monitor", "/monitor/fail"]);
    process.env.VERCEL_ENV = "preview";
    await fleetHeartbeat("healthy");
    assert.equal(deliveries.length, 3);
    process.env.VERCEL_ENV = "production";
    const fetch = globalThis.fetch;
    globalThis.fetch = (url, init) => new URL(String(url)).hostname === "heartbeat.local"
      ? Promise.resolve(new Response("unavailable", { status: 503 })) : fetch(url, init);
    await assert.rejects(fleetHeartbeat("healthy"), /Fleet heartbeat unavailable/);
  } finally {
    globalThis.fetch = original;
    for (const [key, value] of Object.entries(previous))
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
  }
});

test("fleet retries failed lookups once and stops at the Cron deadline", async () => {
  const previous = [process.env.AIRFLOW_API_BASE_URL, process.env.AIRFLOW_API_TOKEN];
  const original = globalThis.fetch;
  let calls = 0;
  try {
    process.env.AIRFLOW_API_BASE_URL = "http://airflow.local";
    process.env.AIRFLOW_API_TOKEN = "fixture-airflow";
    const ids = Array.from({ length: 10 }, (_, i) => `dag-${i}`);
    // Every first lookup fails and every retry succeeds.
    globalThis.fetch = async () => {
      calls++;
      if (calls <= 10) throw new TypeError("fetch failed");
      return Response.json({ dag_runs: [{ state: "success", dag_run_id: "r" }], total_entries: 1 });
    };
    const recovered = await evaluateFleet(ids, 5);
    assert.equal(calls, 20);
    assert.equal(recovered.status, "healthy");
    assert.equal(recovered.failed_fetches, 0);
    // A request starts only if its 30-second timeout ends before the deadline.
    // With 31 seconds of budget and 600 ms failures, only two requests can start.
    calls = 0;
    globalThis.fetch = async () => {
      calls++;
      await new Promise((resolve) => setTimeout(resolve, 600));
      throw new TypeError("fetch failed");
    };
    const warnings: unknown[] = [];
    const warn = console.warn;
    console.warn = (...args: unknown[]) => void warnings.push(args[1]);
    const unchecked = await evaluateFleet(ids, 1, Date.now() + 31_000).finally(() => {
      console.warn = warn;
    });
    assert.equal(calls, 2);
    // No retry could start before the deadline, and none is reported.
    assert.deepEqual(warnings, [{ retried: 0, unchecked: 10, reasons: { "TypeError": 2, "Deadline": 8 } }]);
    assert.equal(unchecked.status, "unknown");
    assert.equal(unchecked.failed_fetches, 10);
    // Inventory shares the deadline: with no room left, it requests nothing.
    calls = 0;
    await assert.rejects(fleetInventory(Date.now() + 1_000), /Fleet deadline/);
    assert.equal(calls, 0);
  } finally {
    globalThis.fetch = original;
    for (const [key, value] of [["AIRFLOW_API_BASE_URL", previous[0]], ["AIRFLOW_API_TOKEN", previous[1]]] as const)
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
  }
});
