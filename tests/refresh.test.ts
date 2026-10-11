import { test } from "node:test";
import assert from "node:assert/strict";
import { GET, maxDuration } from "../app/api/cron/[job]/route";
import { fleetHeartbeat } from "../lib/fleet";

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
  } finally {
    if (secret === undefined) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = secret;
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
  let peak = 0, active = 0, lease = 0, duplicateChecked = false;
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
    const snapshot = JSON.parse([...records.entries()].find(([k]) => k.endsWith(":fleet"))![1]);
    assert.equal(snapshot.data.evaluated_dags, 125);
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
