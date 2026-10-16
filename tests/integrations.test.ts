import { test } from "node:test";
import assert from "node:assert/strict";
import { fetchIssues, projects } from "../lib/linear";
import { completeDelivery, mergedPRs, openPRs } from "../lib/github";
import { blameFile } from "../lib/regressions";
import {
  mapConcurrent,
  graphql,
  requestJson,
  UpstreamError,
} from "../lib/http";
import type { PullRequest } from "../lib/types";
const pageInfo = (more = false, cursor: string | null = null) => ({
  hasNextPage: more,
  endCursor: cursor,
});
test("Linear issues and projects paginate without silently truncating results", async () => {
  const fetch = globalThis.fetch,
    token = process.env.LINEAR_API_KEY;
  process.env.LINEAR_API_KEY = "fixture-key";
  try {
    const cursors: unknown[] = [];
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      cursors.push(body.variables.cursor);
      const second = !!body.variables.cursor;
      const page = {
        nodes: [{ id: second ? "second" : "first" }],
        pageInfo: pageInfo(!second, second ? null : "next"),
      };
      return Response.json({
        data: body.query.includes("query Projects")
          ? {
              teams: {
                nodes: [
                  {
                    projects: {
                      ...page,
                      nodes: [
                        {
                          id: second ? "second" : "first",
                          name: second ? "B" : "A",
                        },
                      ],
                    },
                  },
                ],
              },
            }
          : { issues: page },
      });
    };
    assert.equal((await fetchIssues({})).length, 2);
    assert.equal((await projects()).length, 2);
    assert.deepEqual(cursors, [null, "next", null, "next"]);
    globalThis.fetch = async () =>
      Response.json({
        data: { issues: { nodes: [], pageInfo: pageInfo(true, null) } },
      });
    await assert.rejects(fetchIssues({}), /Incomplete/);
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.LINEAR_API_KEY;
    else process.env.LINEAR_API_KEY = token;
  }
});
test("Linear uses 250-issue pages to preserve complete results within the polling request budget", async () => {
  const original = globalThis.fetch, token = process.env.LINEAR_API_KEY;
  const nodes = Array.from({ length: 600 }, (_, i) => ({ id: `issue-${i}` }));
  const cursors: (string | null)[] = [];
  process.env.LINEAR_API_KEY = "fixture-key";
  try {
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      assert.match(body.query, /issues\(first: 250,/);
      const start = Number(body.variables.cursor || 0), end = start + 250;
      cursors.push(body.variables.cursor);
      return Response.json({ data: { issues: {
        nodes: nodes.slice(start, end),
        pageInfo: pageInfo(end < nodes.length, end < nodes.length ? String(end) : null),
      } } });
    };
    assert.deepEqual(await fetchIssues({}), nodes);
    assert.deepEqual(cursors, [null, "250", "500"]);
  } finally {
    globalThis.fetch = original;
    if (token === undefined) delete process.env.LINEAR_API_KEY;
    else process.env.LINEAR_API_KEY = token;
  }
});
test("Linear paginates incoming project dependencies and rejects missing or repeated cursors", async () => {
  const fetch = globalThis.fetch, token = process.env.LINEAR_API_KEY;
  process.env.LINEAR_API_KEY = "fixture-key";
  try {
    for (const cursor of [null, "next", "repeated"]) {
      const calls: unknown[] = [];
      globalThis.fetch = async (_url, init) => {
        const body = JSON.parse(String(init?.body));
        calls.push(body.variables);
        if (body.query.includes("query ProjectDependencies")) {
          assert.match(body.query, /inverseRelations\(first: 50/);
          return Response.json({ data: { project: { inverseRelations: {
            nodes: [{ type: "dependency", project: null, projectMilestone: { status: "next" } }],
            pageInfo: pageInfo(cursor === "repeated", cursor === "repeated" ? cursor : null),
          } } } });
        }
        assert.match(body.query, /inverseRelations\(first: 5\)/);
        assert.match(body.query, /projectMilestone \{ status \}/);
        return Response.json({ data: { teams: { nodes: [{ projects: {
          nodes: [{ id: "project", name: "Exports", inverseRelations: {
            nodes: [], pageInfo: pageInfo(true, cursor),
          } }], pageInfo: pageInfo(),
        } }] } } });
      };
      if (cursor !== "next") await assert.rejects(projects(), /dependency pagination/);
      else {
        const [item] = await projects();
        assert.equal(item.inverseRelations?.nodes.length, 1);
        assert.deepEqual(calls[1], { id: "project", after: "next" });
      }
    }
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.LINEAR_API_KEY; else process.env.LINEAR_API_KEY = token;
  }
});
test("Linear keeps every assignment page and only report-relevant attachment metadata", async () => {
  const fetch = globalThis.fetch,
    token = process.env.LINEAR_API_KEY;
  process.env.LINEAR_API_KEY = "fixture-key";
  try {
    const assignment = {
      node: {
        toAssignee: { displayName: "michael.neeley" },
        updatedAt: "2026-08-07T00:00:00.000Z",
      },
    };
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      return Response.json({
        data: body.query.includes("query History")
          ? {
              issue: {
                history: {
                  edges: [assignment],
                  pageInfo: pageInfo(false, "history-final"),
                },
              },
            }
          : {
              issues: {
                nodes: [
                  {
                    id: "issue",
                    updatedAt: "2026-09-01T00:00:00Z",
                    history: {
                      edges: [
                        { node: { toAssignee: null, updatedAt: "2026-09-01" } },
                      ],
                      pageInfo: pageInfo(true, "history-next"),
                    },
                    attachments: {
                      nodes: [
                        {
                          metadata: {
                            url: "https://github.com/org/repo/pull/1",
                            status: "merged",
                            linkKind: "closes",
                            unused: "large upstream payload",
                          },
                        },
                      ],
                    },
                  },
                ],
                pageInfo: pageInfo(),
              },
            },
      });
    };
    const [item] = await fetchIssues({}, true);
    assert.deepEqual(item.history?.edges, [assignment]);
    assert.deepEqual(item.attachments?.nodes, [
      {
        metadata: {
          url: "https://github.com/org/repo/pull/1",
          status: "merged",
          linkKind: "closes",
        },
      },
    ]);
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.LINEAR_API_KEY;
    else process.env.LINEAR_API_KEY = token;
  }
});

test("Linear histories batch cold loads, reuse unchanged versions, and invalidate changes and expiry", async (t) => {
  const names = ["LINEAR_API_KEY", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "KV_REST_API_URL", "KV_REST_API_TOKEN"];
  const env = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  const original = globalThis.fetch, records = new Map<string, string>();
  let clock = Date.parse("2026-10-07T12:00:00Z"), calls = 0, active = 0, peak = 0;
  const issues = Array.from({ length: 101 }, (_, i) => ({ id: `i-${i}`, updatedAt: "v1" }));
  t.mock.method(Date, "now", () => clock);
  try {
    for (const name of names) delete process.env[name];
    process.env.LINEAR_API_KEY = "fixture-key";
    process.env.KV_REST_API_URL = "https://cache.example.test";
    process.env.KV_REST_API_TOKEN = "fixture-cache";
    globalThis.fetch = async (url, init) => {
      const body = JSON.parse(String(init?.body));
      if (String(url).startsWith("https://cache.example.test")) {
        const single = typeof body[0] === "string";
        const results = (single ? [body] : body).map(([command, key, value]: string[]) => {
          if (command === "get") return { result: records.get(key) ?? null };
          assert.equal(command, "set"); records.set(key, value); return { result: "OK" };
        });
        return Response.json(single ? results[0] : results);
      }
      if (body.query.includes("query Histories(")) {
        calls++;
        assert.ok(body.variables.ids.length <= 25);
        assert.match(body.query, /createdAt/);
        assert.match(body.query, /fromState \{ id name type \} toState \{ id name type \}/);
        peak = Math.max(peak, ++active);
        await new Promise((resolve) => setTimeout(resolve, 1));
        active--;
        return Response.json({ data: { issues: {
          nodes: issues.filter((issue) => body.variables.ids.includes(issue.id)).map((issue) => ({
            ...issue, history: { edges: [], pageInfo: pageInfo() },
          })), pageInfo: pageInfo(),
        } } });
      }
      assert.ok(!body.query.includes("history(first:"));
      return Response.json({ data: { issues: { nodes: structuredClone(issues), pageInfo: pageInfo() } } });
    };
    assert.equal((await fetchIssues({}, true)).length, 101);
    assert.equal(calls, 5);
    assert.equal(peak, 4);
    assert.equal((await fetchIssues({}, true)).length, 101);
    assert.equal(calls, 5);
    issues[0].updatedAt = "v2";
    await fetchIssues({}, true);
    assert.equal(calls, 6);
    // Open and completed polling cannot overwrite each other's history caches.
    await fetchIssues({}, true, "open");
    assert.equal(calls, 11);
    await fetchIssues({}, true);
    assert.equal(calls, 11);
    clock += 3600001;
    await fetchIssues({}, true);
    assert.equal(calls, 16);
  } finally {
    globalThis.fetch = original;
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
  }
});

test("Linear history batches reject missing issues, changed versions, and broken pagination", async () => {
  const original = globalThis.fetch, token = process.env.LINEAR_API_KEY;
  process.env.LINEAR_API_KEY = "fixture-key";
  try {
    for (const failure of ["missing", "changed", "cursor", "repeated"]) {
      globalThis.fetch = async (_url, init) => {
        const body = JSON.parse(String(init?.body));
        const issue = { id: "issue", updatedAt: "v1" };
        if (body.query.includes("query Histories(")) return Response.json({ data: { issues: {
          nodes: failure === "missing" ? [] : [{ ...issue,
            updatedAt: failure === "changed" ? "v2" : "v1",
            history: { edges: [], pageInfo: pageInfo(true, failure === "cursor" ? null : "same") },
          }], pageInfo: pageInfo(),
        } } });
        if (body.query.includes("query History(")) return Response.json({ data: { issue: {
          history: { edges: [], pageInfo: pageInfo(true, "same") },
        } } });
        return Response.json({ data: { issues: { nodes: [issue], pageInfo: pageInfo() } } });
      };
      await assert.rejects(fetchIssues({}, true), /Incomplete Linear histor|changed during refresh/);
    }
  } finally {
    globalThis.fetch = original;
    if (token === undefined) delete process.env.LINEAR_API_KEY; else process.env.LINEAR_API_KEY = token;
  }
});

test("GitHub partitions over-cap date searches and rejects a single overflowing day", async () => {
  const fetch = globalThis.fetch,
    token = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = "fixture-key";
  try {
    const ranges: string[] = [];
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body)),
        range = /merged:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})/.exec(
          body.variables.query,
        )!;
      assert.match(body.query, /search\(type: ISSUE,.*first: 25,/);
      assert.match(body.query, /reviews\(first: 10, states: \[APPROVED\]\)/);
      ranges.push(range[0]);
      const overflow = range[1] !== range[2];
      return Response.json({
        data: {
          search: {
            issueCount: overflow ? 1001 : 1,
            nodes: overflow
              ? []
              : [
                  {
                    id: range[1],
                    mergedAt: `${range[1]}T12:00:00Z`,
                    reviews: { nodes: [], pageInfo: pageInfo() },
                  },
                ],
            pageInfo: pageInfo(),
          },
        },
      });
    };
    assert.equal(
      (await mergedPRs("2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")).length,
      2,
    );
    assert.equal(ranges.length, 3);
    globalThis.fetch = async () =>
      Response.json({
        data: { search: { issueCount: 1001, nodes: [], pageInfo: pageInfo() } },
      });
    await assert.rejects(
      mergedPRs("2026-09-04T00:00:00Z", "2026-09-05T00:00:00Z"),
      /1,000/,
    );
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = token;
  }
});
test("weekly searches cover the full window with at most four concurrent requests", async () => {
  const fetch = globalThis.fetch,
    token = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = "fixture-key";
  try {
    let active = 0, peak = 0;
    const intervals: string[] = [];
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      const match = /merged:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})/.exec(body.variables.query)!;
      intervals.push(match[0]);
      peak = Math.max(peak, ++active);
      await Promise.resolve();
      active--;
      const nodes = [];
      for (let day = Date.parse(match[1]); day <= Date.parse(match[2]); day += 86400000) {
        const id = new Date(day).toISOString().slice(0, 10);
        nodes.push({ id, mergedAt: `${id}T12:00:00Z`, reviews: { nodes: [], pageInfo: pageInfo() } });
      }
      return Response.json({ data: { search: { issueCount: nodes.length, nodes, pageInfo: pageInfo() } } });
    };
    const prs = await mergedPRs("2026-09-01T13:00:00Z", "2026-10-01T00:00:00Z");
    assert.equal(prs.length, 29);
    assert.equal(new Set(prs.map((pr) => pr.id)).size, 29);
    assert.equal(peak, 4);
    assert.deepEqual(intervals, [
      "merged:2026-09-01..2026-09-07", "merged:2026-09-08..2026-09-14",
      "merged:2026-09-15..2026-09-21", "merged:2026-09-22..2026-09-28",
      "merged:2026-09-29..2026-09-30",
    ]);
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = token;
  }
});
test("historical PR chunks cache complete approvals, expire, and keep today's chunk fresh", async (t) => {
  const names = ["GITHUB_TOKEN", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "KV_REST_API_URL", "KV_REST_API_TOKEN"];
  const env = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  const fetch = globalThis.fetch, records = new Map<string, string>();
  let clock = Date.parse("2026-10-07T12:00:00Z"), searches = 0, approvals = 0;
  t.mock.method(Date, "now", () => clock);
  try {
    for (const name of names) delete process.env[name];
    process.env.GITHUB_TOKEN = "fixture-key";
    process.env.KV_REST_API_URL = "https://cache.example.test";
    process.env.KV_REST_API_TOKEN = "fixture-cache";
    globalThis.fetch = async (url, init) => {
      const body = JSON.parse(String(init?.body));
      if (String(url).startsWith("https://cache.example.test")) {
        const single = typeof body[0] === "string";
        const results = (single ? [body] : body).map(([command, key, value]: string[]) => {
          if (command === "get") return { result: records.get(key) ?? null };
          assert.equal(command, "set");
          records.set(key, value);
          return { result: "OK" };
        });
        return Response.json(single ? results[0] : results);
      }
      if (body.variables.id) {
        approvals++;
        return Response.json({ data: { node: { reviews: {
          nodes: [{ author: { login: "second" }, state: "APPROVED" }], pageInfo: pageInfo(false, "final"),
        } } } });
      }
      searches++;
      const day = /merged:(\d{4}-\d{2}-\d{2})/.exec(body.variables.query)![1];
      return Response.json({ data: { search: {
        issueCount: 1, pageInfo: pageInfo(), nodes: [{ id: day, mergedAt: `${day}T12:00:00Z`,
          reviews: { nodes: [{ author: { login: "first" }, state: "APPROVED" }], pageInfo: pageInfo(true, "more") },
        }],
      } } });
    };
    const first = await mergedPRs("2026-09-29T00:00:00Z", "2026-10-08T00:00:00Z");
    const second = await mergedPRs("2026-09-29T00:00:00Z", "2026-10-08T00:00:00Z");
    assert.deepEqual(second, first);
    assert.equal(searches, 3);
    assert.equal(approvals, 3);
    assert.equal(records.size, 1);
    assert.deepEqual(second[0].reviews.nodes.map((r) => r.author?.login), ["first", "second"]);
    assert.equal(second[0].reviews.pageInfo?.hasNextPage, false);
    assert.equal((await mergedPRs("2026-09-29T13:00:00Z", "2026-10-08T00:00:00Z")).length, 1);
    clock += 3600001;
    await mergedPRs("2026-09-29T00:00:00Z", "2026-10-08T00:00:00Z");
    assert.equal(searches, 6);
    delete process.env.GITHUB_TOKEN;
    await assert.rejects(mergedPRs("2026-09-29T00:00:00Z", "2026-10-08T00:00:00Z"), /not configured/);
  } finally {
    globalThis.fetch = fetch;
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
  }
});
test("small merged-search review pages still retrieve every approval", async () => {
  const fetch = globalThis.fetch,
    token = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = "fixture-key";
  try {
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      const reviews = {
        nodes: [{ author: { login: body.variables.id ? "second" : "first" }, state: "APPROVED" }],
        pageInfo: pageInfo(!body.variables.id, body.variables.id ? "final" : "more"),
      };
      return Response.json({ data: body.variables.id
        ? { node: { reviews } }
        : { search: {
            issueCount: 1, pageInfo: pageInfo(),
            nodes: [{ id: "pr", mergedAt: "2026-08-01T12:00:00Z", reviews }],
          } },
      });
    };
    const [pr] = await mergedPRs("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z");
    assert.deepEqual(pr.reviews.nodes.map((r) => r.author?.login), ["first", "second"]);
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = token;
  }
});
test("review timelines reuse complete history across variants and invalidate on PR or event changes", async (t) => {
  const names = ["GITHUB_TOKEN", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "KV_REST_API_URL", "KV_REST_API_TOKEN"];
  const env = Object.fromEntries(names.map((name) => [name, process.env[name]])),
    fetch = globalThis.fetch, records = new Map<string, string>();
  let clock = Date.parse("2026-10-08T12:00:00Z"), updatedAt = "2026-10-07T12:00:00Z",
    totalCount = 2, timelineCalls = 0, broken = false;
  t.mock.method(Date, "now", () => clock);
  try {
    for (const name of names) delete process.env[name];
    process.env.GITHUB_TOKEN = "fixture-key";
    process.env.KV_REST_API_URL = "https://cache.example.test";
    process.env.KV_REST_API_TOKEN = "fixture-cache";
    globalThis.fetch = async (url, init) => {
      const body = JSON.parse(String(init?.body));
      if (String(url).startsWith("https://cache.example.test")) {
        const single = typeof body[0] === "string";
        const results = (single ? [body] : body).map(([command, key, value]: string[]) => {
          if (command === "get") return { result: records.get(key) ?? null };
          assert.equal(command, "set"); records.set(key, value); return { result: "OK" };
        });
        return Response.json(single ? results[0] : results);
      }
      if (body.query.includes("query Timeline(")) {
        timelineCalls++;
        const older = !!body.variables.before;
        return Response.json({ data: { node: { timelineItems: {
          nodes: [{ __typename: "ReadyForReviewEvent", createdAt: older ? "2026-09-01" : "2026-09-02" }],
          pageInfo: { hasPreviousPage: !older || broken, startCursor: "newer" },
        } } } });
      }
      assert.match(body.query, /timelineCount: timelineItems\(first: 1\) \{ totalCount \}/);
      return Response.json({ data: { search: {
        issueCount: 1, pageInfo: pageInfo(),
        nodes: body.variables.query.startsWith("repo:apollosproject/apollos-platforms ") ? [{
          id: "pr", updatedAt, timelineCount: { totalCount },
          reviews: { nodes: [], pageInfo: pageInfo() },
          reviewRequests: { nodes: [], pageInfo: pageInfo() },
        }] : [],
      } } });
    };
    const first = await openPRs(true), second = await openPRs(false);
    assert.deepEqual(second, first);
    assert.deepEqual(first[0].timelineItems?.nodes.map(n => n.createdAt), ["2026-09-01", "2026-09-02"]);
    assert.equal(timelineCalls, 2);
    totalCount++;
    await openPRs(true);
    assert.equal(timelineCalls, 4);
    updatedAt = "2026-10-08T11:00:00Z";
    await openPRs(false);
    assert.equal(timelineCalls, 6);
    clock += 1800000;
    await openPRs(true);
    assert.equal(timelineCalls, 6);
    const saved = [...records];
    broken = true; totalCount++;
    await assert.rejects(openPRs(true), /Incomplete timeline/);
    assert.deepEqual([...records], saved);
    broken = false; clock += 1800001;
    await openPRs(true);
    assert.equal(timelineCalls, 10);
    updatedAt = "";
    await assert.rejects(openPRs(false), /Missing GitHub timeline version/);
    delete process.env.GITHUB_TOKEN;
    await assert.rejects(openPRs(true), /not configured/);
  } finally {
    globalThis.fetch = fetch;
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
  }
});

test("first-review pagination skips bot/self reviews and stops at the first human response", async () => {
  const original = globalThis.fetch, token = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = "fixture-key";
  let calls = 0;
  const review = (login: string, at: string, kind = "User") => ({
    author: { login, __typename: kind }, state: "COMMENTED", submittedAt: at,
  });
  try {
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      if (body.query.includes("query FirstReview(")) {
        calls++;
        assert.equal(body.variables.cursor, "bots");
        return Response.json({ data: { node: { firstReviews: {
          nodes: [review("human", "2026-09-01T01:00:00Z")], pageInfo: pageInfo(true, "more-never-needed"),
        } } } });
      }
      assert.match(body.query, /firstReviews: reviews\(first: 5/);
      assert.match(body.query, /READY_FOR_REVIEW_EVENT, CONVERT_TO_DRAFT_EVENT/);
      return Response.json({ data: { search: {
        issueCount: 1, pageInfo: pageInfo(), nodes: [{
          id: "pr", author: { login: "solideo-gloria" }, createdAt: "2026-09-01T00:00:00Z",
          mergedAt: "2026-09-01T12:00:00Z", readiness: { nodes: [] }, reviews: { nodes: [], pageInfo: pageInfo() },
          firstReviews: { nodes: [review("bot", "2026-09-01T00:01:00Z", "Bot"),
            review("solideo-gloria", "2026-09-01T00:02:00Z")], pageInfo: pageInfo(true, "bots") },
        }],
      } } });
    };
    const [pr] = await mergedPRs("2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z");
    assert.equal(calls, 1);
    assert.equal(pr.firstHumanReviewAt, "2026-09-01T01:00:00Z");
    assert.equal(pr.firstReviews, undefined);
  } finally {
    globalThis.fetch = original;
    if (token === undefined) delete process.env.GITHUB_TOKEN; else process.env.GITHUB_TOKEN = token;
  }
});

test("approved-only pipeline searches are bounded to eight repos and never fetch request timelines", async () => {
  const original = globalThis.fetch, token = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = "fixture-key";
  let calls = 0, active = 0, peak = 0;
  try {
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      assert.match(body.query, /query Search/);
      assert.match(body.variables.query, /is:open draft:false review:approved$/);
      peak = Math.max(peak, ++active);
      await new Promise((resolve) => setTimeout(resolve, 1));
      active--; calls++;
      return Response.json({ data: { search: { issueCount: 1, pageInfo: pageInfo(), nodes: [{
        id: body.variables.query, reviewDecision: "APPROVED", reviews: { nodes: [], pageInfo: pageInfo() },
        reviewRequests: { nodes: [], pageInfo: { hasNextPage: false } },
      }] } } });
    };
    assert.equal((await openPRs("only")).length, 8);
    assert.equal(calls, 8);
    assert.equal(peak, 4);
  } finally {
    globalThis.fetch = original;
    if (token === undefined) delete process.env.GITHUB_TOKEN; else process.env.GITHUB_TOKEN = token;
  }
});

test("reports use Redis beyond Next's size limit and retain compact stage totals and assignments", async (t) => {
  const names = ["LINEAR_API_KEY", "GITHUB_TOKEN", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "KV_REST_API_URL", "KV_REST_API_TOKEN", "VERCEL_ENV", "VERCEL_URL"];
  const env = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  const original = globalThis.fetch, records = new Map<string, string>();
  let calls = 0, shipped = 0, clock = Date.parse("2026-10-07T12:00:00Z");
  t.mock.method(Date, "now", () => clock);
  const todo = { id: "todo", name: "Todo", type: "unstarted" }, done = { id: "done", name: "Done", type: "completed" };
  const assignment = { node: { toAssignee: { displayName: "zach" }, updatedAt: "2026-09-02T00:00:00Z" } };
  const issue = { id: "i", identifier: "APO-1", title: "x".repeat(2100000), createdAt: "2026-09-01T00:00:00Z",
    updatedAt: "2026-09-03T00:00:00Z", completedAt: "2026-09-03T00:00:00Z", state: done,
    assignee: { displayName: "zach" }, project: null, priority: 2, labels: { nodes: [] } };
  try {
    for (const name of names) delete process.env[name];
    process.env.LINEAR_API_KEY = process.env.GITHUB_TOKEN = "fixture-key";
    process.env.KV_REST_API_URL = "https://cache.example.test";
    process.env.KV_REST_API_TOKEN = "fixture-cache";
    globalThis.fetch = async (url, init) => {
      const body = JSON.parse(String(init?.body));
      if (String(url).startsWith("https://cache.example.test")) {
        const single = typeof body[0] === "string";
        const results = (single ? [body] : body).map(([command, key, value]: string[]) => {
          if (command === "get") return { result: records.get(key) ?? null };
          assert.equal(command, "set"); records.set(key, value); return { result: "OK" };
        });
        return Response.json(single ? results[0] : results);
      }
      calls++;
      if (body.query.includes("customerTicketCount")) shipped++;
      if (String(url).includes("api.github.com")) return Response.json({ data: { search: { nodes: [], issueCount: 0, pageInfo: pageInfo() } } });
      if (body.query.includes("query Projects")) return Response.json({ data: { teams: { nodes: [{ projects: { nodes: [], pageInfo: pageInfo() } }] } } });
      if (body.query.includes("query Histories")) return Response.json({ data: { issues: { nodes: [{ ...issue, history: {
        edges: [assignment, { node: { fromState: todo, toState: done, toAssignee: null, createdAt: issue.completedAt, updatedAt: issue.completedAt } }],
        pageInfo: pageInfo(),
      } }], pageInfo: pageInfo() } } });
      return Response.json({ data: { issues: { nodes: [issue], pageInfo: pageInfo() } } });
    };
    const { reportFor, openWork, projectDashboard } = await import("../lib/reports");
    const { timeWindow } = await import("../lib/window");
    const window = timeWindow({ start: "2026-09-01", end: "2026-09-03" });
    const first = await reportFor(window), before = calls;
    assert.equal(first.completed[0].stageDurations?.[0].seconds, 2 * 86400);
    assert.deepEqual(first.completed[0].history?.edges, [assignment]);
    assert.deepEqual(await reportFor(window), first);
    assert.equal(calls, before);
    assert.ok([...records].some(([key, value]) => key.includes("report-v3:") && value.length > 2097152));
    clock += 61000;
    await reportFor(window);
    assert.ok(calls > before);
    // Release and customer details keep their own 15-minute cache across report refreshes.
    assert.equal(shipped, 1);

    // Previews have no Cron: the default window must read through, including after expiry.
    clock = Date.parse("2026-09-03T12:00:00Z");
    process.env.VERCEL_ENV = "preview";
    process.env.VERCEL_URL = "first-preview.vercel.app";
    const defaultWindow = timeWindow({ days: "30" });
    assert.equal((await reportFor(defaultWindow)).completed.length, 1);
    const warmCalls = calls;
    await reportFor(defaultWindow);
    assert.equal(calls, warmCalls);
    clock += 181000;
    await reportFor(defaultWindow);
    assert.ok(calls > warmCalls);
    process.env.VERCEL_URL = "second-preview.vercel.app";
    await reportFor(defaultWindow);
    assert.ok([...records.keys()].some((key) => key.includes(":preview:first-preview.vercel.app:") && key.includes("report-v3:")));
    assert.ok([...records.keys()].some((key) => key.includes(":preview:second-preview.vercel.app:") && key.includes("report-v3:")));
    // Production still requires fresh Cron snapshots and cannot read either preview's cache.
    process.env.VERCEL_ENV = "production";
    const beforeProduction = calls;
    await assert.rejects(reportFor(defaultWindow), /Team metrics are refreshing/);
    await assert.rejects(openWork(), /Open work is refreshing/);
    await assert.rejects(projectDashboard(), /Projects are refreshing/);
    assert.equal(calls, beforeProduction);
  } finally {
    globalThis.fetch = original;
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
  }
});

test("merged PR delivery timing is fetched once in bounded batches, cached, and stops at the first failure", async (t) => {
  const names = ["GITHUB_TOKEN", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "KV_REST_API_URL", "KV_REST_API_TOKEN"];
  const env = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  const original = globalThis.fetch, records = new Map<string, string>(), batches: string[][] = [];
  let fail = false;
  t.mock.method(console, "warn", () => {});
  const pr = (id: string) => ({ id, url: `https://github.com/a/b/pull/${id}`, createdAt: "2026-10-01T00:00:00Z",
    mergedAt: new Date().toISOString(), readiness: { nodes: [] }, author: { login: "solideo-gloria" } }) as unknown as PullRequest;
  try {
    for (const name of names) delete process.env[name];
    process.env.GITHUB_TOKEN = "fixture-key";
    process.env.KV_REST_API_URL = "https://cache.example.test";
    process.env.KV_REST_API_TOKEN = "fixture-cache";
    globalThis.fetch = async (url, init) => {
      const body = JSON.parse(String(init?.body));
      if (String(url).startsWith("https://cache.example.test")) {
        const single = typeof body[0] === "string";
        const results = (single ? [body] : body).map(([command, key, value]: string[]) => {
          if (command === "get") return { result: records.get(key) ?? null };
          records.set(key, value); return { result: "OK" };
        });
        return Response.json(single ? results[0] : results);
      }
      assert.match(body.query, /query Delivery/);
      batches.push(body.variables.ids);
      if (fail) return new Response("unavailable", { status: 502 });
      const commits = { nodes: [] };
      return Response.json({ data: { nodes: body.variables.ids.map(() => ({ reviews: { nodes: [] }, opened: commits, head: commits })) } });
    };
    // A refresh fills at most 50 PRs, so cold caches never burst GitHub's compute limit.
    const prs = Array.from({ length: 62 }, (_, i) => pr(`pr-${i}`));
    await completeDelivery(prs);
    assert.deepEqual(batches.map((ids) => ids.length), [10, 10, 10, 10, 10]);
    assert.equal(prs.filter((item) => item.delivery).length, 50);
    await completeDelivery(prs);
    assert.deepEqual(batches.slice(5).map((ids) => ids.length), [10, 2]);
    // The first failure stops the refresh; cached PRs keep their timing.
    const again = [...prs.map((item) => pr(item.id)), pr("new"), ...Array.from({ length: 15 }, (_, i) => pr(`more-${i}`))];
    fail = true;
    const before = batches.length;
    await completeDelivery(again);
    assert.equal(batches.length, before + 1);
    assert.ok(again.slice(0, 62).every((item) => item.delivery));
    assert.ok(again.slice(62).every((item) => item.delivery === undefined));
  } finally {
    globalThis.fetch = original;
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
  }
});

test("blame loads metadata only for removed-line commits and exposes failures for durable retry", async () => {
  const fetch = globalThis.fetch,
    token = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = "fixture-key";
  const context = {
    owner: "org", repo: "repo", oid: "parent", complete: true,
    url: "https://github.com/org/repo/pull/2", mergedAt: "2026-09-01T00:00:00Z",
  };
  const file = { filename: "file.ts", status: "modified", patch: "@@ -10,2 +10,0 @@\n-old\n-old" };
  try {
    const requested: string[] = [];
    globalThis.fetch = async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      if (body.query.includes("query Blame(")) {
        assert.ok(!body.query.includes("reviews"));
        return Response.json({ data: { repository: { object: { blame: { ranges: [
          { startingLine: 1, endingLine: 9, commit: { oid: "irrelevant", committedDate: "2026-01-01" } },
          { startingLine: 10, endingLine: 10, commit: { oid: "relevant", committedDate: "2026-08-01" } },
          { startingLine: 11, endingLine: 11, commit: { oid: "relevant", committedDate: "2026-08-01" } },
        ] } } } } });
      }
      requested.push(body.variables.oid);
      return Response.json({ data: { repository: { object: { associatedPullRequests: {
        pageInfo: pageInfo(), nodes: [{
          url: "https://github.com/org/repo/pull/1", mergedAt: "2026-08-01T00:00:00Z",
          author: { login: "author" }, reviews: { nodes: [], pageInfo: pageInfo() },
        }],
      } } } } });
    };
    const result = await blameFile(context, file);
    assert.equal(result.complete, true);
    assert.equal(result.candidates.reduce((n, c) => n + c.line_count, 0), 2);
    assert.deepEqual(requested, ["relevant"]);
    globalThis.fetch = async () => Response.json({}, { status: 502 });
    await assert.rejects(blameFile(context, file), /HTTP 502/);
    globalThis.fetch = async () => Response.json({ data: { repository: { object: null } } });
    assert.equal((await blameFile(context, file)).complete, false);
  } finally {
    globalThis.fetch = fetch;
    if (token === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = token;
  }
});
test("GraphQL rejects partial-error payloads instead of returning partial metrics", async () => {
  const fetch = globalThis.fetch;
  try {
    globalThis.fetch = async () =>
      Response.json({
        data: { partial: true },
        errors: [{ message: "unavailable" }],
      });
    await assert.rejects(
      graphql("https://example.test/graphql", "fixture", "query {}"),
      /unavailable/,
    );
  } finally {
    globalThis.fetch = fetch;
  }
});
test("GitHub rejection reasons are classified without exposing response secrets", async () => {
  const fetch = globalThis.fetch;
  try {
    for (const [body, reason] of [
      [
        { message: "You have exceeded a secondary rate limit. private-token" },
        "rate limit",
      ],
      [
        { message: "Please specify a User-Agent header" },
        "User-Agent required",
      ],
      [{ message: "SAML authorization required" }, "organization SSO required"],
      [{ message: "private-token" }, "request rejected"],
      [null, "request rejected"],
    ] as const) {
      globalThis.fetch = async () => Response.json(body, { status: 403 });
      await assert.rejects(
        requestJson("https://api.github.com/graphql", {}, "GitHub"),
        (error: unknown) => {
          assert.ok(error instanceof UpstreamError);
          assert.equal(error.status, 403);
          assert.equal(
            error.message,
            `GitHub unavailable (HTTP 403): ${reason}`,
          );
          assert.ok(!error.message.includes("private-token"));
          return true;
        },
      );
    }
  } finally {
    globalThis.fetch = fetch;
  }
});
test("bounded fan-out preserves input order", async () => {
  let active = 0,
    peak = 0;
  const values = await mapConcurrent([1, 2, 3, 4, 5], 2, async (n) => {
    active++;
    peak = Math.max(peak, active);
    await Promise.resolve();
    active--;
    return n * 2;
  });
  assert.deepEqual(values, [2, 4, 6, 8, 10]);
  assert.equal(peak, 2);
});

test("GitHub rate-limit errors keep the wait the service asked for", async () => {
  const { requestJson, UpstreamError } = await import("../lib/http");
  const original = globalThis.fetch;
  try {
    globalThis.fetch = async () =>
      new Response(JSON.stringify({ message: "You have exceeded a secondary rate limit." }), {
        status: 403,
        headers: { "retry-after": "60" },
      });
    const error = await requestJson("https://api.github.com/graphql", {}, "GitHub").catch((e) => e);
    assert.ok(error instanceof UpstreamError);
    assert.equal(error.reason, "rate limit");
    assert.equal(error.retryAfter, 60);
    const reset = Math.floor(Date.now() / 1000) + 120;
    globalThis.fetch = async () =>
      new Response(JSON.stringify({ message: "API rate limit exceeded" }), {
        status: 403,
        headers: { "x-ratelimit-remaining": "0", "x-ratelimit-reset": String(reset) },
      });
    const primary = await requestJson("https://api.github.com/graphql", {}, "GitHub").catch((e) => e);
    assert.ok(primary instanceof UpstreamError);
    assert.ok(primary.retryAfter! >= 119 && primary.retryAfter! <= 120);
  } finally {
    globalThis.fetch = original;
  }
});
