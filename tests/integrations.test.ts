import { test } from "node:test";
import assert from "node:assert/strict";
import { fetchIssues, projects } from "../lib/linear";
import { mergedPRs } from "../lib/github";
import { blameFile } from "../lib/regressions";
import {
  mapConcurrent,
  graphql,
  requestJson,
  UpstreamError,
} from "../lib/http";
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
