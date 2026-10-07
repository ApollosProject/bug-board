import { test } from "node:test";
import assert from "node:assert/strict";
import { fetchIssues, projects } from "../lib/linear";
import { mergedPRs } from "../lib/github";
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
