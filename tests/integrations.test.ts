import { test } from "node:test";
import assert from "node:assert/strict";
import { fetchIssues, projects } from "../lib/linear";
import { mergedPRs } from "../lib/github";
import { mapConcurrent, graphql } from "../lib/http";
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
