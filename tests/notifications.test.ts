import { test } from "node:test";
import assert from "node:assert/strict";
import { notify, priorityMessage, reviewReminders, staleMessage } from "../lib/notifications";
import { reviewRows } from "../lib/reviews";
import type { Issue, PullRequest } from "../lib/types";
import type { Report } from "../lib/reports";
import { timeWindow } from "../lib/window";
import { people } from "../lib/config";

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

function row(number: number, priority: number | null, overrides: Partial<PullRequest> = {}) {
  const pr: PullRequest = {
    id: `pr-${number}`,
    number,
    title: `PR ${number}`,
    url: `https://github.com/apollosproject/apollos-platforms/pull/${number}`,
    createdAt: "2026-10-01T00:00:00Z",
    updatedAt: "2026-10-13T12:00:00Z",
    timelineCount: { totalCount: 2 },
    author: { login: "solideo-gloria" },
    headRefName: `fix/APO-${number}`,
    additions: 5_000,
    deletions: 0,
    baseRefName: "main",
    repository: { nameWithOwner: "apollosproject/apollos-platforms", defaultBranchRef: { name: "main" } },
    mergeable: "MERGEABLE",
    reviewDecision: "REVIEW_REQUIRED",
    statusCheckRollup: { state: "SUCCESS" },
    reviewRequests: { nodes: [{ requestedReviewer: { login: "redreceipt" } }, { requestedReviewer: { login: "nlewis84" } }] },
    reviews: { nodes: [], pageInfo: { hasNextPage: false, endCursor: null } },
    timelineItems: { nodes: [
      { __typename: "ReviewRequestedEvent", createdAt: "2026-10-10T12:00:00Z", requestedReviewer: { login: "redreceipt" } },
      { __typename: "ReviewRequestedEvent", createdAt: "2026-10-13T12:00:00Z", requestedReviewer: { login: "nlewis84" } },
    ] },
    ...overrides,
  };
  return reviewRows([pr], new Map(priority === null ? [] : [[number, { ...issue(number), priority }]]))[0];
}

test("Slack digests preserve complete lines and claim delivery per part", async (t) => {
  const env = {
    VERCEL_ENV: "preview",
    UPSTASH_REDIS_REST_URL: "https://redis.example.test",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis",
    SLACK_WEBHOOK_URL: "https://slack.example.test",
    APP_URL: "https://board.example.test",
    GITHUB_TOKEN: "fixture-github",
    LINEAR_API_KEY: "fixture-linear",
  };
  const previous = Object.fromEntries(Object.keys(env).map((key) => [key, process.env[key]]));
  Object.assign(process.env, env);
  const claims = new Map<string, string>();
  const snapshots = new Map<string, string>();
  const posts: string[] = [];
  const postedAt: number[] = [];
  const claimedAt: number[] = [];
  let issues: Issue[] = [];
  let prs: PullRequest[] = [];
  let report: Report | null = null;
  let failure: "rejected" | "ambiguous" | undefined;
  let failAt = 0;
  t.mock.method(globalThis, "fetch", async (url: unknown, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (String(url) === "https://api.github.com/graphql") {
      if (body.query.includes("query Timeline"))
        return Response.json({ data: { node: { timelineItems: {
          ...prs.find((pr) => pr.id === body.variables.id)!.timelineItems,
          pageInfo: { hasPreviousPage: false, startCursor: null },
        } } } });
      const nodes = prs.filter((pr) => body.variables.query.includes(`repo:${pr.repository.nameWithOwner} `));
      assert.ok(!body.variables.query.includes("-review:approved"));
      return Response.json({ data: { search: { issueCount: nodes.length, pageInfo: { hasNextPage: false, endCursor: null }, nodes } } });
    }
    if (String(url) === "https://api.linear.app/graphql") {
      const numbers = body.variables.filter.number?.in || [];
      return Response.json({ data: { issues: {
        nodes: issues.filter((i) => numbers.includes(i.number)),
        pageInfo: { hasNextPage: false, endCursor: null },
      } } });
    }
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
      const records = key.includes(":notification:") ? claims : snapshots;
      if (op === "get") return key.endsWith(":open-issues")
        ? JSON.stringify({ at: Date.now(), data: issues })
        : key.endsWith(":projects") ? JSON.stringify({ at: Date.now(), data: [] })
        : key.endsWith(":metrics") ? report && JSON.stringify({ at: Date.now(), data: report })
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
    await t.test("stale issues include projects, skip sub-issues, and keep age or assignee gates", async () => {
      const project = { id: "project", name: "Backlog cleanup" };
      issues = [
        issue(2, "Standalone stale"),
        { ...issue(3, "Project stale"), project },
        { ...issue(4, "Project merged"), project, state: { name: "Merged", type: "started" } },
        { ...issue(5, "Project parked"), project, state: { name: "Not Now", type: "backlog" } },
        { ...issue(6, "Project recent"), project, updatedAt: new Date(Date.now() - 21 * 86_400_000).toISOString() },
        { ...issue(7, "Project unassigned"), project, assignee: null },
        { ...issue(8, "Project outside engineering"), project, assignee: { name: "vincent", displayName: "vincent" } },
        { ...issue(10, "Stale sub-issue"), parent: { id: "2" } },
        { ...issue(11, "Top-level null parent"), parent: null },
      ];
      const message = (await staleMessage())!;
      assert.match(message, /\*Stale Open Issues\*/);
      assert.match(message, /<@U0BLNNPAB> \(avg completion: n\/a, last 30d\):/);
      for (const title of ["Standalone stale", "Project stale", "Project merged", "Project parked", "Top-level null parent"])
        assert.ok(message.includes(`|${title}>`), title);
      for (const title of ["Project recent", "Project unassigned", "Project outside engineering", "Stale sub-issue"])
        assert.ok(!message.includes(title), title);
    });
    await t.test("person headings reuse the metrics snapshot for both 30-day averages", async () => {
      const work = issue(9);
      const reviewed = row(9, 2).pr;
      report = { window: timeWindow({ days: "30" }), rows: [], projects: [],
        completed: [{ ...work, completedAt: "2026-01-05T00:00:00Z",
          history: { edges: [{ node: { toAssignee: work.assignee, updatedAt: "2026-01-02T00:00:00Z" } }] } }],
        prs: [{ ...reviewed, author: { login: "other-author" }, readiness: { nodes: [] },
          decisionReviewsComplete: true, mergedAt: "2026-10-03T00:00:00Z", reviews: {
            nodes: [{ author: { login: "redreceipt" }, state: "CHANGES_REQUESTED", submittedAt: "2026-10-01T06:00:00Z" }],
            pageInfo: { hasNextPage: false, endCursor: null },
          } }],
      };
      issues = [{ ...work, priority: 2 }];
      prs = [{ ...reviewed, timelineItems: { nodes: [{
        __typename: "ReviewRequestedEvent", createdAt: new Date(Date.now() - 3 * 86400000).toISOString(),
        requestedReviewer: { login: "redreceipt" },
      }] } }];
      const message = (await staleMessage())!;
      assert.ok(message.includes("<@U01865757S9> (avg review decision: 6.0h, last 30d):"));
      assert.ok(message.includes("<@U0BLNNPAB> (avg completion: 3d, last 30d):"));
      report = null;
      const unavailable = (await staleMessage())!;
      assert.ok(unavailable.includes("(avg review decision: n/a, last 30d)"));
      assert.ok(unavailable.includes("(avg completion: n/a, last 30d)"));
      prs = [];
    });
    await t.test("priority bugs label and mention only reviewers eligible for PR reminders", async () => {
      const old = new Date(Date.now() - 3 * 86_400_000).toISOString();
      const requested = (login: string, createdAt = old) => ({
        __typename: "ReviewRequestedEvent", createdAt, requestedReviewer: { login },
      });
      const cases = [
        row(20, 1),
        row(21, 2, { statusCheckRollup: null }),
        row(22, 1, { baseRefName: "stacked" }),
        row(23, 1, { mergeable: "CONFLICTING" }),
        row(24, 1, { statusCheckRollup: { state: "FAILURE" } }),
        row(25, 1, { statusCheckRollup: { state: "PENDING" } }),
        row(26, 1, { reviewDecision: "APPROVED" }),
        row(27, 1, { reviews: { nodes: [{ author: { login: "redreceipt" }, state: "APPROVED", submittedAt: new Date().toISOString() }], pageInfo: { hasNextPage: false, endCursor: null } } }),
        row(28, 1, { timelineItems: { nodes: [] } }),
        row(29, 1, { reviewRequests: { nodes: [{ requestedReviewer: { login: "outsider" } }] } }),
        row(30, 1, { reviewDecision: "CHANGES_REQUESTED", reviews: { nodes: [{ author: { login: "nlewis84" }, state: "CHANGES_REQUESTED", submittedAt: old }], pageInfo: { hasNextPage: false, endCursor: null } } }),
        row(31, 1),
        row(32, 1),
      ];
      prs = cases.map(({ pr }) => ({
        ...pr,
        timelineItems: pr.number === 28 ? pr.timelineItems : { nodes: [
          requested("redreceipt", pr.number === 32 ? new Date().toISOString() : old),
          requested("nlewis84", pr.number === 20 ? old : new Date().toISOString()),
          requested("outsider"),
        ] },
      }));
      // A second PR waiting on the same person must not duplicate their mention.
      prs.push({ ...prs[0], id: "duplicate-pr", number: 100, url: "https://github.com/apollosproject/apollos-platforms/pull/100" });
      issues = cases.map(({ issue: i }) => ({
        ...i!, assignee: { name: "zach", displayName: "zach" },
        slaBreachesAt: old, labels: { nodes: [{ name: "Bug" }] },
      }));
      issues.find((i) => i.number === 31)!.slaHighRiskAt = old;
      issues.find((i) => i.number === 31)!.slaBreachesAt = new Date(Date.now() + 86_400_000).toISOString();
      const message = (await priorityMessage())!;
      assert.match(message, /\*Overdue\*/);
      assert.match(message, /\*At Risk\*/);
      for (const i of issues) {
        const line = message.split("\n").find((line) => line.includes(`<${i.url}|`))!;
        assert.ok(line.includes("<@U0BLNNPAB>"), "Keep the assignee");
        assert.equal(line.includes("Review requested:"), [20, 21, 31].includes(i.number), i.identifier);
        assert.equal((line.match(/<@U01865757S9>/g) || []).length, [20, 21, 31].includes(i.number) ? 1 : 0, "Mention each eligible reviewer once");
        if (i.number === 20)
          assert.match(line, /Review requested: <@U01865757S9>, <@U022N4RTD40>/);
        else if ([21, 31].includes(i.number))
          assert.match(line, /Review requested: <@U01865757S9>\)$/);
      }
      prs = [];
    });
    await t.test("priority bugs retain assignees without a configured Slack mention", async () => {
      const overdue = new Date(Date.now() - 86_400_000).toISOString();
      issues = [
        { ...issue(40), assignee: { name: "Medic", displayName: "medic" } },
        { ...issue(41), assignee: { name: "Metametic" } },
        { ...issue(42), assignee: { displayName: "Outside <@U123> & team | `name`" } },
        { ...issue(43), assignee: { name: "zach", displayName: "zach" } },
        { ...issue(44), assignee: null },
      ].map((i) => ({ ...i, priority: 2, slaBreachesAt: overdue, labels: { nodes: [{ name: "Bug" }] } }));
      const slackId = people.zach.slack_id;
      people.zach.slack_id = "";
      try {
        const message = (await priorityMessage())!;
        const lines = issues.map((i) => message.split("\n").find((line) => line.includes(`<${i.url}|`))!);
        assert.ok(lines[0].endsWith(", medic)"));
        assert.ok(lines[1].endsWith(", Metametic)"));
        assert.ok(lines[2].endsWith(", Outside &lt;@U123&gt; &amp; team - -name-)"));
        assert.ok(lines[3].endsWith(", zach)"));
        assert.ok(!lines[4].includes("Assignee"));
        assert.ok(!message.includes("No Assignee"));
        const unassigned = message.split("*Unassigned Priority Bugs*\n\n")[1].split("\n\n")[0];
        assert.ok(unassigned.includes(issues[4].url));
        for (const i of issues.slice(0, 4)) assert.ok(!unassigned.includes(i.url));
      } finally {
        people.zach.slack_id = slackId;
      }
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

test("review reminders keep the old gates with Urgent/High priority instead of size", () => {
  const now = Date.parse("2026-10-13T14:00:00Z");
  const message = reviewReminders([
    row(1, 1),
    row(2, 2, { title: "High <one>" }),
    row(3, 3),
    row(4, 0),
    row(5, null),
    row(6, 1, { baseRefName: "stacked" }),
    row(7, 1, { mergeable: "CONFLICTING" }),
    row(8, 1, { statusCheckRollup: { state: "PENDING" } }),
    row(9, 1, { statusCheckRollup: null }),
    row(11, 1, { reviewDecision: "APPROVED" }),
    row(10, 1, { reviews: { nodes: [{ author: { login: "redreceipt" }, state: "APPROVED", submittedAt: "2026-10-11T00:00:00Z" }], pageInfo: { hasNextPage: false, endCursor: null } } }),
  ], now);
  assert.equal(message, [
    "*PRs - Checks Passing, Waiting for Review (+24h, Urgent/High priority)*",
    "",
    "<@U01865757S9> (avg review decision: n/a, last 30d):",
    "- <https://github.com/apollosproject/apollos-platforms/pull/1|PR 1> (+3d)",
    "- <https://github.com/apollosproject/apollos-platforms/pull/2|High &lt;one&gt;> (+3d)",
    "- <https://github.com/apollosproject/apollos-platforms/pull/9|PR 9> (+3d)",
  ].join("\n"));
  assert.equal(reviewReminders([row(3, 3)], now), null);
});
