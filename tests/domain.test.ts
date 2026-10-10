import { test } from "node:test";
import assert from "node:assert/strict";
import { timeWindow } from "../lib/window";
import {
  assignmentDays,
  blocked,
  comparison,
  csv,
  done,
  inactive,
  isCardBug,
  isPriorityBug,
  personMetrics,
  plannedWeeks,
  projectScoringWeeks,
  supportSlugs,
  teamRows,
} from "../lib/metrics";
import { classify, reviewRows, ticketNumber } from "../lib/reviews";
import { creditedAuthors } from "../lib/github";
import { fleetStats } from "../lib/fleet";
import {
  appIdentity,
  deployTarget,
  revisionsMatch,
  selectMobile,
  selectObserved,
} from "../lib/apps";
import { publishedAndroid, publishedApple } from "../lib/stores";
import {
  deletedLines,
  mergeAttribution,
  regressionMetrics,
} from "../lib/regressions";
import { calendarURL, parseCalendar } from "../lib/pto";
import { scheduledNotifications } from "../lib/notifications";
import type { AppRow, Issue, Project, PullRequest } from "../lib/types";
const now = Date.parse("2026-09-07T12:00:00Z"),
  window = timeWindow({ days: "7" }, now);
const pageInfo = { hasNextPage: false, endCursor: null };
function issue(overrides: Partial<Issue> = {}): Issue {
  return {
    id: "i1",
    identifier: "APO-1",
    number: 1,
    title: "Fix bug",
    url: "https://linear.app/differential/issue/APO-1",
    priority: 2,
    createdAt: "2026-09-01T00:00:00Z",
    updatedAt: "2026-09-06T00:00:00Z",
    completedAt: "2026-09-06T00:00:00Z",
    assignee: { displayName: "zach", name: "zach" },
    project: null,
    state: { name: "Done", type: "completed" },
    labels: { nodes: [{ name: "Bug" }] },
    ...overrides,
  };
}
function project(overrides: Partial<Project> = {}): Project {
  return {
    id: "p1",
    name: "Project",
    url: "https://linear.app/p1",
    status: { name: "Completed", type: "completed" },
    lead: { displayName: "Zach" },
    startDate: "2026-09-01",
    targetDate: "2026-09-07",
    completedAt: "2026-09-06T00:00:00Z",
    members: { nodes: [] },
    initiatives: { nodes: [] },
    ...overrides,
  };
}
function pr(overrides: Partial<PullRequest> = {}): PullRequest {
  return {
    id: "pr1",
    number: 1,
    title: "APO-1",
    url: "https://github.com/apollosproject/repo/pull/1",
    createdAt: "2026-09-01T00:00:00Z",
    author: { login: "solideo-gloria" },
    reviews: { nodes: [], pageInfo },
    baseRefName: "main",
    headRefName: "fix/APO-1",
    additions: 10,
    deletions: 5,
    mergeable: "MERGEABLE",
    reviewDecision: null,
    statusCheckRollup: { state: "SUCCESS" },
    repository: {
      nameWithOwner: "apollosproject/repo",
      defaultBranchRef: { name: "main" },
    },
    reviewRequests: { nodes: [] },
    ...overrides,
  };
}
const app: AppRow = {
  church: "demo",
  build_church: "preview",
  deploy_target_count: 1,
  apollos_platform: "ios",
  application_name: "Demo",
  bundle_id: "com.demo.app",
  apollos_version: "101",
  native_build: "20",
  native_version: "1.0",
  app_version: "1.0",
};
test("Ready projects honor prerequisite projects and milestone dependencies", () => {
  const dependency = (prerequisite: NonNullable<Project["inverseRelations"]>["nodes"][number]["project"], milestone: string | null = null, type = "dependency") =>
    project({ inverseRelations: { nodes: [{ type, project: prerequisite, projectMilestone: milestone === null ? null : { status: milestone } }] } });
  const active = { status: { name: "In Progress", type: "started" } },
    completed = { status: { name: "Completed", type: "completed" } };
  assert.equal(blocked(project()), false);
  assert.equal(blocked(dependency(active)), true);
  assert.equal(blocked(dependency(null)), true);
  assert.equal(blocked(dependency(active, null, "related")), false);
  assert.equal(blocked(dependency(completed)), false);
  assert.equal(blocked(dependency({ status: { name: "Released", type: "started" } })), false);
  assert.equal(blocked(dependency({ status: { name: "Custom", type: "completed" } })), false);
  assert.equal(blocked(dependency({ status: { name: "Canceled", type: "canceled" }, completedAt: "2026-09-01" })), false);
  assert.equal(blocked(dependency({ status: { name: "Canceled", type: "canceled" } })), true);
  assert.equal(blocked(dependency(active, "done")), false);
  assert.equal(blocked(dependency(completed, "next")), true);
  const mixed = dependency(completed);
  mixed.inverseRelations!.nodes.push(...dependency(active).inverseRelations!.nodes);
  assert.equal(blocked(mixed), true);
});
test("windows use inclusive calendar days and UTC exclusive upper bounds", () => {
  const w = timeWindow({ start: "2026-09-07", end: "2026-09-01" }, now);
  assert.equal(w.days, 7);
  assert.equal(w.after, "2026-09-01T00:00:00.000Z");
  assert.equal(w.before, "2026-09-08T00:00:00.000Z");
  assert.equal(
    timeWindow({ start: "2026-02-30", end: "2026-03-01" }, now).preset_days,
    30,
  );
  assert.equal(timeWindow({ days: "-1" }, now).days, 30);
  assert.throws(() =>
    timeWindow({ start: "2020-01-01", end: "2026-01-01" }, now),
  );
});
test("assignment time uses the latest matching assignment before completion", () => {
  const i = issue({
    history: {
      edges: [
        {
          node: {
            toAssignee: { displayName: "zach" },
            updatedAt: "2026-09-01T00:00:00Z",
          },
        },
        {
          node: {
            toAssignee: { displayName: "zach" },
            updatedAt: "2026-09-04T00:00:00Z",
          },
        },
        {
          node: {
            toAssignee: { displayName: "zach" },
            updatedAt: "2026-09-08T00:00:00Z",
          },
        },
      ],
    },
  });
  assert.equal(assignmentDays(i), 2);
  assert.equal(assignmentDays(issue()), null);
  assert.equal(isPriorityBug(issue({ priority: 0 })), false);
});
test("person bug cards preserve unprioritized counts without widening alerts or regression candidates", () => {
  const unprioritized = issue({ priority: 0 });
  assert.equal(isCardBug(unprioritized), true);
  assert.equal(isPriorityBug(unprioritized), false);
  assert.equal(isCardBug(issue({ priority: 3 })), false);
  assert.equal(isCardBug(issue({ labels: { nodes: [] } })), false);
  assert.equal(
    personMetrics("zach", [unprioritized, issue()], [], [], window)
      .priority_bugs_fixed,
    2,
  );
});
test("completed projects, incomplete projects, and scheduled weeks remain distinct", () => {
  assert.equal(plannedWeeks(project()), 1);
  assert.equal(
    plannedWeeks(
      project({ startDate: "2026-09-15", targetDate: "2026-09-01" }),
    ),
    2,
  );
  assert.equal(
    done(project({ status: { name: "Incomplete", type: "canceled" } })),
    false,
  );
  assert.equal(
    inactive(project({ status: { name: "Incomplete", type: "canceled" } })),
    true,
  );
  assert.equal(projectScoringWeeks(project(), window), 1);
  assert.equal(
    projectScoringWeeks(project({ targetDate: "2026-08-01" }), window),
    0,
  );
});
test("support includes only engineers without started lead or member assignments, even overdue", () => {
  const active = project({
    completedAt: undefined,
    status: { name: "In Progress", type: "started" },
    startDate: "2026-08-01",
    targetDate: "2026-08-02",
    members: { nodes: [{ displayName: "michael.neeley" }] },
  });
  assert.equal(supportSlugs([active], now).includes("zach"), false);
  assert.equal(supportSlugs([active], now).includes("michael"), false);
  assert.equal(supportSlugs([active], now).includes("andy"), false);
  assert.equal(
    supportSlugs(
      [
        project({
          completedAt: undefined,
          startDate: undefined,
          status: { name: "Ready", type: "planned" },
        }),
      ],
      now,
    ).includes("zach"),
    true,
  );
});
test("z scores trim tails, reverse lower-is-better metrics, and reject zero spread", () => {
  const values = [1, 2, 3, 4, 1000];
  assert.equal(comparison(4, values)?.eng_avg, 3);
  assert.equal(comparison(4, values, true)?.tone, "low");
  assert.equal(comparison(1, [1, 1, 1]), null);
  assert.equal(comparison(null, [1, 2]), null);
});
test("CSV formats raw z scores once, without rounding through the API's two decimals", () => {
  const rows = teamRows([], [], [], new Map(), window).slice(0, 6);
  [0, 3, 0, 0, 0, 1].forEach((value, i) => { rows[i].urgent_issues = value; });
  assert.equal(comparison(3, [0, 3, 0, 0, 0, 1])?.z, 6.35);
  assert.ok(csv(rows).split("\n")[2].includes('"3","6.4"'));
});
test("person metrics count approval once per PR and never invent missing assignment time", () => {
  const metrics = personMetrics(
    "zach",
    [issue()],
    [
      pr({
        reviews: {
          nodes: [
            { author: { login: "solideo-gloria" }, state: "APPROVED" },
            { author: { login: "solideo-gloria" }, state: "APPROVED" },
          ],
          pageInfo,
        },
      }),
    ],
    [project()],
    window,
  );
  assert.equal(metrics.prs_merged, 1);
  assert.equal(metrics.prs_reviewed, 1);
  assert.equal(metrics.priority_bug_avg_time_to_fix, null);
  assert.equal(metrics.lead_completed_projects, 1);
});
test("project contributors come from completed issues and exclude leads", () => {
  const rows = teamRows(
    [issue()],
    [],
    [project()],
    new Map([["p1", new Set(["zach", "michael.neeley"])]]),
    window,
  );
  assert.equal(rows.find((r) => r.slug === "zach")?.project_lead_weeks, 1);
  assert.equal(
    rows.find((r) => r.slug === "zach")?.project_contributor_weeks,
    0,
  );
  assert.equal(
    rows.find((r) => r.slug === "michael")?.project_contributor_weeks,
    1,
  );
  assert.ok(csv(rows).includes("prs_merged_z"));
  assert.ok(csv(rows).endsWith("\n"));
});
test("Cursor coauthor credit requires both the app and cursoragent primary commit author", () => {
  const cursor = pr({
    author: { login: "cursor" },
    commits: {
      nodes: [
        {
          commit: {
            author: { user: { login: "cursoragent" } },
            authors: {
              nodes: ["cursoragent", "RedReceipt", "redreceipt"].map(
                (login) => ({ user: { login } }),
              ),
            },
          },
        },
      ],
    },
  });
  assert.deepEqual(creditedAuthors(cursor), ["redreceipt"]);
  assert.deepEqual(creditedAuthors({ ...cursor, author: { login: "other" } }), [
    "other",
  ]);
});
for (const [name, input, expected] of [
  ["stack", { baseRefName: "parent" }, "Stacked"],
  ["conflicts", { mergeable: "CONFLICTING" }, "Conflicts"],
  ["failed checks", { statusCheckRollup: { state: "FAILURE" } }, "CI failing"],
] as const)
  test(`review queue blocks ${name}`, () =>
    assert.equal(classify(pr(input)).reason, expected));
test("review queue separates running and approved, with an approval fallback for unprotected repos", () => {
  assert.equal(
    classify(pr({ statusCheckRollup: { state: "PENDING" } })).section,
    "running",
  );
  assert.equal(
    classify(pr({ reviewDecision: "APPROVED", baseRefName: "parent" })).section,
    "approved",
  );
  assert.equal(
    classify(
      pr({
        reviews: {
          nodes: [{ author: { login: "redreceipt" }, state: "APPROVED" }],
          pageInfo,
        },
      }),
    ).section,
    "approved",
  );
  assert.equal(
    classify(
      pr({
        reviews: {
          nodes: [
            { author: { login: "redreceipt" }, state: "CHANGES_REQUESTED" },
          ],
          pageInfo,
        },
      }),
    ).reason,
    "Changes requested",
  );
});
test("review ticket parsing prefers branches and handles underscores without accepting embedded identifiers", () => {
  assert.equal(
    ticketNumber(pr({ headRefName: "feature/APO-123_fix", title: "APO-456" })),
    123,
  );
  assert.equal(
    ticketNumber(pr({ headRefName: "XAPO-123X", title: "No issue" })),
    null,
  );
});
test("mixed review sections preserve priority order and longest approved wait", () => {
  const priorities = [4, 1, 2, 4, 1];
  const prs = priorities.map((_, index) => {
    const number = index + 1;
    return pr({
      id: `pr${number}`,
      number,
      headRefName: `fix/APO-${number}`,
      createdAt: `2026-09-0${number}T00:00:00Z`,
      reviewDecision: [2, 4].includes(number) ? "APPROVED" : null,
    });
  });
  const rows = reviewRows(
    prs,
    new Map(priorities.map((priority, index) => [index + 1, issue({ priority })])),
    now,
  );
  assert.deepEqual(rows.filter((r) => r.section === "ready").map((r) => r.pr.number), [5, 3, 1]);
  assert.deepEqual(rows.filter((r) => r.section === "approved").map((r) => r.pr.number), [2, 4]);
});
test("human review requests reset wait time; bot requests do not", () => {
  const rows = reviewRows(
    [
      pr({
        timelineItems: {
          nodes: [
            {
              __typename: "ReviewRequestedEvent",
              createdAt: "2026-09-07T10:00:00Z",
              requestedReviewer: { login: "redreceipt" },
            },
            {
              __typename: "ReviewRequestedEvent",
              createdAt: "2026-09-07T11:00:00Z",
              requestedReviewer: { login: "bot" },
            },
          ],
        },
      }),
    ],
    new Map([[1, issue()]]),
    now,
  );
  assert.equal(rows[0].waiting, "2h");
  assert.equal(rows[0].size, "XS");
  assert.equal(rows[0].priority, 2);
});
test("fleet failure thresholds use terminal runs while inventory shows latest state", () => {
  const ids = Array.from({ length: 20 }, (_, i) => `dag-${i}`);
  const runs = ids.map((_, i) => ({
    latest: i < 2 ? "running" : "success",
    terminal: i < 2 ? "failed" : "success",
    id: "run",
    hasRuns: true,
  }));
  const stats = fleetStats(ids, runs, now);
  assert.equal(stats.status, "degraded");
  assert.equal(stats.failure_ratio, 0.1);
  assert.equal(stats.dags[0].state, "running");
  assert.equal(stats.dags[0].dag_run_id, "");
  assert.equal(
    fleetStats(ids.slice(0, 19), runs.slice(0, 19)).status,
    "healthy",
  );
  assert.equal(fleetStats(["a"], [null]).status, "unknown");
});
test("mobile runtime requires every published build to have one consistent native-ID match", () => {
  assert.equal(
    selectMobile([app], {
      builds: [{ native_build: "20", native_version: "1.0" }],
    }).apollos_version,
    "101",
  );
  assert.equal(
    selectMobile([app], {
      builds: [{ native_build: "21", native_version: "1.0" }],
    }).apollos_version,
    null,
  );
  assert.equal(
    selectMobile([app, { ...app, apollos_version: "102" }], {
      builds: [{ native_build: "20", native_version: "1.0" }],
    }).apollos_version,
    null,
  );
  assert.equal(
    selectMobile([{ ...app, app_version: "0.9" }], {
      builds: [{ native_build: "20", native_version: "1.0" }],
    }).apollos_version,
    null,
  );
  assert.equal(selectMobile([app]).apollos_version, null);
  assert.equal(
    appIdentity({ ...app, apollos_platform: "android" }) ===
      appIdentity({ ...app, apollos_platform: "androidtv" }),
    false,
  );
});
test("multiple published runtimes are explicit and never claimed current", () => {
  const selected = selectMobile(
    [
      { ...app, apollos_platform: "android" },
      {
        ...app,
        apollos_platform: "android",
        native_build: "21",
        apollos_version: "102",
      },
    ],
    { builds: [{ native_build: "20" }, { native_build: "21" }] },
  );
  assert.equal(selected.apollos_version, null);
  assert.equal(selected.live_runtime_display, "101, 102");
});
test("store parsers accept published releases only and refuse partial Apple inventories", () => {
  assert.deepEqual(
    publishedAndroid(
      {
        releases: [
          {
            track: "production",
            releaseLifecycleState: "RELEASE_LIFECYCLE_STATE_PUBLISHED",
            activeArtifacts: [{ versionCode: 20 }],
          },
          {
            track: "production",
            releaseLifecycleState: "IN_REVIEW",
            activeArtifacts: [{ versionCode: 21 }],
          },
        ],
      },
      "production",
    ),
    [{ native_build: "20" }],
  );
  assert.throws(() => publishedApple({ data: [], links: { next: "next" } }));
  assert.deepEqual(publishedApple({ data: [] }), []);
});
test("deployment targets require a unique verified identity, never generic TV or ambiguous rows", () => {
  assert.ok(deployTarget([app], "ios", app.bundle_id, "preview"));
  assert.equal(deployTarget([app, app], "ios", app.bundle_id, "preview"), null);
  assert.equal(
    deployTarget(
      [{ ...app, deploy_target_count: 2 }],
      "ios",
      app.bundle_id,
      "preview",
    ),
    null,
  );
  assert.equal(
    deployTarget(
      [{ ...app, apollos_platform: "tv" }],
      "tv",
      app.bundle_id,
      "preview",
    ),
    null,
  );
});
test("observed release selection excludes internal, master, and unmatched alpha revisions", () => {
  const source = {
    revisions: { "v2026.09.01.01": "abcdef123456" },
    mobile: null,
    tv: null,
    roku: null,
  };
  const rows = selectObserved(
    [
      {
        ...app,
        apollos_platform: "tvos",
        source_version: "v2026.09.01.01-alpha.1",
        source_revision: "abcdef1",
      },
      {
        ...app,
        apollos_platform: "tvos",
        source_version: "v2026.10.01.01",
        deployment_track: "beta",
      },
    ],
    source,
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].apollos_version, "v2026.09.01.01");
  assert.equal(revisionsMatch("garbage", "garbage"), false);
});
test("regression analysis parses deleted old-file lines, merges scores, and excludes fixing PRs", () => {
  assert.deepEqual(
    deletedLines("@@ -10,3 +10,2 @@\n same\n-old\n+new\n-last"),
    [11, 12],
  );
  const candidate = {
    url: "https://github.com/org/repo/pull/1",
    merged_at: "2026-09-01T00:00:00Z",
    author: "solideo-gloria",
    reviewers: ["redreceipt"],
    line_count: 2,
    score: 1,
  };
  const record = mergeAttribution(
    issue(),
    ["https://github.com/org/repo/pull/2"],
    [
      { candidates: [candidate], complete: true },
      { candidates: [candidate], complete: false },
    ],
  );
  assert.equal(record.attribution?.score, 2);
  assert.equal(record.complete, false);
  const metrics = regressionMetrics(
    [record],
    { zach: { prs_merged: 4, prs_reviewed: 0 } },
    window,
    false,
  );
  assert.equal(metrics.find((m) => m.slug === "zach")?.rate, 25);
});
test("PTO calendar rejects SSRF and observes exclusive all-day end dates", () => {
  assert.throws(() =>
    calendarURL("https://evil.com/api/feed/calendar/pto/token"),
  );
  assert.throws(() =>
    calendarURL("https://app.rippling.com:444/api/feed/calendar/pto/token"),
  );
  assert.equal(
    calendarURL("webcal://app.rippling.com/api/feed/calendar/pto/token")
      .protocol,
    "https:",
  );
  const events = parseCalendar(
    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:1\r\nSUMMARY:Zach OOO\r\nDTSTART;VALUE=DATE:20260901\r\nDTEND;VALUE=DATE:20260903\r\nEND:VEVENT\r\nEND:VCALENDAR",
  );
  assert.equal(events[0].start, "2026-09-01");
  assert.equal(events[0].end, "2026-09-02");
});
test("notification schedule preserves New York time through DST", () => {
  assert.ok(
    scheduledNotifications(new Date("2026-07-01T14:00:00Z")).includes("stale"),
  );
  assert.ok(
    scheduledNotifications(new Date("2026-12-01T15:00:00Z")).includes("stale"),
  );
  assert.ok(
    scheduledNotifications(new Date("2026-09-04T13:00:00Z")).includes(
      "performance",
    ),
  );
});
