import { test } from "node:test";
import assert from "node:assert/strict";
import {
  approvalAge, compactOpenIssues, customerImpact, duration, durationStats, feedbackSeconds,
  firstReviewSeconds, issueStageTimes, mergeBlocker, mergeToProduction, recovery, stageSummary,
  teamIssue, teamPR,
} from "../lib/pipeline";
import { deliveryFacts, humanReview, prCounts, type DeliveryNode } from "../lib/github";
import type { Issue, IssueState, PullRequest } from "../lib/types";

const day = 86400, now = Date.parse("2026-09-08T00:00:00Z");
const todo = { id: "todo", name: "Todo", type: "unstarted" };
const dev = { id: "dev", name: "In Progress", type: "started" };
const review = { id: "review", name: "In Review", type: "started" };
const done = { id: "done", name: "Done", type: "completed" };
function transition(fromState: IssueState, toState: IssueState, date: string) {
  return { node: { fromState, toState, toAssignee: null, createdAt: date, updatedAt: date } };
}
function issue(overrides: Partial<Issue> = {}): Issue {
  return {
    id: "i", identifier: "APO-1", number: 1, title: "Work", url: "https://linear.app/i",
    createdAt: "2026-09-01T00:00:00Z", updatedAt: "2026-09-07T23:59:00Z",
    priority: 2, assignee: { name: "zach" }, project: { id: "p", name: "Project" },
    labels: { nodes: [] }, state: dev, ...overrides,
  };
}
function pr(overrides: Partial<PullRequest> = {}): PullRequest {
  return {
    id: "pr", number: 1, title: "PR", url: "https://github.com/a/b/pull/1",
    createdAt: "2026-09-01T00:00:00Z", mergedAt: "2026-09-07T00:00:00Z",
    author: { login: "solideo-gloria" }, reviews: { nodes: [], pageInfo: { hasNextPage: false, endCursor: null } },
    readiness: { nodes: [] }, additions: 1, deletions: 0, baseRefName: "main", headRefName: "work",
    mergeable: "MERGEABLE", reviewDecision: "APPROVED", statusCheckRollup: { state: "SUCCESS" },
    repository: { nameWithOwner: "a/b", defaultBranchRef: { name: "main" } }, reviewRequests: { nodes: [] },
    ...overrides,
  };
}

test("stage duration sums repeat visits per completed issue, and excludes terminal statuses", () => {
  const work = issue({ state: done, completedAt: "2026-09-07T00:00:00Z", history: { edges: [
    transition(review, done, "2026-09-07T00:00:00Z"),
    transition(dev, review, "2026-09-06T00:00:00Z"),
    transition(review, dev, "2026-09-05T00:00:00Z"),
    transition(dev, review, "2026-09-04T00:00:00Z"),
    transition(todo, dev, "2026-09-02T00:00:00Z"),
  ] } });
  const timing = issueStageTimes(work, now)!;
  assert.equal(timing.totals.get("todo")?.seconds, day);
  assert.equal(timing.totals.get("dev")?.seconds, 3 * day);
  assert.equal(timing.totals.get("review")?.seconds, 2 * day);
  assert.equal(timing.totals.has("done"), false);
  const rows = stageSummary([work], [], now).rows;
  assert.equal(rows.find((row) => row.state.id === "dev")?.count, 1);
  assert.equal(rows.find((row) => row.state.id === "dev")?.median, 3 * day);
  const compact = { ...work, history: undefined, stageDurations: [...timing.totals.values()] };
  assert.deepEqual(stageSummary([compact], [], now), stageSummary([work], [], now));
});
test("status labels merge across transferred workflow IDs without double-counting samples", () => {
  const otherDev = { ...dev, id: "other-dev" };
  const work = issue({ state: done, completedAt: "2026-09-04T00:00:00Z", history: { edges: [
    transition(dev, otherDev, "2026-09-02T00:00:00Z"),
    transition(otherDev, done, "2026-09-04T00:00:00Z"),
  ] } });
  const summary = stageSummary([work], [issue({ state: otherDev, history: { edges: [] } })], now);
  assert.equal(summary.rows.length, 1);
  assert.equal(summary.rows[0].count, 1);
  assert.equal(summary.rows[0].median, 3 * day);
  assert.equal(summary.rows[0].current, 1);
});
test("current age uses latest status entry, not issue updates or total prior visits", () => {
  const work = issue({ history: { edges: [
    transition(todo, dev, "2026-09-02T00:00:00Z"),
    transition(dev, review, "2026-09-04T00:00:00Z"),
    transition(review, dev, "2026-09-07T12:00:00Z"),
  ] } });
  const timing = issueStageTimes(work, now)!;
  assert.equal(timing.age, day / 2);
  assert.equal(timing.totals.get("dev")?.seconds, 2.5 * day);
  assert.equal(issueStageTimes(issue({ history: { edges: [] } }), now)?.age, 7 * day);
});
test("compact open snapshots discard timelines but keep a clock that ages after caching", () => {
  const work = issue({ history: { edges: [transition(todo, dev, "2026-09-02T00:00:00Z")] } });
  const [compact] = compactOpenIssues([work]);
  assert.equal(compact.history, undefined);
  assert.equal(compact.stageEnteredAt, "2026-09-02T00:00:00.000Z");
  assert.equal(stageSummary([], [compact], now).rows[0].oldest[0].seconds, 6 * day);
  assert.equal(stageSummary([], [compact], now + day * 1000).rows[0].oldest[0].seconds, 7 * day);
  assert.equal(compactOpenIssues([issue()])[0].stageEnteredAt, null);
});
test("reopened work does not count its previous completed interval as active work", () => {
  const work = issue({ state: done, completedAt: "2026-09-07T00:00:00Z", history: { edges: [
    transition(dev, done, "2026-09-03T00:00:00Z"),
    transition(done, dev, "2026-09-06T00:00:00Z"),
    transition(dev, done, "2026-09-07T00:00:00Z"),
  ] } });
  assert.equal(issueStageTimes(work, now)?.totals.get("dev")?.seconds, 3 * day);
});
test("missing, invalid, and inconsistent histories are unknown; current queues still count them", () => {
  for (const work of [
    issue(),
    issue({ history: { edges: [transition(todo, review, "2026-09-02")] } }),
    issue({ history: { edges: [transition(todo, dev, "invalid")] } }),
    issue({ history: { edges: [transition(todo, dev, "2026-09-09")] } }),
    issue({ history: { edges: [transition(todo, review, "2026-09-02"), transition(dev, dev, "2026-09-03")] } }),
  ]) assert.equal(issueStageTimes(work, now), null);
  const summary = stageSummary([issue()], [issue()], now);
  assert.equal(summary.unknownCompleted, 1);
  assert.equal(summary.unknownOpen, 1);
  assert.equal(summary.rows[0].count, 0);
  assert.equal(summary.rows[0].current, 1);
  assert.equal(summary.rows[0].oldest[0].seconds, null);
});
test("oldest stage items are bounded and sorted; median and nearest-rank p85 retain subday precision", () => {
  const open = Array.from({ length: 20 }, (_, i) => issue({ id: String(i),
    createdAt: new Date(now - (i + 1) * 3600000).toISOString(), history: { edges: [] },
  }));
  const row = stageSummary([], open, now).rows[0];
  assert.equal(row.current, 20);
  assert.equal(row.oldest.length, 5);
  assert.equal(row.oldest[0].seconds, 20 * 3600);
  assert.deepEqual(durationStats([1, 2, 3, 4, NaN, -1]), { count: 4, median: 2.5, p85: 4 });
  assert.deepEqual(durationStats([]), { count: 0, median: null, p85: null });
  assert.equal(duration(5400), "1.5h");
  assert.equal(duration(null), "Unknown");
});
test("first review starts when a draft is ready, ignores bots/self/pending, and includes comments and changes requested", () => {
  const work = pr({ firstHumanReviewAt: "2026-09-03T01:00:00Z", readiness: { nodes: [{ __typename: "ReadyForReviewEvent", createdAt: "2026-09-03T00:00:00Z" }] },
    reviews: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [
      { author: { login: "bot", __typename: "Bot" }, state: "APPROVED", submittedAt: "2026-09-03T00:05:00Z" },
      { author: { login: "solideo-gloria" }, state: "COMMENTED", submittedAt: "2026-09-03T00:10:00Z" },
      { author: { login: "human" }, state: "PENDING", submittedAt: "2026-09-03T00:15:00Z" },
      { author: { login: "human" }, state: "COMMENTED", submittedAt: "2026-09-03T01:00:00Z" },
      { author: { login: "human" }, state: "CHANGES_REQUESTED", submittedAt: "2026-09-04T00:00:00Z" },
    ] },
  });
  assert.deepEqual(work.reviews.nodes.map((r) => humanReview(r, work)), [false, false, false, true, true]);
  assert.equal(firstReviewSeconds(work), 3600);
  assert.equal(firstReviewSeconds({ ...work, readiness: undefined }), null);
  assert.equal(firstReviewSeconds(pr()), null);
  assert.equal(firstReviewSeconds({ ...work, readiness: { nodes: [] } }), 2 * day + 3600);
  assert.equal(firstReviewSeconds({ ...work, readiness: { nodes: [{ __typename: "ConvertToDraftEvent", createdAt: "2026-09-02" }] } }), 2 * day + 3600);
});
test("approved aging uses latest approval, never review-request timestamps; blockers stay visible", () => {
  const work = pr({ reviews: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [
    { author: { login: "a" }, state: "APPROVED", submittedAt: "2026-09-03T00:00:00Z" },
    { author: { login: "b" }, state: "APPROVED", submittedAt: "2026-09-07T00:00:00Z" },
  ] }, timelineItems: { nodes: [{ __typename: "ReviewRequestedEvent", createdAt: "2026-09-07T23:59:00Z" }] } });
  assert.equal(approvalAge(work, now), day);
  assert.equal(approvalAge(pr(), now), null);
  assert.equal(mergeBlocker(work), "Awaiting merge");
  assert.equal(mergeBlocker({ ...work, statusCheckRollup: { state: "FAILURE" } }), "CI failing");
  assert.equal(mergeBlocker({ ...work, baseRefName: "parent" }), "Stacked");
  assert.equal(mergeBlocker({ ...work, mergeable: "CONFLICTING" }), "Conflicts");
});
test("pipeline respects team cohort and richer review history does not inflate approval counts", () => {
  assert.equal(teamIssue(issue(), false), true);
  assert.equal(teamIssue(issue({ assignee: null }), false), true);
  assert.equal(teamPR(pr(), false), true);
  assert.equal(teamPR(pr({ author: { login: "outside" } }), true), false);
  const work = pr({ reviews: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [
    { author: { login: "reviewer" }, state: "COMMENTED" },
    { author: { login: "reviewer" }, state: "CHANGES_REQUESTED" },
    { author: { login: "reviewer" }, state: "DISMISSED" },
  ] } });
  assert.equal(prCounts([work], "reviewer").prs_reviewed, 0);
  work.reviews.nodes.push({ author: { login: "reviewer" }, state: "APPROVED" });
  assert.equal(prCounts([work], "reviewer").prs_reviewed, 1);
});

test("delivery separates automated feedback from human decisions and times head-commit CI", () => {
  const ready = "2026-09-01T10:00:00Z";
  const work = pr({ createdAt: "2026-09-01T09:00:00Z", mergedAt: "2026-09-02T00:00:00Z",
    readiness: { nodes: [{ __typename: "ReadyForReviewEvent", createdAt: ready }] } });
  const run = (startedAt: string, completedAt: string, conclusion = "SUCCESS") => ({ startedAt, completedAt, conclusion });
  const suites = (nodes: { createdAt: string; runs: ReturnType<typeof run>[] }[], more = false) => ({ nodes: [{ commit: { checkSuites: {
    nodes: nodes.map(({ createdAt, runs }) => ({ createdAt, checkRuns: { nodes: runs, pageInfo: { hasNextPage: false } } })),
    pageInfo: { hasNextPage: more },
  } } }] });
  const review = (login: string, state: string, submittedAt: string, __typename = "User") => ({ author: { login, __typename }, state, submittedAt });
  const node: DeliveryNode = {
    reviews: { nodes: [
      review("mary-pr-poppins", "APPROVED", "2026-09-01T10:20:00Z"),
      review("hellogerard", "COMMENTED", "2026-09-01T10:30:00Z"),
      review("solideo-gloria", "APPROVED", "2026-09-01T10:40:00Z"),
      review("vinnyjth", "CHANGES_REQUESTED", "2026-09-01T11:00:00Z"),
    ] },
    // The draft's CI verdict landed before ready, so feedback was already waiting; skipped runs never count.
    opened: suites([{ createdAt: "2026-09-01T09:00:00Z", runs: [
      run("2026-09-01T09:00:00Z", "2026-09-01T09:00:00Z", "SKIPPED"),
      run("2026-09-01T09:01:00Z", "2026-09-01T09:02:00Z"),
      run("2026-09-01T09:01:00Z", "2026-09-01T09:05:00Z"),
    ] }]),
    head: suites([
      { createdAt: "2026-09-01T20:00:00Z", runs: [run("2026-09-01T20:02:00Z", "2026-09-01T20:10:00Z"), run("2026-09-01T20:03:00Z", "2026-09-01T20:30:00Z")] },
      { createdAt: "2026-09-01T20:00:30Z", runs: [run("2026-09-01T20:05:30Z", "2026-09-01T20:06:00Z")] },
      // A later label/edit-triggered workflow is a separate run, not CI waiting.
      { createdAt: "2026-09-01T23:00:00Z", runs: [run("2026-09-01T23:00:00Z", "2026-09-01T23:59:00Z")] },
    ]),
  };
  const delivery = deliveryFacts(work, node);
  assert.deepEqual(delivery, {
    automatedAt: "2026-09-01T10:00:00.000Z", decisionAt: "2026-09-01T11:00:00.000Z",
    queue: 300, job: 27 * 60, ci: 30 * 60,
  });
  assert.deepEqual(feedbackSeconds({ ...work, delivery }), { automated: 0, decision: 3600 });
  assert.equal(humanReview(node.reviews.nodes[0], work), false);
  // Without a CI verdict by then, an AI review is the first automated feedback.
  const reviewed = deliveryFacts(work, { ...node, opened: suites([{ createdAt: ready, runs: [run(ready, "2026-09-01T10:45:00Z")] }]) });
  assert.equal(reviewed.automatedAt, "2026-09-01T10:20:00.000Z");
  assert.equal(deliveryFacts(work, { ...node, reviews: { nodes: [] }, opened: suites([{ createdAt: ready, runs: [run(ready, "2026-09-01T10:45:00Z")] }]) }).automatedAt,
    "2026-09-01T10:45:00.000Z");
  // Truncated check pages stay unknown instead of understating waits.
  assert.deepEqual(deliveryFacts(work, { ...node, head: suites([], true) }).ci, null);
  assert.deepEqual(feedbackSeconds(work), { automated: null, decision: null });
});
test("merge-to-production uses verified Linear production releases and never invents samples", () => {
  const release = (completedAt: string | null, isProduction = true) => ({ completedAt, pipeline: { isProduction } });
  const attachment = (url: string) => ({ metadata: { url, status: "merged", linkKind: "closes" } });
  const merged = pr({ url: "https://github.com/ApollosProject/apollos-cluster/pull/1", mergedAt: "2026-09-02T00:00:00Z" });
  const bug = issue({ priority: 1, labels: { nodes: [{ name: "Bug" }] }, createdAt: "2026-09-01T00:00:00Z", completedAt: "2026-09-02T00:00:00Z",
    attachments: { nodes: [attachment("https://github.com/apollosproject/apollos-cluster/pull/1")] },
    releases: { nodes: [release("2026-09-02T12:00:00Z", false), release("2026-09-03T00:00:00Z"), release(null)] } });
  assert.equal(mergeToProduction(bug, [merged]), day);
  assert.equal(mergeToProduction({ ...bug, releases: { nodes: [release(null)] } }, [merged]), null);
  assert.equal(mergeToProduction(bug, []), null);
  assert.deepEqual(recovery([bug, issue()]), { bugs: [bug], fixed: [day], production: [2 * day] });
  assert.equal(customerImpact(bug), false);
  assert.equal(customerImpact({ ...bug, customerTicketCount: 1 }), true);
  assert.equal(customerImpact({ ...bug, needs: { nodes: [{ id: "need" }] } }), true);
});
