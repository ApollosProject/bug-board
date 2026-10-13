import { test } from "node:test";
import assert from "node:assert/strict";
import {
  approvalAge, duration, durationStats, firstReviewSeconds, issueStageTimes,
  mergeBlocker, stageSummary, teamIssue, teamPR,
} from "../lib/pipeline";
import { humanReview, prCounts } from "../lib/github";
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
