import { creditedAuthors, reviewReadyAt } from "./github";
import { engineers, people, slugFor } from "./config";
import { ciState } from "./reviews";
import type { Issue, IssueState, PullRequest } from "./types";

const stageKey = (state: IssueState) => `${state.type}:${state.name}`;
const stateKey = (state: IssueState) => state.id || stageKey(state);
const active = (state: IssueState) => !["completed", "canceled"].includes(state.type);
export const inTeam = (slug: string | null | undefined, everyone: boolean) =>
  !!slug && (everyone ? !!people[slug] : engineers.includes(slug));
export const teamIssue = (issue: Issue, everyone: boolean) =>
  !issue.assignee || inTeam(slugFor(issue.assignee.name, issue.assignee.displayName), everyone);
export const teamPR = (pr: PullRequest, everyone: boolean) => {
  const logins = new Set(creditedAuthors(pr));
  return Object.entries(people).some(([slug, person]) =>
    inTeam(slug, everyone) && logins.has(person.github_username.toLowerCase()),
  );
};

// Status time is elapsed calendar time, not measured hands-on work.
export function issueStageTimes(issue: Issue, now = Date.now()) {
  if (!issue.history) return null;
  const end = issue.completedAt ? Date.parse(issue.completedAt) : now;
  let start = Date.parse(issue.createdAt);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return null;
  const events = issue.history.edges.map(({ node }) => node)
    .filter((node) => node.fromState || node.toState)
    .sort((a, b) => Date.parse(a.createdAt || "") - Date.parse(b.createdAt || ""));
  let state = events.length ? events[0].fromState : issue.state;
  if (!state) return null;
  const totals = new Map<string, { state: IssueState; seconds: number }>();
  const add = (until: number) => {
    if (state && active(state)) {
      const key = stateKey(state), previous = totals.get(key)?.seconds || 0;
      totals.set(key, { state, seconds: previous + (until - start) / 1000 });
    }
  };
  for (const event of events) {
    const at = Date.parse(event.createdAt || "");
    if (!event.fromState || !event.toState || !Number.isFinite(at) || at < start || at > end ||
      stateKey(event.fromState) !== stateKey(state)) return null;
    if (stateKey(event.toState) === stateKey(state)) continue;
    add(at);
    state = event.toState;
    start = at;
  }
  if (stateKey(state) !== stateKey(issue.state)) return null;
  add(end);
  return { totals, age: (end - start) / 1000, enteredAt: start };
}
export function compactOpenIssues(issues: Issue[]) {
  return issues.map((issue) => {
    const timing = issueStageTimes(issue);
    return { ...issue, history: undefined, stageEnteredAt: timing ? new Date(timing.enteredAt).toISOString() : null };
  });
}

export function durationStats(values: number[]) {
  const sorted = values.filter((n) => Number.isFinite(n) && n >= 0).sort((a, b) => a - b);
  const n = sorted.length, middle = Math.floor(n / 2);
  return {
    count: n,
    median: n ? (n % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2) : null,
    p85: n ? sorted[Math.ceil(n * 0.85) - 1] : null,
  };
}
export function stageSummary(completed: Issue[], open: Issue[], now = Date.now()) {
  const stages = new Map<string, {
    state: IssueState;
    durations: number[];
    items: { issue: Issue; seconds: number | null }[];
  }>();
  const stage = (state: IssueState) => {
    const key = stageKey(state);
    if (!stages.has(key)) stages.set(key, { state, durations: [], items: [] });
    return stages.get(key)!;
  };
  let unknownCompleted = 0, unknownOpen = 0;
  for (const issue of completed) {
    const totals = issue.stageDurations === undefined ? issueStageTimes(issue, now)?.totals.values() : issue.stageDurations;
    if (!totals) { unknownCompleted++; continue; }
    // Transfers can give the same status label multiple IDs; keep one sample per issue.
    const visits = new Map<string, { state: IssueState; seconds: number }>();
    for (const { state, seconds } of totals) {
      const key = stageKey(state);
      visits.set(key, { state, seconds: (visits.get(key)?.seconds || 0) + seconds });
    }
    for (const { state, seconds } of visits.values()) stage(state).durations.push(seconds);
  }
  for (const issue of open) {
    const entered = Date.parse(issue.stageEnteredAt || "");
    const age = issue.stageEnteredAt === undefined ? issueStageTimes(issue, now)?.age ?? null :
      Number.isFinite(entered) && entered <= now ? (now - entered) / 1000 : null;
    if (age === null) unknownOpen++;
    stage(issue.state).items.push({ issue, seconds: age });
  }
  const order = ["triage", "backlog", "unstarted", "started"];
  return {
    unknownCompleted, unknownOpen,
    rows: [...stages.values()].sort((a, b) =>
      order.indexOf(a.state.type) - order.indexOf(b.state.type) || a.state.name.localeCompare(b.state.name),
    ).map(({ state, durations, items }) => ({
      state, ...durationStats(durations), current: items.length,
      oldest: items.sort((a, b) => (b.seconds ?? -1) - (a.seconds ?? -1)).slice(0, 5),
    })),
  };
}

export function firstReviewSeconds(pr: PullRequest) {
  if (!pr.readiness) return null;
  const created = Date.parse(pr.createdAt), merged = Date.parse(pr.mergedAt || "");
  const ready = Date.parse(reviewReadyAt(pr));
  if (!Number.isFinite(ready) || !Number.isFinite(created) || ready < created || ready > merged) return null;
  const first = Date.parse(pr.firstHumanReviewAt || "");
  return Number.isFinite(first) && first >= ready && first <= merged ? (first - ready) / 1000 : null;
}
export function approvalAge(pr: PullRequest, now = Date.now()) {
  const approvals = pr.reviews.nodes.filter((r) => r.state === "APPROVED")
    .map((r) => Date.parse(r.submittedAt || "")).filter(Number.isFinite);
  const since = approvals.length ? Math.max(...approvals) : NaN;
  return Number.isFinite(since) && since <= now ? (now - since) / 1000 : null;
}
export function mergeBlocker(pr: PullRequest) {
  if (pr.baseRefName !== pr.repository.defaultBranchRef?.name) return "Stacked";
  if (pr.mergeable === "CONFLICTING") return "Conflicts";
  const ci = ciState(pr);
  return ci === "failing" ? "CI failing" : ci === "running" ? "CI running" :
    ci === "none" ? "No CI status" : "Awaiting merge";
}
export function duration(seconds: number | null) {
  if (seconds === null) return "Unknown";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}
