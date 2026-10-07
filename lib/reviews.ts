import { config, people } from "./config";
import { openPRs } from "./github";
import { issuesByNumber } from "./linear";
import type { Issue, PullRequest } from "./types";
export function ticketNumber(pr: Pick<PullRequest, "headRefName" | "title">) {
  const pattern = new RegExp(
    `(?<![A-Za-z0-9])${config.linear_team_key}-(\\d+)(?![A-Za-z0-9])`,
    "i",
  );
  const number =
    pattern.exec(pr.headRefName || "")?.[1] ||
    pattern.exec(pr.title || "")?.[1];
  return number ? Number(number) : null;
}
export function ciState(pr: PullRequest) {
  return !pr.statusCheckRollup
    ? "none"
    : pr.statusCheckRollup.state === "SUCCESS"
      ? "passing"
      : ["PENDING", "EXPECTED"].includes(pr.statusCheckRollup.state)
        ? "running"
        : "failing";
}
export function activeChangeRequests(pr: PullRequest) {
  if (pr.reviewDecision && pr.reviewDecision !== "CHANGES_REQUESTED") return [];
  const latest = new Map<string, { state: string; at: number }>();
  for (const review of pr.reviews.nodes) {
    const login = review.author?.login?.toLowerCase(),
      at = Date.parse(review.submittedAt || "");
    if (
      !login ||
      !["APPROVED", "CHANGES_REQUESTED", "DISMISSED"].includes(review.state)
    )
      continue;
    const current = latest.get(login);
    if (!current || at >= current.at)
      latest.set(login, { state: review.state, at });
  }
  return [...latest]
    .filter(([login, review]) => {
      if (review.state !== "CHANGES_REQUESTED") return false;
      const requests =
        pr.timelineItems?.nodes
          .filter((n) => n.requestedReviewer?.login?.toLowerCase() === login)
          .map((n) => Date.parse(n.createdAt))
          .filter(Number.isFinite) || [];
      return (
        pr.reviewDecision === "CHANGES_REQUESTED" ||
        !requests.length ||
        !Number.isFinite(review.at) ||
        review.at >= Math.max(...requests)
      );
    })
    .map(([login]) => login);
}
export function classify(pr: PullRequest) {
  const approved =
    pr.reviewDecision === "APPROVED" ||
    (pr.reviewDecision === null &&
      pr.reviews.nodes.some((r) => r.state === "APPROVED") &&
      !pr.reviews.nodes.some((r) => r.state === "CHANGES_REQUESTED"));
  if (approved) return { section: "approved", reason: null };
  if (pr.baseRefName !== pr.repository.defaultBranchRef?.name)
    return { section: "not_ready", reason: "Stacked" };
  if (pr.mergeable === "CONFLICTING")
    return { section: "not_ready", reason: "Conflicts" };
  if (ciState(pr) === "failing")
    return { section: "not_ready", reason: "CI failing" };
  const requested = pr.reviewRequests.nodes.map((r) =>
    r.requestedReviewer?.login?.toLowerCase(),
  );
  const active = activeChangeRequests(pr);
  if (active.some((login) => !requested.includes(login)))
    return { section: "not_ready", reason: "Changes requested" };
  return {
    section: ciState(pr) === "running" ? "running" : "ready",
    reason: null,
  };
}
export type ReviewRow = {
  pr: PullRequest;
  issue: Issue | undefined;
  section: string;
  reason: string | null;
  priority: number;
  size: string;
  sizeRank: number;
  waiting: string;
  seconds: number;
  reviewers: string[];
  ci: string;
};
export function reviewRows(
  prs: PullRequest[],
  issues: Map<number, Issue>,
  now = Date.now(),
): ReviewRow[] {
  const team = new Set(
    Object.values(people).map((p) => p.github_username.toLowerCase()),
  );
  return prs
    .map((pr) => {
      const number = ticketNumber(pr),
        issue = number ? issues.get(number) : undefined;
      const events =
        pr.timelineItems?.nodes
          .filter(
            (n) =>
              n.__typename === "ReadyForReviewEvent" ||
              team.has(n.requestedReviewer?.login?.toLowerCase() || ""),
          )
          .map((n) => Date.parse(n.createdAt))
          .filter(Number.isFinite) || [];
      const since = events.length
        ? Math.max(...events)
        : Date.parse(pr.createdAt);
      const seconds = Math.max(
          (now - (Number.isFinite(since) ? since : now)) / 1000,
          0,
        ),
        hours = Math.floor(seconds / 3600);
      const sizeRank = [50, 200, 500, Infinity].findIndex(
        (limit) => pr.additions + pr.deletions <= limit,
      );
      return {
        pr,
        issue,
        ...classify(pr),
        priority: issue?.priority || 0,
        size: ["XS", "S", "M", "L"][sizeRank],
        sizeRank,
        waiting: hours < 24 ? `${hours}h` : `${Math.floor(hours / 24)}d`,
        seconds,
        reviewers: pr.reviewRequests.nodes
          .map((r) => r.requestedReviewer?.login || "")
          .filter((login) => team.has(login.toLowerCase())),
        ci: ciState(pr),
      };
    })
    .sort(
      (a, b) =>
        a.section.localeCompare(b.section) ||
        (a.section === "not_ready" || a.section === "approved"
          ? b.seconds - a.seconds
          : (a.priority || 99) - (b.priority || 99) ||
            a.sizeRank - b.sizeRank ||
            b.seconds - a.seconds),
    );
}
export async function reviewQueue(approved: boolean) {
  const prs = await openPRs(approved);
  return reviewRows(
    prs,
    await issuesByNumber([
      ...new Set(prs.map(ticketNumber).filter((n): n is number => n !== null)),
    ]),
  );
}
