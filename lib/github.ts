import { cache } from "react";
import { config, people } from "./config";
import { readSnapshot, redis, writeSnapshot } from "./cache";
import { graphql, mapConcurrent, requestJson } from "./http";
import { DAY, date } from "./window";
import type { Connection, Delivery, PullRequest, Review, Window } from "./types";
export const repositories = [
  "apollosproject/apollos-platforms",
  "apollosproject/apollos-plugin",
  "apollosproject/apollos-cluster",
  "apollosproject/apollos-admin",
  "apollosproject/admin-transcriptions",
  "apollosproject/apollos-shovel",
  "apollosproject/apollos-embeds",
  "differential/crossroads-anywhere",
];
export const github = <T>(query: string, variables: object = {}) =>
  graphql<T>(
    "https://api.github.com/graphql",
    `Bearer ${process.env.GITHUB_TOKEN || ""}`,
    query,
    variables,
  );
export function githubHeaders(token = process.env.GITHUB_TOKEN) {
  return {
    Accept: "application/vnd.github+json",
    "User-Agent": "apollos-bug-board",
    "X-GitHub-Api-Version": "2022-11-28",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
}
export const githubRest = <T>(path: string, token = process.env.GITHUB_TOKEN) =>
  requestJson<T>(
    `https://api.github.com${path}`,
    { headers: githubHeaders(token) },
    "GitHub",
  );
class SearchLimit extends Error {}
const reviewFields =
  "nodes { author { login __typename } state submittedAt } pageInfo { hasNextPage endCursor }";
async function search(query: string, fields: string, size = 100) {
  if (!process.env.GITHUB_TOKEN) throw new Error("GitHub not configured");
  const result: PullRequest[] = [];
  let cursor: string | null = null;
  do {
    const data: { search: Connection<PullRequest> & { issueCount: number } } =
      await github<{
        search: Connection<PullRequest> & { issueCount: number };
      }>(
        `query Search($query: String!, $cursor: String) { search(type: ISSUE, query: $query, first: ${size}, after: $cursor) { issueCount pageInfo { hasNextPage endCursor } nodes { ... on PullRequest { ${fields} } } } }`,
        { query, cursor },
      );
    const page = data.search;
    if (page.issueCount > 1000)
      throw new SearchLimit("GitHub search exceeds 1,000 results");
    result.push(...page.nodes.filter(Boolean));
    if (!page.pageInfo.hasNextPage) break;
    if (!page.pageInfo.endCursor || cursor === page.pageInfo.endCursor)
      throw new Error("Incomplete GitHub pagination");
    cursor = page.pageInfo.endCursor;
  } while (cursor);
  return result;
}
async function completeReviews(pr: PullRequest, opinionated: boolean) {
  let page = pr.reviews;
  while (page.pageInfo?.hasNextPage) {
    const data = await github<{ node: { reviews: Connection<Review> } }>(
      `query Reviews($id: ID!, $cursor: String) { node(id: $id) { ... on PullRequest { reviews: ${opinionated ? "latestOpinionatedReviews" : "reviews"}(first: 100, after: $cursor${opinionated ? "" : ", states: [APPROVED]"}) { ${reviewFields} } } } }`,
      { id: pr.id, cursor: page.pageInfo.endCursor },
    );
    if (
      !data.node.reviews.pageInfo.endCursor ||
      data.node.reviews.pageInfo.endCursor === page.pageInfo.endCursor
    )
      throw new Error("Incomplete reviews");
    page = data.node.reviews;
    pr.reviews.nodes.push(...page.nodes);
  }
  pr.reviews.pageInfo = page.pageInfo;
}
// Keep nested search pages small; paginate approvals, but stop first-review reads at the first human.
const mergedFields = `id url createdAt mergedAt author { login }
 readiness: timelineItems(first: 1, itemTypes: [READY_FOR_REVIEW_EVENT, CONVERT_TO_DRAFT_EVENT]) {
   nodes { __typename ... on ReadyForReviewEvent { createdAt } ... on ConvertToDraftEvent { createdAt } }
 }
 reviews(first: 10, states: [APPROVED]) { ${reviewFields} }
 firstReviews: reviews(first: 5, states: [APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED]) { ${reviewFields} }
 commits(first: 1) { nodes { commit { author { user { login } } authors(first: 10) { nodes { user { login } } } } } }`;
export function reviewReadyAt(pr: PullRequest) {
  const event = pr.readiness?.nodes[0];
  return event?.__typename === "ReadyForReviewEvent" ? event.createdAt : pr.createdAt;
}
const automatedReviewer = (review: Review) =>
  review.author?.__typename === "Bot" || !!review.author?.login?.endsWith("[bot]") ||
  (config.automated_reviewers || []).includes(review.author?.login?.toLowerCase() || "");
export function humanReview(review: Review, pr: PullRequest) {
  return !!review.author?.login && !automatedReviewer(review) &&
    review.author.login.toLowerCase() !== pr.author?.login?.toLowerCase() &&
    ["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED"].includes(review.state);
}
async function completeFirstReview(pr: PullRequest) {
  if (pr.firstHumanReviewAt !== undefined) return;
  const authors = creditedAuthors(pr), ready = Date.parse(reviewReadyAt(pr));
  let page = pr.firstReviews;
  pr.firstHumanReviewAt = null;
  if (Object.values(people).some((p) => authors.includes(p.github_username.toLowerCase()))) {
    while (page) {
      const first = page.nodes.find((review) => humanReview(review, pr) &&
        Date.parse(review.submittedAt || "") >= ready);
      if (first) { pr.firstHumanReviewAt = first.submittedAt || null; break; }
      if (!page.pageInfo?.hasNextPage) break;
      const cursor: string | null = page.pageInfo.endCursor;
      if (!cursor) throw new Error("Incomplete first-review history");
      const data = await github<{ node: { firstReviews: Connection<Review> } }>(
        `query FirstReview($id: ID!, $cursor: String!) { node(id: $id) { ... on PullRequest {
          firstReviews: reviews(first: 100, after: $cursor, states: [APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED]) { ${reviewFields} }
        } } }`, { id: pr.id, cursor },
      );
      page = data.node.firstReviews;
      if (page.pageInfo.hasNextPage && (!page.pageInfo.endCursor || page.pageInfo.endCursor === cursor))
        throw new Error("Incomplete first-review history");
    }
  }
  delete pr.firstReviews;
}
export const mergedPRs = cache(async (after: string, before: string) => {
  if (!process.env.GITHUB_TOKEN) throw new Error("GitHub not configured");
  const snapshots: [string, PullRequest[]][] = [];
  async function range(start: number, end: number): Promise<PullRequest[]> {
    const historical = end < Date.parse(date(Date.now())),
      name = `merged-prs-v3:${config.github_orgs.join(",")}:${date(start)}:${date(end)}`;
    const snapshot = historical
      ? await readSnapshot<PullRequest[]>(name, 3600)
      : null;
    if (snapshot) return snapshot;
    let prs: PullRequest[];
    try {
      prs = await search(
        `${config.github_orgs.map((org) => `org:${org}`).join(" ")} is:pr is:merged merged:${date(start)}..${date(end)}`,
        mergedFields,
        25,
      );
    } catch (error) {
      if (!(error instanceof SearchLimit) || date(start) === date(end))
        throw error;
      const middle = Date.parse(date(start + (end - start) / 2));
      prs = [
        ...(await range(start, middle)),
        ...(await range(middle + DAY, end)),
      ];
    }
    if (historical && redis()) snapshots.push([name, prs]);
    return prs;
  }
  const start = Date.parse(date(Date.parse(after))),
    end = Date.parse(date(Date.parse(before) - 1));
  const weeks: number[] = [];
  for (let day = start; day <= end; day += 7 * DAY) weeks.push(day);
  const prs = (
    await mapConcurrent(weeks, 4, (day) => range(day, Math.min(day + 6 * DAY, end)))
  ).flat();
  await mapConcurrent(prs, 4, async (pr) => {
    await completeReviews(pr, false);
    await completeFirstReview(pr);
  });
  await Promise.all(snapshots.map(([name, data]) =>
    writeSnapshot(name, data, 3600).catch(() =>
      console.warn("Historical PR cache write unavailable"),
    ),
  ));
  return prs.filter(
    (pr) =>
      !!pr.mergedAt &&
      Date.parse(pr.mergedAt) >= Date.parse(after) &&
      Date.parse(pr.mergedAt) < Date.parse(before),
  );
});
export const merged = (w: Window) => mergedPRs(w.after, w.before);
type CheckSuites = { nodes: { commit: { checkSuites: {
  nodes: { createdAt: string; checkRuns: {
    nodes: { startedAt: string | null; completedAt: string | null; conclusion: string | null }[];
    pageInfo: { hasNextPage: boolean };
  } }[];
  pageInfo: { hasNextPage: boolean };
} } }[] };
export type DeliveryNode = {
  reviews: { nodes: Review[] };
  opened: CheckSuites;
  head: CheckSuites;
};
const runFields = "checkRuns(first: 50) { nodes { startedAt completedAt conclusion } pageInfo { hasNextPage } }";
// GitHub Actions' app ID: Codecov, Vercel, and other apps report on their own schedules.
const suiteFields = `checkSuites(first: 30, filterBy: { appId: 15368 }) { nodes { createdAt ${runFields} } pageInfo { hasNextPage } }`;
const deliveryFields = `reviews(first: 50) { nodes { author { login __typename } state submittedAt } }
 opened: commits(first: 1) { nodes { commit { ${suiteFields} } } }
 head: commits(last: 1) { nodes { commit { ${suiteFields} } } }`;
// One push creates its workflows within seconds; later suites (label/edit triggers) are separate runs.
function firstBurst(suites: CheckSuites) {
  const page = suites.nodes[0]?.commit.checkSuites;
  // Truncated check pages would understate waits, so they stay unknown.
  if (!page || page.pageInfo.hasNextPage || page.nodes.some((s) => s.checkRuns.pageInfo.hasNextPage)) return [];
  const ran = page.nodes.map((suite) => ({
    created: Date.parse(suite.createdAt),
    runs: suite.checkRuns.nodes.filter((r) => r.conclusion !== "SKIPPED")
      .map((r) => ({ started: Date.parse(r.startedAt || ""), completed: Date.parse(r.completedAt || "") }))
      .filter((r) => Number.isFinite(r.started) && Number.isFinite(r.completed)),
  })).filter((suite) => suite.runs.length && Number.isFinite(suite.created));
  const start = Math.min(...ran.map((suite) => suite.created));
  return ran.filter((suite) => suite.created - start <= 60_000);
}
export function deliveryFacts(pr: PullRequest, node: DeliveryNode): Delivery {
  const ready = Date.parse(reviewReadyAt(pr)), merged = Date.parse(pr.mergedAt || "");
  const at = (n: number) => (Number.isFinite(n) ? new Date(n).toISOString() : null);
  const opened = firstBurst(node.opened).flatMap((suite) => suite.runs);
  // A CI verdict or AI review already waiting at ready-for-review counts from ready.
  const automated = Math.min(
    opened.length ? Math.max(...opened.map((r) => r.completed)) : Infinity,
    ...node.reviews.nodes.filter(automatedReviewer).map((r) => Date.parse(r.submittedAt || "")).filter(Number.isFinite),
  );
  const decision = node.reviews.nodes.filter((r) => humanReview(r, pr) && ["APPROVED", "CHANGES_REQUESTED"].includes(r.state))
    .map((r) => Date.parse(r.submittedAt || "")).filter((n) => n >= ready && n <= merged);
  const suites = firstBurst(node.head), runs = suites.flatMap((suite) => suite.runs);
  return {
    automatedAt: automated <= merged ? at(Math.max(automated, ready)) : null,
    decisionAt: at(Math.min(...decision)),
    queue: runs.length ? Math.max(...suites.map((s) => Math.min(...s.runs.map((r) => r.started)) - s.created)) / 1000 : null,
    job: runs.length ? Math.max(...runs.map((r) => r.completed - r.started)) / 1000 : null,
    ci: runs.length ? (Math.max(...runs.map((r) => r.completed)) - Math.min(...suites.map((s) => s.created))) / 1000 : null,
  };
}
// Merged PRs rarely change, so each one is fetched once and reused across refreshes.
export async function completeDelivery(prs: PullRequest[]) {
  const name = "pr-delivery-v1", ttl = 30 * 86400;
  const saved = (await readSnapshot<Record<string, { mergedAt: string; delivery: Delivery }>>(name, ttl)) || {};
  const missing = prs.filter((pr) => {
    pr.delivery = saved[pr.id]?.delivery;
    return !pr.delivery;
  });
  // A cold fill spans a few refreshes: bursts trip GitHub's secondary limit and fail the PR search too.
  const batches: PullRequest[][] = [];
  for (let i = 0; i < Math.min(missing.length, 200); i += 10) batches.push(missing.slice(i, i + 10));
  await mapConcurrent(batches, 2, async (batch) => {
    try {
      const { nodes } = await github<{ nodes: (DeliveryNode | null)[] }>(
        `query Delivery($ids: [ID!]!) { nodes(ids: $ids) { ... on PullRequest { ${deliveryFields} } } }`,
        { ids: batch.map((pr) => pr.id) },
      );
      batch.forEach((pr, i) => {
        if (!nodes[i]) return;
        pr.delivery = deliveryFacts(pr, nodes[i]);
        saved[pr.id] = { mergedAt: pr.mergedAt || "", delivery: pr.delivery };
      });
    } catch {
      // Delivery timing is supplementary: leave it unknown rather than fail team metrics.
      console.warn("PR delivery timing unavailable");
    }
  });
  if (missing.length && redis()) {
    // shortcut: keeps 120 days of PRs to bound the record; longer custom windows refetch older PRs.
    for (const [id, value] of Object.entries(saved))
      if (!(Date.now() - Date.parse(value.mergedAt) < 120 * DAY)) delete saved[id];
    await writeSnapshot(name, saved, ttl).catch(() => console.warn("PR delivery cache write unavailable"));
  }
}
export function creditedAuthors(pr: PullRequest) {
  const author = pr.author?.login || "";
  const commit = pr.commits?.nodes[0]?.commit;
  if (
    author.toLowerCase() !== "cursor" ||
    commit?.author.user?.login?.toLowerCase() !== "cursoragent"
  )
    return author ? [author.toLowerCase()] : [];
  return [
    ...new Set(
      commit.authors.nodes
        .map((a) => a.user?.login?.toLowerCase())
        .filter((name): name is string => !!name && name !== "cursoragent"),
    ),
  ];
}
export function prCounts(prs: PullRequest[], username: string) {
  const login = username.toLowerCase();
  return {
    prs_merged: prs.filter((pr) => creditedAuthors(pr).includes(login)).length,
    prs_reviewed: prs.filter((pr) =>
      pr.reviews.nodes.some(
        (review) => review.state === "APPROVED" && review.author?.login?.toLowerCase() === login,
      ),
    ).length,
  };
}
const timelineFields =
  "nodes { __typename ... on ReadyForReviewEvent { createdAt } ... on ReviewRequestedEvent { createdAt requestedReviewer { ... on User { login } } } } pageInfo { hasPreviousPage startCursor }";
export const openPRs = cache(async (approved: boolean | "only") => {
  const fields = `id number title url createdAt updatedAt baseRefName headRefName additions deletions mergeable reviewDecision author { login }
 timelineCount: timelineItems(first: 1) { totalCount }
 repository { nameWithOwner defaultBranchRef { name } } statusCheckRollup { state }
 reviews: latestOpinionatedReviews(first: 100) { ${reviewFields} }
 reviewRequests(first: 100) { nodes { requestedReviewer { ... on User { login } } } pageInfo { hasNextPage } }`;
  const prs = (
    await mapConcurrent(repositories, 4, (repo) =>
      search(
        `repo:${repo} is:pr is:open draft:false${approved === "only" ? " review:approved" : approved ? "" : " -review:approved"}`,
        fields,
        // Three nested connections round up to two GitHub points at 50 PRs.
        49,
      ),
    )
  ).flat();
  // Approved aging needs current approvals, not review-request timelines.
  if (approved === "only") {
    await mapConcurrent(prs, 4, (pr) => completeReviews(pr, true));
    return prs;
  }
  type Timeline = NonNullable<PullRequest["timelineItems"]> & {
    pageInfo: { hasPreviousPage: boolean; startCursor: string };
  };
  type CachedTimeline = {
    updatedAt: string;
    totalCount: number;
    at: number;
    nodes: Timeline["nodes"];
  };
  const now = Date.now(),
    timelines = Object.fromEntries(
      Object.entries(
        (await readSnapshot<Record<string, CachedTimeline>>("review-timelines", 3600)) || {},
      ).filter(([, value]) => now - value.at < 3600000),
    );
  await mapConcurrent(prs, 8, async (pr) => {
    await completeReviews(pr, true);
    if (
      (
        pr.reviewRequests as typeof pr.reviewRequests & {
          pageInfo?: { hasNextPage: boolean };
        }
      ).pageInfo?.hasNextPage
    )
      throw new Error("Too many requested reviewers; queue incomplete");
    const totalCount = pr.timelineCount?.totalCount;
    if (!pr.updatedAt || typeof totalCount !== "number" || !Number.isSafeInteger(totalCount) || totalCount < 0)
      throw new Error("Missing GitHub timeline version");
    const cached = timelines[pr.id];
    if (cached?.updatedAt === pr.updatedAt && cached.totalCount === totalCount) {
      pr.timelineItems = { nodes: cached.nodes };
      return;
    }
    // Fetch timelines individually: batched queries can silently omit events.
    const nodes: Timeline["nodes"] = [];
    let before: string | null = null;
    while (true) {
      const data: { node: { timelineItems: Timeline } } = await github<{
        node: { timelineItems: Timeline };
      }>(
        `query Timeline($id: ID!, $before: String) { node(id: $id) { ... on PullRequest { timelineItems(last: 100, before: $before, itemTypes: [REVIEW_REQUESTED_EVENT, READY_FOR_REVIEW_EVENT]) { ${timelineFields} } } } }`,
        { id: pr.id, before },
      );
      const timeline: Timeline = data.node.timelineItems;
      nodes.unshift(...timeline.nodes);
      const page: Timeline["pageInfo"] = timeline.pageInfo;
      if (!page.hasPreviousPage) break;
      if (!page.startCursor || before === page.startCursor)
        throw new Error("Incomplete timeline");
      before = page.startCursor;
    }
    pr.timelineItems = { nodes };
    timelines[pr.id] = { updatedAt: pr.updatedAt, totalCount, at: now, nodes };
  });
  if (redis())
    await writeSnapshot("review-timelines", timelines, 3600).catch(() =>
      console.warn("Review timeline cache write unavailable"),
    );
  return prs;
});
export const stableTagPattern = /^v\d{4}\.\d{2}\.\d{2}\.\d{2}$/;
export async function stableTags(token = process.env.GITHUB_TOKEN) {
  const result: { name: string; commit: { sha: string } }[] = [];
  for (let page = 1; ; page++) {
    const tags = await githubRest<typeof result>(
      `/repos/ApollosProject/apollos-platforms/tags?per_page=100&page=${page}`,
      token,
    );
    result.push(...tags.filter((tag) => stableTagPattern.test(tag.name)));
    if (tags.length < 100) break;
  }
  return result.sort((a, b) => b.name.localeCompare(a.name));
}
