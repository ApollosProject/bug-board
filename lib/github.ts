import { cache } from "react";
import { config } from "./config";
import { graphql, mapConcurrent, requestJson } from "./http";
import { DAY, date } from "./window";
import type { Connection, PullRequest, Review, Window } from "./types";
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
  "nodes { author { login } state submittedAt } pageInfo { hasNextPage endCursor }";
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
}
const mergedFields = `id url mergedAt author { login } reviews(first: 100, states: [APPROVED]) { ${reviewFields} }
 commits(first: 1) { nodes { commit { author { user { login } } authors(first: 10) { nodes { user { login } } } } } }`;
export const mergedPRs = cache(async (after: string, before: string) => {
  async function range(start: number, end: number): Promise<PullRequest[]> {
    try {
      return await search(
        `${config.github_orgs.map((org) => `org:${org}`).join(" ")} is:pr is:merged merged:${date(start)}..${date(end)}`,
        mergedFields,
      );
    } catch (error) {
      if (!(error instanceof SearchLimit) || date(start) === date(end))
        throw error;
      const middle = Date.parse(date(start + (end - start) / 2));
      return [
        ...(await range(start, middle)),
        ...(await range(middle + DAY, end)),
      ];
    }
  }
  const prs = (
    await range(
      Date.parse(date(Date.parse(after))),
      Date.parse(date(Date.parse(before) - 1)),
    )
  ).filter(
    (pr) =>
      !!pr.mergedAt &&
      Date.parse(pr.mergedAt) >= Date.parse(after) &&
      Date.parse(pr.mergedAt) < Date.parse(before),
  );
  await mapConcurrent(prs, 4, (pr) => completeReviews(pr, false));
  return prs;
});
export const merged = (w: Window) => mergedPRs(w.after, w.before);
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
        (review) => review.author?.login?.toLowerCase() === login,
      ),
    ).length,
  };
}
const timelineFields =
  "nodes { __typename ... on ReadyForReviewEvent { createdAt } ... on ReviewRequestedEvent { createdAt requestedReviewer { ... on User { login } } } } pageInfo { hasPreviousPage startCursor }";
export const openPRs = cache(async (approved: boolean) => {
  const fields = `id number title url createdAt baseRefName headRefName additions deletions mergeable reviewDecision author { login }
 repository { nameWithOwner defaultBranchRef { name } } statusCheckRollup { state }
 reviews: latestOpinionatedReviews(first: 100) { ${reviewFields} }
 reviewRequests(first: 100) { nodes { requestedReviewer { ... on User { login } } } pageInfo { hasNextPage } }`;
  const prs = (
    await mapConcurrent(repositories, 4, (repo) =>
      search(
        `repo:${repo} is:pr is:open draft:false${approved ? "" : " -review:approved"}`,
        fields,
        50,
      ),
    )
  ).flat();
  // GitHub silently truncates timelines when more than ten are queried together.
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
    type Timeline = NonNullable<PullRequest["timelineItems"]> & {
      pageInfo: { hasPreviousPage: boolean; startCursor: string };
    };
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
  });
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
