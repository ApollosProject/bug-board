import { cache } from "react";
import { config } from "./config";
import { graphql, mapConcurrent } from "./http";
import { readSnapshot, redis, writeSnapshot } from "./cache";
import type { Connection, Issue, Project, Window } from "./types";
export const linear = <T>(query: string, variables: object = {}) =>
  graphql<T>(
    "https://api.linear.app/graphql",
    process.env.LINEAR_API_KEY || "",
    query,
    variables,
  );
const fields = `id identifier number title url priority createdAt updatedAt completedAt startedAt dueDate
 slaType slaStartedAt slaMediumRiskAt slaHighRiskAt slaBreachesAt
 assignee { name displayName } project { id name } state { id name type }
 labels(first: 100) { nodes { name } } attachments(first: 100) { nodes { metadata } }`;
const openTypes = ["triage", "backlog", "unstarted", "started"];
const historyFields = `edges { node { toAssignee { displayName } updatedAt createdAt
 fromState { id name type } toState { id name type } } } pageInfo { hasNextPage endCursor }`;
type History = NonNullable<Issue["history"]> & { pageInfo: Connection<never>["pageInfo"] };
// Batch cold histories; unchanged issues reuse a complete, versioned history for an hour.
async function issueHistories(issues: Issue[], scope: string) {
  type Cached = { updatedAt: string; at: number; history: Issue["history"] };
  const name = `linear-histories:${scope}`, now = Date.now();
  const saved = Object.fromEntries(Object.entries(
    (await readSnapshot<Record<string, Cached>>(name, 3600)) || {},
  ).filter(([, value]) => now - value.at < 3600000));
  const missing = issues.filter((issue) => {
    if (!issue.id || !issue.updatedAt) throw new Error("Missing Linear history version");
    const cached = saved[issue.id];
    if (issue.updatedAt && cached?.updatedAt === issue.updatedAt) {
      issue.history = cached.history;
      return false;
    }
    return true;
  });
  const batches: Issue[][] = [];
  for (let i = 0; i < missing.length; i += 25) batches.push(missing.slice(i, i + 25));
  await mapConcurrent(batches, 4, async (batch) => {
    const { issues: page } = await linear<{ issues: Connection<Issue & { history: History }> }>(
      `query Histories($ids: [ID!]!) { issues(first: 25, filter: { id: { in: $ids } }) {
        nodes { id updatedAt history(first: 100) { ${historyFields} } } pageInfo { hasNextPage endCursor }
      } }`, { ids: batch.map((issue) => issue.id) },
    );
    const byId = new Map(page.nodes.map((issue) => [issue.id, issue]));
    if (page.pageInfo.hasNextPage || byId.size !== batch.length)
      throw new Error("Incomplete Linear histories");
    for (const issue of batch) {
      const source = byId.get(issue.id);
      if (!source?.history || source.updatedAt !== issue.updatedAt)
        throw new Error("Linear history changed during refresh");
      const edges = [...source.history.edges];
      let info = source.history.pageInfo;
      while (info.hasNextPage) {
        if (!info.endCursor) throw new Error("Incomplete Linear history");
        const { issue: next } = await linear<{ issue: { history: History } }>(
          `query History($id: String!, $cursor: String) { issue(id: $id) {
            history(first: 100, after: $cursor) { ${historyFields} }
          } }`, { id: issue.id, cursor: info.endCursor },
        );
        if (!next.history.pageInfo.endCursor || next.history.pageInfo.endCursor === info.endCursor)
          throw new Error("Incomplete Linear history");
        edges.push(...next.history.edges);
        info = next.history.pageInfo;
      }
      issue.history = { edges: edges.filter(({ node }) => node.toAssignee || node.toState || node.fromState) };
      saved[issue.id] = { updatedAt: issue.updatedAt, at: now, history: issue.history };
    }
  });
  if (missing.length && redis()) await writeSnapshot(name, saved, 3600).catch(() =>
    console.warn("Linear history cache write unavailable"),
  );
}
export async function fetchIssues(
  filter: object,
  history = false,
  historyScope = "completed",
): Promise<Issue[]> {
  const query = `query Issues($filter: IssueFilter, $cursor: String) { issues(first: 250, after: $cursor, filter: $filter) {
    nodes { ${fields} }
    pageInfo { hasNextPage endCursor }
  } }`;
  const issues: Issue[] = [];
  let cursor: string | null = null;
  do {
    const { issues: page }: { issues: Connection<Issue> } = await linear<{ issues: Connection<Issue> }>(query, { filter, cursor });
    for (const issue of page.nodes) {
      if (issue.attachments)
        issue.attachments.nodes = issue.attachments.nodes.map(({ metadata }) => ({
          metadata: {
            url: metadata?.url,
            status: metadata?.status,
            linkKind: metadata?.linkKind,
          },
        }));
      issues.push(issue);
    }
    if (!page.pageInfo.hasNextPage) break;
    if (!page.pageInfo.endCursor || cursor === page.pageInfo.endCursor)
      throw new Error("Incomplete Linear issue pagination");
    cursor = page.pageInfo.endCursor;
  } while (cursor);
  if (history) await issueHistories(issues, historyScope);
  return issues;
}
const team = { team: { key: { eq: config.linear_team_key } } };
export const openIssues = cache(() =>
  fetchIssues({ ...team, state: { type: { in: openTypes } } }, true, "open"),
);
export const completedIssues = cache((after: string, before: string) =>
  fetchIssues(
    {
      ...team,
      state: { type: { eq: "completed" } },
      completedAt: { gte: after, lt: before },
    },
    true,
  ),
);
export const completed = (w: Window) => completedIssues(w.after, w.before);
export async function issuesByNumber(numbers: number[]) {
  const issues: Issue[] = [];
  for (let i = 0; i < numbers.length; i += 250)
    issues.push(
      ...(await fetchIssues({
        ...team,
        number: { in: numbers.slice(i, i + 250) },
      })),
    );
  return new Map(issues.map((issue) => [issue.number, issue]));
}
// Linear attaches PRs linked by branch, title, or magic words like "closes" in the body.
export async function issuesLinkedTo(urls: string[]) {
  const issues: Issue[] = [];
  for (let i = 0; i < urls.length; i += 250)
    issues.push(
      ...(await fetchIssues({
        ...team,
        attachments: { some: { url: { in: urls.slice(i, i + 250) } } },
      })),
    );
  return issues;
}
export const projects = cache(async (): Promise<Project[]> => {
  type Relations = NonNullable<Project["inverseRelations"]> & {
    pageInfo: Connection<never>["pageInfo"];
  };
  type ProjectPage = Connection<Project & { inverseRelations?: Relations }>;
  const relationFields = "type project { status { name type } completedAt } projectMilestone { status }";
  const query = `query Projects($key: String!, $cursor: String) { teams(first: 1, filter: { key: { eq: $key } }) { nodes { projects(first: 50, after: $cursor) {
    nodes { id name url health priorityLabel status { name type } completedAt startDate targetDate lastUpdate { createdAt } lead { displayName } members(first: 50) { nodes { displayName } } initiatives(first: 50) { nodes { id name } }
      inverseRelations(first: 5) { nodes { ${relationFields} } pageInfo { hasNextPage endCursor } } }
    pageInfo { hasNextPage endCursor }
  } } } }`;
  const result: Project[] = [];
  let cursor: string | null = null;
  do {
    const data: { teams: { nodes: { projects: ProjectPage }[] } } =
      await linear<{ teams: { nodes: { projects: ProjectPage }[] } }>(
        query,
        { key: config.linear_team_key, cursor },
      );
    const page = data.teams.nodes[0]?.projects;
    if (!page) return result;
    for (const project of page.nodes) {
      let relations = project.inverseRelations;
      while (relations?.pageInfo.hasNextPage) {
        const after = relations.pageInfo.endCursor;
        if (!after) throw new Error("Incomplete Linear project dependency pagination");
        const { project: next } = await linear<{ project: { inverseRelations: Relations } }>(
          `query ProjectDependencies($id: String!, $after: String!) { project(id: $id) { inverseRelations(first: 50, after: $after) { nodes { ${relationFields} } pageInfo { hasNextPage endCursor } } } }`,
          { id: project.id, after },
        );
        relations = next.inverseRelations;
        if (relations.pageInfo.hasNextPage && (!relations.pageInfo.endCursor || relations.pageInfo.endCursor === after))
          throw new Error("Incomplete Linear project dependency pagination");
        project.inverseRelations?.nodes.push(...relations.nodes);
      }
      result.push(project);
    }
    if (!page.pageInfo.hasNextPage) break;
    if (!page.pageInfo.endCursor || cursor === page.pageInfo.endCursor)
      throw new Error("Incomplete Linear project pagination");
    cursor = page.pageInfo.endCursor;
  } while (cursor);
  return result.sort((a, b) => a.name.localeCompare(b.name));
});
export async function contributors(projectIds: string[]) {
  if (!projectIds.length) return new Map<string, Set<string>>();
  const issues = await fetchIssues({
    project: { id: { in: projectIds } },
    state: { type: { eq: "completed" } },
  });
  const result = new Map<string, Set<string>>();
  for (const issue of issues)
    if (issue.project && issue.assignee?.displayName) {
      const names = result.get(issue.project.id) || new Set<string>();
      names.add(issue.assignee.displayName);
      result.set(issue.project.id, names);
    }
  return result;
}
