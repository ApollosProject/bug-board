import { cache } from "react";
import { config } from "./config";
import { graphql } from "./http";
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
 assignee { name displayName } project { id name } state { name type }
 labels(first: 100) { nodes { name } } attachments(first: 100) { nodes { metadata } }`;
const openTypes = ["triage", "backlog", "unstarted", "started"];
export async function fetchIssues(
  filter: object,
  history = false,
): Promise<Issue[]> {
  const query = `query Issues($filter: IssueFilter, $cursor: String) { issues(first: 100, after: $cursor, filter: $filter) {
    nodes { ${fields} ${history ? "history(first: 100) { edges { node { toAssignee { displayName } updatedAt } } pageInfo { hasNextPage endCursor } }" : ""} }
    pageInfo { hasNextPage endCursor }
  } }`;
  const issues: Issue[] = [];
  let cursor: string | null = null;
  do {
    const {
      issues: page,
    }: {
      issues: Connection<
        Issue & {
          history?: Issue["history"] & {
            pageInfo: Connection<never>["pageInfo"];
          };
        }
      >;
    } = await linear<{
      issues: Connection<
        Issue & {
          history?: Issue["history"] & {
            pageInfo: Connection<never>["pageInfo"];
          };
        }
      >;
    }>(query, { filter, cursor });
    for (const issue of page.nodes) {
      let historyPage = issue.history?.pageInfo;
      while (historyPage?.hasNextPage) {
        const { issue: next } = await linear<{
          issue: { history: NonNullable<typeof issue.history> };
        }>(
          `query History($id: String!, $cursor: String) { issue(id: $id) { history(first: 100, after: $cursor) { edges { node { toAssignee { displayName } updatedAt } } pageInfo { hasNextPage endCursor } } } }`,
          { id: issue.id, cursor: historyPage.endCursor },
        );
        if (
          !next.history.pageInfo.endCursor ||
          next.history.pageInfo.endCursor === historyPage.endCursor
        )
          throw new Error("Incomplete Linear history");
        issue.history?.edges.push(...next.history.edges);
        historyPage = next.history.pageInfo;
      }
      // Only assignment events and fixing-PR metadata are used by reports.
      if (issue.history)
        issue.history.edges = issue.history.edges.filter(
          (edge) => edge.node.toAssignee,
        );
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
  return issues;
}
const team = { team: { key: { eq: config.linear_team_key } } };
export const openIssues = cache(() =>
  fetchIssues({ ...team, state: { type: { in: openTypes } } }),
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
