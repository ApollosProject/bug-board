import { cache } from "react";
import { unstable_cache } from "next/cache";
import { completed, contributors, openIssues, projects } from "./linear";
import { completeDelivery, merged, reviewDecisionSeconds } from "./github";
import { readSnapshot, redis, writeSnapshot } from "./cache";
import {
  comparisons,
  done,
  inactive,
  isCardBug,
  linearListUrl,
  metricDisplay,
  metricLabels,
  personMetrics,
  projectScoringWeeks,
  statusName,
  teamRows,
  variance,
  type MetricKey,
  type TeamKey,
  type TeamRow,
} from "./metrics";
import { engineers, people, personName, slugFor, normalize } from "./config";
import { DAY, inWindow, param, timeWindow } from "./window";
import type { Issue, Project, PullRequest, Search, Window } from "./types";
import { regressionDashboard } from "./regressions";
import { compactOpenIssues, issueStageTimes, teamPR } from "./pipeline";
export type Report = {
  completed: Issue[];
  prs: PullRequest[];
  projects: Project[];
  rows: TeamRow[];
  window: Window;
  members?: Record<string, string[]>;
};
export async function computeReport(window: Window): Promise<Report> {
  const [issues, prs, projectList] = await Promise.all([
    completed(window),
    merged(window),
    projects(),
  ]);
  const scoring = projectList
    .filter((p) => projectScoringWeeks(p, window))
    .map((p) => p.id);
  const [members] = await Promise.all([
    contributors(scoring),
    completeDelivery(prs.filter((pr) => teamPR(pr, true))),
  ]);
  return {
    completed: issues.map((issue) => {
      const totals = issueStageTimes(issue)?.totals;
      return {
        ...issue,
        stageDurations: totals ? [...totals.values()] : null,
        // Reports need status totals and assignment events, not a second full timeline copy.
        history: { edges: (issue.history?.edges || []).filter(({ node }) => node.toAssignee).map(({ node }) => ({
          node: { toAssignee: node.toAssignee, updatedAt: node.updatedAt },
        })) },
      };
    }),
    prs,
    projects: projectList,
    rows: teamRows(issues, prs, projectList, members, window),
    members: Object.fromEntries([...members].map(([id, names]) => [id, [...names]])),
    window,
  };
}
export const report = cache(async (query: string): Promise<Report> => {
  const search = Object.fromEntries(new URLSearchParams(query));
  const window = timeWindow(search);
  if (window.preset_days !== null && window.preset_days <= 30) {
    const snapshot = await readSnapshot<Report>("metrics", 180);
    if (snapshot && window.preset_days === 30) return snapshot;
    if (snapshot?.members) {
      const selected = timeWindow(search, Date.parse(snapshot.window.before));
      const issues = snapshot.completed.filter((issue) => inWindow(issue.completedAt, selected));
      const prs = snapshot.prs.filter((pr) => inWindow(pr.mergedAt, selected));
      const members = new Map(Object.entries(snapshot.members).map(([id, names]) => [id, new Set(names)]));
      return { ...snapshot, completed: issues, prs, window: selected,
        rows: teamRows(issues, prs, snapshot.projects, members, selected) };
    }
    if (process.env.VERCEL_ENV && process.env.VERCEL_ENV !== "preview")
      throw new Error(
        "Team metrics are refreshing. Configure Cron and Upstash Redis.",
      );
  }
  const compute = () => computeReport(timeWindow(search, Math.floor(Date.now() / 60_000) * 60_000));
  // Full reports exceed Next's 2 MB cache limit on real team windows.
  if (redis()) {
    const name = `report-v4:${query}`, saved = await readSnapshot<Report>(name, 60);
    if (saved) return saved;
    const data = await compute();
    await writeSnapshot(name, data, 60).catch(() => console.warn("Report cache write unavailable"));
    return data;
  }
  // Only integration data is cached. Session checks stay outside the data cache.
  return unstable_cache(
    compute,
    ["report-v4", query],
    { revalidate: 60 },
  )();
});
export const reportFor = (window: Window) =>
  report(new URLSearchParams(window.query).toString());
export const openWork = cache(async () => {
  if (process.env.VERCEL_ENV) {
    const snapshot = await readSnapshot<Issue[]>("open-issues", 180);
    if (snapshot) return snapshot;
    if (process.env.VERCEL_ENV !== "preview")
      throw new Error(
        "Open work is refreshing. Configure Cron and Upstash Redis.",
      );
  }
  return unstable_cache(async () => compactOpenIssues(await openIssues()),
    ["open-work-v4", process.env.VERCEL_URL || "local"], { revalidate: 60 })();
});
export function sortedTeam(rows: TeamRow[], search: Search) {
  const everyone = param(search, "everyone") === "1";
  const requested = param(search, "sort"),
    sort = ["person", ...Object.keys(rows[0] || {})].includes(
      requested.replace(/^-/, ""),
    )
      ? requested
      : "-prs_merged";
  const key = sort.replace(/^-/, "") as TeamKey | "person";
  return rows
    .filter((row) => everyone || engineers.includes(row.slug))
    .slice()
    .sort((a, b) => {
      const compared =
        key === "person" ? a.person.localeCompare(b.person) : a[key] - b[key];
      return (
        compared * (sort.startsWith("-") ? -1 : 1) ||
        a.person.localeCompare(b.person)
      );
    });
}
export async function personPayload(slug: string, window: Window) {
  const data = await reportFor(window);
  const metrics = personMetrics(
    slug,
    data.completed,
    data.prs,
    data.projects,
    data.window,
  );
  const cohort = engineers.includes(slug)
    ? engineers.map((key) =>
        personMetrics(
          key,
          data.completed,
          data.prs,
          data.projects,
          data.window,
        ),
      )
    : [];
  const compare = comparisons(metrics, cohort);
  const person = people[slug];
  const issues = data.completed.filter(
    (i) => slugFor(i.assignee?.name, i.assignee?.displayName) === slug,
  );
  const led = data.projects.filter(
    (p) =>
      normalize(p.lead?.displayName) ===
      normalize(person.linear_display_name || personName(slug)),
  );
  const finished = led.filter(
    (p) => done(p) && inWindow(p.completedAt, data.window),
  );
  const summary = await regressionDashboard();
  const authored = summary?.author_metrics.find((m) => m.slug === slug),
    approved = summary?.reviewer_metrics.find((m) => m.slug === slug);
  const links = {
    github_merged_prs: `https://github.com/pulls?${new URLSearchParams({ q: `is:pr is:merged author:${person.github_username} merged:${window.start}..${window.end}` })}`,
    priority_bugs_fixed: linearListUrl(
      "issues",
      issues.filter(isCardBug).map((i) => i.identifier),
    ),
    all_work_done: linearListUrl(
      "issues",
      issues.map((i) => i.identifier),
    ),
    lead_current_projects: linearListUrl(
      "projects",
      led.filter((p) => !inactive(p)).map((p) => p.id),
    ),
    lead_completed_projects: linearListUrl(
      "projects",
      finished.map((p) => p.id),
    ),
    lead_incomplete_projects: linearListUrl(
      "projects",
      led.filter((p) => statusName(p) === "incomplete").map((p) => p.id),
    ),
    lead_completed_projects_avg_early_late: linearListUrl(
      "projects",
      finished.filter((p) => variance(p) !== null).map((p) => p.id),
    ),
  };
  return {
    person: {
      slug,
      name: personName(slug),
      linear_username: person.linear_username,
      github_username: person.github_username,
    },
    window: {
      start: data.window.start,
      end: data.window.end,
      days: data.window.days,
      preset_days: data.window.preset_days,
      label: data.window.label,
    },
    review_decision_samples: reviewDecisionSeconds(data.prs, person.github_username).length,
    metrics: Object.fromEntries(
      (Object.keys(metricLabels) as MetricKey[]).map((key) => [
        key,
        {
          label: metricLabels[key],
          value: metrics[key],
          display: metricDisplay(key, metrics[key]),
          vs_team: compare[key],
        },
      ]),
    ),
    regressions: {
      status: !summary
        ? "refreshing"
        : summary.configured
          ? "ready"
          : "unconfigured",
      authored: authored?.regression_count ?? null,
      authored_rate: authored?.rate ?? null,
      approved: approved?.regression_count ?? null,
      approved_rate: approved?.rate ?? null,
    },
    links,
  };
}
export const daysSince = (value: string, now = Date.now()) =>
  Math.max(0, Math.floor((now - Date.parse(value)) / DAY));
export const projectDashboard = cache(async () => {
  const snapshot = await readSnapshot<Project[]>("projects", 180);
  if (snapshot) return snapshot;
  if (process.env.VERCEL_ENV === "preview")
    return unstable_cache(projects, ["preview-projects-v1", process.env.VERCEL_URL || "preview"], { revalidate: 60 })();
  if (process.env.VERCEL_ENV)
    throw new Error(
      "Projects are refreshing. Configure Cron and Upstash Redis.",
    );
  return projects();
});
