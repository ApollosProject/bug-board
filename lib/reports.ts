import { cache } from "react";
import { unstable_cache } from "next/cache";
import { completed, contributors, openIssues, projects } from "./linear";
import { merged } from "./github";
import { readSnapshot } from "./cache";
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
export type Report = {
  completed: Issue[];
  prs: PullRequest[];
  projects: Project[];
  rows: TeamRow[];
  window: Window;
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
  const members = await contributors(scoring);
  return {
    completed: issues,
    prs,
    projects: projectList,
    rows: teamRows(issues, prs, projectList, members, window),
    window,
  };
}
export const report = cache(async (query: string): Promise<Report> => {
  const search = Object.fromEntries(new URLSearchParams(query));
  const window = timeWindow(search);
  if (window.preset_days === 30) {
    const snapshot = await readSnapshot<Report>("metrics", 180);
    if (snapshot) return snapshot;
    if (process.env.VERCEL_ENV)
      throw new Error(
        "Team metrics are refreshing. Configure Cron and Upstash Redis.",
      );
  }
  // Only integration data is cached. Session checks stay outside the data cache.
  return unstable_cache(
    async () =>
      computeReport(
        timeWindow(search, Math.floor(Date.now() / 60_000) * 60_000),
      ),
    ["report-v1", query],
    { revalidate: 60 },
  )();
});
export const reportFor = (window: Window) =>
  report(new URLSearchParams(window.query).toString());
export const openWork = cache(async () => {
  if (process.env.VERCEL_ENV) {
    const snapshot = await readSnapshot<Issue[]>("open-issues", 180);
    if (!snapshot)
      throw new Error(
        "Open work is refreshing. Configure Cron and Upstash Redis.",
      );
    return snapshot;
  }
  return unstable_cache(openIssues, ["open-work-v1"], { revalidate: 60 })();
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
export async function projectDashboard() {
  const snapshot = await readSnapshot<Project[]>("projects", 180);
  if (snapshot) return snapshot;
  if (process.env.VERCEL_ENV)
    throw new Error(
      "Projects are refreshing. Configure Cron and Upstash Redis.",
    );
  return projects();
}
