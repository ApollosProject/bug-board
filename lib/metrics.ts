import {
  config,
  engineers,
  people,
  personName,
  normalize,
  slugFor,
  priorityPoints,
} from "./config";
import { DAY, inWindow } from "./window";
import type { Issue, Project, PullRequest, Window } from "./types";
import { prCounts } from "./github";
export const metricLabels = {
  prs_merged: "PRs Merged",
  prs_reviewed: "PRs Approved",
  priority_bugs_fixed: "Priority Bugs Fixed",
  priority_bug_avg_time_to_fix: "Priority Bug Time to Fix",
  all_work_done: "All Work Done",
  avg_all_time_to_fix: "Time to Completion",
  lead_current_projects: "Current Projects",
  lead_completed_projects: "Completed Project Weeks",
  lead_incomplete_projects: "Incomplete Projects",
  lead_completed_projects_avg_early_late: "Avg Days Early/Late",
};
export type MetricKey = keyof typeof metricLabels;
export type Metrics = Record<MetricKey, number | null>;
const lowerBetter = new Set<MetricKey>([
  "priority_bug_avg_time_to_fix",
  "avg_all_time_to_fix",
  "lead_incomplete_projects",
  "lead_completed_projects_avg_early_late",
]);
export const teamColumns = {
  prs_merged: "PRs merged",
  prs_reviewed: "PRs approved",
  urgent_issues: "Urgent issues",
  high_issues: "High issues",
  medium_issues: "Medium issues",
  low_issues: "Low issues",
  project_lead_weeks: "Project lead weeks",
  project_contributor_weeks: "Project contributor weeks",
};
export type TeamKey = keyof typeof teamColumns;
export type TeamRow = Record<TeamKey, number> & {
  person: string;
  slug: string;
  score: number;
};
export const isBug = (issue: Issue) =>
  issue.labels.nodes.some((label) => label.name === "Bug");
export const isPriorityBug = (issue: Issue) =>
  isBug(issue) && issue.priority >= 1 && issue.priority <= 2;
// Person cards historically include unprioritized bugs; alerts and regressions do not.
export const isCardBug = (issue: Issue) =>
  isBug(issue) && issue.priority >= 0 && issue.priority <= 2;
export const isWork = (issue: Issue) =>
  issue.labels.nodes.some((label) =>
    ["Bug", "Feature Request", "Technical Change"].includes(label.name),
  );
export const isResolutionWork = (issue: Issue) =>
  issue.labels.nodes.some((label) =>
    ["Bug", "Feature Request"].includes(label.name),
  );
export const statusName = (p: Pick<Project, "status">) => p.status.name.trim().toLowerCase();
export const inactive = (p: Project) =>
  !!p.completedAt ||
  ["completed", "incomplete", "canceled", "cancelled", "released"].includes(
    statusName(p),
  );
export const done = (p: Pick<Project, "status" | "completedAt">) =>
  !["incomplete", "canceled", "cancelled"].includes(statusName(p)) &&
  (!!p.completedAt || ["completed", "released"].includes(statusName(p)));
export const blocked = (p: Project) =>
  (p.inverseRelations?.nodes || []).some(({ type, project, projectMilestone }) =>
    type === "dependency" &&
    (projectMilestone
      ? projectMilestone.status !== "done"
      : !(project && (project.completedAt || project.status.type === "completed" || done(project)))),
  );
export function plannedWeeks(p: Project) {
  const a = Date.parse(p.startDate || p.targetDate || ""),
    b = Date.parse(p.targetDate || p.startDate || "");
  return Number.isFinite(a) && Number.isFinite(b)
    ? Math.max(1, Math.floor((Math.abs(b - a) / DAY + 1) / 7 + 0.5))
    : 1;
}
export function variance(p: Project) {
  return p.targetDate && p.completedAt
    ? (Date.parse(p.completedAt.slice(0, 10)) -
        Date.parse(p.targetDate.slice(0, 10))) /
        DAY
    : null;
}
export function assignmentDays(issue: Issue) {
  if (!issue.completedAt || !issue.assignee?.displayName) return null;
  const completedAt = Date.parse(issue.completedAt);
  const assignments = (issue.history?.edges || [])
    .map((edge) => edge.node)
    .filter(
      (node) =>
        node.toAssignee?.displayName === issue.assignee?.displayName &&
        Date.parse(node.updatedAt) <= completedAt,
    )
    .map((node) => Date.parse(node.updatedAt));
  return assignments.length
    ? Math.max(0, Math.floor((completedAt - Math.max(...assignments)) / DAY))
    : null;
}
const average = (values: number[]) =>
  values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;
export function personMetrics(
  slug: string,
  issues: Issue[],
  prs: PullRequest[],
  projects: Project[],
  window: Window,
): Metrics {
  const person = people[slug];
  const items = issues.filter(
    (issue) =>
      slugFor(issue.assignee?.displayName, issue.assignee?.name) === slug,
  );
  const bugs = items.filter(isCardBug);
  const times = items
    .map(assignmentDays)
    .filter((n): n is number => n !== null);
  const bugTimes = bugs
    .map(assignmentDays)
    .filter((n): n is number => n !== null);
  const led = projects.filter(
    (project) =>
      normalize(project.lead?.displayName) ===
      normalize(person.linear_display_name || personName(slug)),
  );
  const completed = led.filter(
    (p) => done(p) && inWindow(p.completedAt, window),
  );
  const variances = completed
    .map(variance)
    .filter((n): n is number => n !== null && Number.isFinite(n));
  return {
    ...prCounts(prs, person.github_username),
    priority_bugs_fixed: bugs.length,
    priority_bug_avg_time_to_fix: bugTimes.length
      ? Math.trunc(average(bugTimes)!)
      : null,
    all_work_done: items.length,
    avg_all_time_to_fix: times.length ? Math.trunc(average(times)!) : null,
    lead_current_projects: led.filter((p) => !inactive(p)).length,
    lead_completed_projects: completed.reduce(
      (sum, p) => sum + plannedWeeks(p),
      0,
    ),
    lead_incomplete_projects: led.filter((p) => statusName(p) === "incomplete")
      .length,
    lead_completed_projects_avg_early_late: average(variances),
  };
}
export function baseline(values: number[]) {
  const sorted = values.slice().sort((a, b) => a - b),
    trim = Math.floor(values.length * 0.2);
  const trimmed =
    trim && sorted.length - 2 * trim >= 2 ? sorted.slice(trim, -trim) : sorted;
  const stats = (nums: number[]) => {
    const mean = average(nums) || 0;
    return {
      mean,
      stdev: Math.sqrt(
        nums.reduce((sum, n) => sum + (n - mean) ** 2, 0) / (nums.length || 1),
      ),
    };
  };
  return stats(trimmed).stdev ? stats(trimmed) : stats(sorted);
}
export function comparison(
  value: number | null,
  values: (number | null)[],
  lower = false,
) {
  const nums = values.filter(
    (n): n is number => n !== null && Number.isFinite(n),
  );
  if (value === null || nums.length < 2) return null;
  const { mean, stdev } = baseline(nums);
  if (!stdev) return null;
  const z = ((value - mean) / stdev) * (lower ? -1 : 1);
  return {
    z: Math.round(z * 100) / 100,
    label:
      Math.abs(z) < 0.05
        ? "0.0σ"
        : `${z > 0 ? "+" : "−"}${Math.abs(z).toFixed(1)}σ`,
    tone: z >= 1 ? "high" : z <= -1 ? "low" : null,
    eng_avg: Math.round(mean * 100) / 100,
    eng_stdev: Math.round(stdev * 100) / 100,
  };
}
export function comparisons(metrics: Metrics, cohort: Metrics[]) {
  return Object.fromEntries(
    (Object.keys(metricLabels) as MetricKey[]).map((key) => [
      key,
      comparison(
        metrics[key],
        cohort.map((m) => m[key]),
        lowerBetter.has(key),
      ),
    ]),
  ) as Record<MetricKey, ReturnType<typeof comparison>>;
}
export function metricDisplay(key: MetricKey, value: number | null) {
  if (value === null) return "n/a";
  if (key === "lead_completed_projects_avg_early_late")
    return value === 0
      ? "on time"
      : `${Math.abs(value).toFixed(1)}d ${value < 0 ? "early" : "late"}`;
  return `${value}${key === "priority_bug_avg_time_to_fix" || key === "avg_all_time_to_fix" ? "d" : ""}`;
}
export function projectScoringWeeks(p: Project, w: Window) {
  if (p.status.type !== "completed" || !p.lead?.displayName || !p.targetDate)
    return 0;
  const target = Date.parse(p.targetDate),
    end = Math.min(target + DAY, Date.parse(w.before));
  const start = Math.max(
    Math.min(Date.parse(p.startDate || p.targetDate), target),
    Date.parse(w.after),
  );
  if (end <= start) return 0;
  let count = 0;
  for (let b = Date.parse(w.before); b > Date.parse(w.after); b -= 7 * DAY)
    if (Math.min(end, b) > Math.max(start, b - 7 * DAY, Date.parse(w.after)))
      count++;
  return count;
}
export function teamRows(
  issues: Issue[],
  prs: PullRequest[],
  projects: Project[],
  members: Map<string, Set<string>>,
  w: Window,
): TeamRow[] {
  return Object.keys(people).map((slug) => {
    const items = issues.filter(
      (i) =>
        !i.project &&
        i.priority > 0 &&
        isWork(i) &&
        slugFor(i.assignee?.name, i.assignee?.displayName) === slug,
    );
    let lead = 0,
      contributor = 0;
    for (const p of projects) {
      const weeks = projectScoringWeeks(p, w);
      if (slugFor(p.lead?.displayName) === slug) lead += weeks;
      else if (
        [...(members.get(p.id) || [])].some((name) => slugFor(name) === slug)
      )
        contributor += weeks;
    }
    const counts = prCounts(prs, people[slug].github_username);
    return {
      slug,
      person: personName(slug),
      ...counts,
      urgent_issues: items.filter((i) => i.priority === 1).length,
      high_issues: items.filter((i) => i.priority === 2).length,
      medium_issues: items.filter((i) => i.priority === 3).length,
      low_issues: items.filter((i) => i.priority === 4 || i.priority === 5)
        .length,
      project_lead_weeks: lead,
      project_contributor_weeks: contributor,
      score:
        items.reduce((sum, i) => sum + (priorityPoints[i.priority] || 0), 0) +
        counts.prs_merged +
        counts.prs_reviewed +
        lead * 30 +
        contributor * 15,
    };
  });
}
export function supportSlugs(projects: Project[], now = Date.now()) {
  const day = new Date(now).toISOString().slice(0, 10);
  return engineers.filter(
    (slug) =>
      !projects.some(
        (p) =>
          !inactive(p) &&
          !!p.startDate &&
          p.startDate <= day &&
          [p.lead, ...p.members.nodes].some(
            (member) => slugFor(member?.displayName) === slug,
          ),
      ),
  );
}
export function slaText(issue: Issue, now = Date.now()) {
  const breach = Date.parse(issue.slaBreachesAt || "");
  if (!Number.isFinite(breach)) return null;
  const days = Math.floor(Math.abs(breach - now) / DAY);
  return breach >= now ? `${days}d` : `${days}d overdue`;
}
export function platformFor(issue: Issue) {
  return issue.labels.nodes.find(
    (label) => label.name.toLowerCase().replace(/ /g, "-") in config.platforms,
  )?.name;
}
export function resolution(issues: Issue[]) {
  return [1, 2, 3, 4, 5].flatMap((priority) => {
    const values = issues
      .filter(
        (i) =>
          !i.project &&
          i.priority === priority &&
          isResolutionWork(i) &&
          i.completedAt,
      )
      .map((i) =>
        Math.floor(
          (Date.parse(i.completedAt!) - Date.parse(i.createdAt)) / DAY,
        ),
      )
      .filter(Number.isFinite)
      .sort((a, b) => a - b);
    return values.length
      ? [
          {
            priority,
            count: values.length,
            avg_days: Math.trunc(average(values)!),
            p95_days:
              values[
                Math.min(Math.floor(values.length * 0.95), values.length - 1)
              ],
          },
        ]
      : [];
  });
}
export function linearListUrl(path: "issues" | "projects", ids: string[]) {
  if (path === "issues")
    return ids.length
      ? `https://linear.app/differential/issues/${[...new Set(ids)].map(encodeURIComponent).join(",")}`
      : `https://linear.app/differential/team/${config.linear_team_key}/all?layout=list`;
  const filter = Buffer.from(
    JSON.stringify({ and: [{ id: { in: [...new Set(ids)] } }] }),
  ).toString("base64url");
  return `https://linear.app/differential/projects/all?filter=${filter}&layout=list`;
}
export function csv(rows: TeamRow[]) {
  const keys = Object.keys(teamColumns) as TeamKey[];
  const columns = [
    "person",
    "slug",
    ...keys.flatMap((key) => [key, `${key}_z`]),
  ];
  const escape = (s: unknown) => `"${String(s ?? "").replace(/"/g, '""')}"`;
  return (
    [
      columns,
      ...rows.map((row) => [
        row.person,
        row.slug,
        ...keys.flatMap((key) => {
          const { mean, stdev } = baseline(rows.map((r) => r[key]));
          return [
            row[key],
            stdev ? ((row[key] - mean) / stdev).toFixed(1) : "",
          ];
        }),
      ]),
    ]
      .map((row) => row.map(escape).join(","))
      .join("\n") + "\n"
  );
}
