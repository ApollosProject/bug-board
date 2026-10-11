import { slugFor, personName } from "./config";
import { DAY } from "./window";
import type { Issue, Project } from "./types";
// Linear upkeep lists, computed from the open-work and project snapshots so the
// page adds no Linear requests.
export function linearHygiene(
  issues: Issue[],
  projects: Project[],
  now = Date.now(),
) {
  // Whole days for display; thresholds compare raw elapsed time.
  const age = (value: string) => Math.floor((now - Date.parse(value)) / DAY);
  const olderThan = (value: string, days: number) =>
    now - Date.parse(value) > days * DAY;
  const owner = (issue: Issue) => {
    const slug = slugFor(issue.assignee?.name, issue.assignee?.displayName);
    return slug
      ? personName(slug)
      : issue.assignee?.displayName || issue.assignee?.name || null;
  };
  const byLabel = (list: Issue[]) => {
    const groups = new Map<string, Issue[]>();
    for (const issue of list)
      for (const label of issue.labels.nodes.length
        ? issue.labels.nodes.map((l) => l.name)
        : ["No label"])
        groups.set(label, [...(groups.get(label) || []), issue]);
    return [...groups].sort((a, b) => b[1].length - a[1].length);
  };
  const triage = issues
    .filter((i) => i.state.type === "triage")
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  const started = issues.filter((i) => i.state.type === "started");
  const people = new Map<string, Issue[]>();
  for (const issue of started) {
    const who = owner(issue) || "Unassigned";
    people.set(who, [...(people.get(who) || []), issue]);
  }
  const open = new Map<string, number>();
  for (const issue of issues)
    if (issue.project)
      open.set(issue.project.id, (open.get(issue.project.id) || 0) + 1);
  return {
    triage,
    triageOver: (days: number) =>
      triage.filter((i) => olderThan(i.createdAt, days)),
    started: [...people]
      .map(([who, list]) => ({
        who,
        issues: list,
        stale: list.filter((i) => olderThan(i.updatedAt, 14)),
      }))
      .sort((a, b) => b.issues.length - a.issues.length),
    merged: started
      .filter((i) => i.state.name.trim().toLowerCase() === "merged")
      .sort((a, b) => a.updatedAt.localeCompare(b.updatedAt)),
    unassigned: byLabel(issues.filter((i) => !i.assignee)),
    noProject: byLabel(issues.filter((i) => !i.project)),
    // "Not Now" and paused projects that hold almost nothing. Open work covers
    // only our team, so shared (or unknown-team) projects are left out.
    emptyProjects: projects
      .filter(
        (p) =>
          p.teams?.nodes.length === 1 &&
          ["backlog", "paused"].includes(p.status.type) &&
          !p.completedAt &&
          (open.get(p.id) || 0) <= 1,
      )
      .map((p) => ({ project: p, open: open.get(p.id) || 0 }))
      .sort((a, b) => a.project.name.localeCompare(b.project.name)),
    age,
    owner,
  };
}
