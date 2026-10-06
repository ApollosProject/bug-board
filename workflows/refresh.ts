import { appObservations, appSource, buildApps, type Source } from "@/lib/apps";
import { planStores, plannedRelease, type StoreRelease } from "@/lib/stores";
import { writeSnapshot, release, unknownCount } from "@/lib/cache";
import { fleetInventory, evaluateFleet } from "@/lib/fleet";
import { completed, contributors, openIssues, projects } from "@/lib/linear";
import { merged } from "@/lib/github";
import { projectScoringWeeks, teamRows } from "@/lib/metrics";
import { ptoCalendar } from "@/lib/pto";
import {
  blameFile,
  fixingContext,
  mergeAttribution,
  regressionIssues,
  summarizeRegressions,
  type Attribution,
  type Candidate,
  type FixContext,
  type FixFile,
} from "@/lib/regressions";
import { notify, scheduledNotifications } from "@/lib/notifications";
import { timeWindow, date } from "@/lib/window";
import type {
  AppRow,
  Fleet,
  Issue,
  Job,
  Project,
  PullRequest,
  Window,
} from "@/lib/types";
async function windowStep(days = 30) {
  "use step";
  return timeWindow({ days: String(days) });
}
async function unlockStep(job: Job, token: string) {
  "use step";
  await release(`refresh:${job}`, token);
}
async function issuesStep(window: Window) {
  "use step";
  return completed(window);
}
async function prsStep(window: Window) {
  "use step";
  return merged(window);
}
async function projectsStep() {
  "use step";
  return projects();
}
async function openStep() {
  "use step";
  return openIssues();
}
async function metricsStep(
  window: Window,
  issues: Issue[],
  prs: PullRequest[],
  projectList: Project[],
  open: Issue[],
) {
  "use step";
  const members = await contributors(
    projectList.filter((p) => projectScoringWeeks(p, window)).map((p) => p.id),
  );
  const rows = teamRows(issues, prs, projectList, members, window);
  await Promise.all([
    writeSnapshot(
      "metrics",
      { completed: issues, prs, projects: projectList, rows, window },
      900,
    ),
    writeSnapshot("projects", projectList, 900),
    writeSnapshot("open-issues", open, 900),
    writeSnapshot("pto", await ptoCalendar(), 900),
  ]);
  return { people: rows.length, completed: issues.length };
}
async function fleetIDsStep() {
  "use step";
  return fleetInventory();
}
async function fleetRunsStep(ids: string[]) {
  "use step";
  return evaluateFleet(ids);
}
async function saveFleetStep(parts: Fleet[]) {
  "use step";
  const total = parts.reduce(
    (sum, part) => ({
      active: sum.active + part.active_dags_total,
      evaluated: sum.evaluated + part.evaluated_dags,
      fetches: sum.fetches + part.failed_fetches,
      without: sum.without + part.dags_without_runs,
      nonterminal: sum.nonterminal + part.non_terminal_dags,
    }),
    { active: 0, evaluated: 0, fetches: 0, without: 0, nonterminal: 0 },
  );
  const failed = parts.flatMap((p) => p.failed_dags),
    ratio = total.evaluated ? failed.length / total.evaluated : 0;
  const fleet: Fleet = {
    status:
      total.active && total.fetches === total.active
        ? "unknown"
        : total.evaluated >= 20 && ratio >= 0.1
          ? "degraded"
          : "healthy",
    checked_at: new Date().toISOString(),
    active_dags_total: total.active,
    evaluated_dags: total.evaluated,
    failed_fetches: total.fetches,
    dags_without_runs: total.without,
    non_terminal_dags: total.nonterminal,
    failed_runs: failed.length,
    failure_ratio: ratio,
    threshold_ratio: 0.1,
    dags: parts.flatMap((p) => p.dags),
    failed_dags: failed,
    top_failed_dags: failed.slice(0, 10),
  };
  await writeSnapshot("fleet", fleet, 900);
  return { status: fleet.status, dags: fleet.active_dags_total };
}
async function heartbeatStep(status: Fleet["status"]) {
  "use step";
  const count = await unknownCount(status !== "unknown");
  if (
    (status === "unknown" && count < 3) ||
    !process.env.AIRFLOW_FLEET_HEARTBEAT_URL ||
    process.env.VERCEL_ENV !== "production"
  )
    return;
  const url = new URL(process.env.AIRFLOW_FLEET_HEARTBEAT_URL);
  if (status !== "healthy")
    url.pathname = `${url.pathname.replace(/\/$/, "")}/fail`;
  const response = await fetch(url, {
    redirect: "error",
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error("Fleet heartbeat unavailable");
}
async function observationsStep() {
  "use step";
  return appObservations();
}
async function sourceStep() {
  "use step";
  return appSource();
}
async function storesPlanStep(rows: AppRow[]) {
  "use step";
  return planStores(rows);
}
async function releaseStep(
  plan: Awaited<ReturnType<typeof planStores>>[number],
) {
  "use step";
  return plannedRelease(plan);
}
async function appsStep(
  rows: AppRow[],
  source: Source,
  releases: [string, StoreRelease][],
) {
  "use step";
  const apps = await buildApps(rows, source, new Map(releases));
  await writeSnapshot("apps", apps, 300);
  return { apps: apps.rows.length };
}
async function regressionIssuesStep(window: Window) {
  "use step";
  return regressionIssues(window);
}
async function contextStep(url: string) {
  "use step";
  return fixingContext(url);
}
async function blameStep(context: FixContext, file: FixFile) {
  "use step";
  return blameFile(context, file);
}
async function mergeStep(
  issue: { identifier: string; url: string },
  urls: string[],
  results: { candidates: Candidate[]; complete: boolean }[],
) {
  "use step";
  return mergeAttribution(issue, urls, results);
}
async function regressionSummaryStep(records: Attribution[], window: Window) {
  "use step";
  const summary = await summarizeRegressions(records, window);
  await writeSnapshot("regressions", summary, 86400);
  return { regressions: summary.regression_count, complete: summary.complete };
}
async function scheduleStep() {
  "use step";
  return {
    names:
      process.env.VERCEL_ENV === "production" ? scheduledNotifications() : [],
    day: date(Date.now()),
  };
}
async function notifyStep(name: string, day: string) {
  "use step";
  return notify(name, day);
}
notifyStep.maxRetries = 0;
export async function refresh(job: Job, token: string) {
  "use workflow";
  try {
    if (job === "metrics") {
      const window = await windowStep();
      const [issues, prs, projectList, open] = await Promise.all([
        issuesStep(window),
        prsStep(window),
        projectsStep(),
        openStep(),
      ]);
      return await metricsStep(window, issues, prs, projectList, open);
    }
    if (job === "fleet") {
      try {
        const ids = await fleetIDsStep();
        const parts: Fleet[] = [];
        // Each durable step scans at most 30 DAGs, rather than one fleet-sized function.
        for (let i = 0; i < ids.length; i += 120)
          parts.push(
            ...(await Promise.all(
              Array.from(
                { length: Math.min(4, Math.ceil((ids.length - i) / 30)) },
                (_, n) =>
                  fleetRunsStep(ids.slice(i + n * 30, i + (n + 1) * 30)),
              ),
            )),
          );
        const result = await saveFleetStep(parts);
        await heartbeatStep(result.status);
        return result;
      } catch (error) {
        await heartbeatStep("unknown");
        throw error;
      }
    }
    if (job === "apps") {
      const [rows, source] = await Promise.all([
        observationsStep(),
        sourceStep(),
      ]);
      const plans = await storesPlanStep(rows),
        releases: [string, StoreRelease][] = [];
      for (let i = 0; i < plans.length; i += 8)
        releases.push(
          ...(await Promise.all(
            plans
              .slice(i, i + 8)
              .map(
                async (plan) =>
                  [plan.key, await releaseStep(plan)] as [string, StoreRelease],
              ),
          )),
        );
      return await appsStep(rows, source, releases);
    }
    if (job === "regressions") {
      const window = await windowStep(),
        issues = await regressionIssuesStep(window),
        results = new Map<
          string,
          { candidates: Candidate[]; complete: boolean }
        >();
      const urls = [...new Set(issues.flatMap((issue) => issue.fixing_urls))];
      for (const url of urls) {
        const context = await contextStep(url);
        if (!context) {
          results.set(url, { candidates: [], complete: false });
          continue;
        }
        const files: { candidates: Candidate[]; complete: boolean }[] = [];
        for (let i = 0; i < context.files.length; i += 4)
          files.push(
            ...(await Promise.all(
              context.files
                .slice(i, i + 4)
                .map((file) => blameStep(context, file)),
            )),
          );
        results.set(url, {
          candidates: files.flatMap((f) => f.candidates),
          complete: context.complete && files.every((f) => f.complete),
        });
      }
      const records: Attribution[] = [];
      for (const issue of issues)
        records.push(
          await mergeStep(
            issue,
            issue.fixing_urls,
            issue.fixing_urls.map((url) => results.get(url)!),
          ),
        );
      return await regressionSummaryStep(records, window);
    }
    const schedule = await scheduleStep();
    for (const name of schedule.names) await notifyStep(name, schedule.day);
    return { notifications: schedule.names.length };
  } finally {
    await unlockStep(job, token);
  }
}
