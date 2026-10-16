import { computeApps } from "./apps";
import { churchDirectory } from "./churches";
import { writeSnapshot } from "./cache";
import {
  evaluateFleet,
  fleetDeadline,
  fleetInventory,
  fleetHeartbeat,
} from "./fleet";
import { openIssues } from "./linear";
import { ptoCalendar } from "./pto";
import { computeReport } from "./reports";
import { compactOpenIssues } from "./pipeline";
import { reviewQueue } from "./reviews";
import type { Job } from "./types";
import { timeWindow } from "./window";

export async function refreshSnapshot(
  job: Exclude<Job, "regressions" | "notifications">,
) {
  if (job === "reviews") {
    const rows = await reviewQueue(true);
    await writeSnapshot("reviews", rows, 900);
    return { prs: rows.length };
  }
  if (job === "metrics") {
    const [report, open, pto] = await Promise.all([
      computeReport(timeWindow({ days: "30" })),
      openIssues(),
      ptoCalendar(),
    ]);
    await Promise.all([
      writeSnapshot("metrics", report, 900),
      writeSnapshot("projects", report.projects, 900),
      writeSnapshot("open-issues", compactOpenIssues(open), 900),
      writeSnapshot("pto", pto, 900),
    ]);
    return { people: report.rows.length, completed: report.completed.length };
  }
  if (job === "fleet") {
    try {
      // Keep the previous four-batch limit of 120 simultaneous DAG lookups: it
      // bounds a full Airflow stall to about 150 seconds inside the Cron limit.
      const deadline = fleetDeadline();
      const fleet = await evaluateFleet(
        await fleetInventory(deadline),
        120,
        deadline,
      );
      await writeSnapshot("fleet", fleet, 900);
      await fleetHeartbeat(fleet.status);
      return { status: fleet.status, dags: fleet.active_dags_total };
    } catch (error) {
      await fleetHeartbeat("unknown");
      throw error;
    }
  }
  // The church directory rides along but settles on its own: either failure
  // keeps that snapshot's previous value without blocking the other.
  const [apps, directory] = await Promise.allSettled([
    computeApps(),
    churchDirectory(),
  ]);
  const listed = directory.status === "fulfilled" ? directory.value : [];
  if (listed.length)
    await writeSnapshot("churches", listed, 3600).catch(() =>
      console.warn("Church directory snapshot write unavailable"),
    );
  if (apps.status === "rejected") throw apps.reason;
  await writeSnapshot("apps", apps.value, 300);
  return { apps: apps.value.rows.length, churches: listed.length };
}
