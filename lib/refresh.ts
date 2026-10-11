import { computeApps } from "./apps";
import { writeSnapshot } from "./cache";
import { evaluateFleet, fleetInventory, fleetHeartbeat } from "./fleet";
import { openIssues } from "./linear";
import { ptoCalendar } from "./pto";
import { computeReport } from "./reports";
import type { Job } from "./types";
import { timeWindow } from "./window";

export async function refreshSnapshot(
  job: Exclude<Job, "regressions" | "notifications">,
) {
  if (job === "metrics") {
    const [report, open, pto] = await Promise.all([
      computeReport(timeWindow({ days: "30" })),
      openIssues(),
      ptoCalendar(),
    ]);
    await Promise.all([
      writeSnapshot("metrics", report, 900),
      writeSnapshot("projects", report.projects, 900),
      writeSnapshot("open-issues", open, 900),
      writeSnapshot("pto", pto, 900),
    ]);
    return { people: report.rows.length, completed: report.completed.length };
  }
  if (job === "fleet") {
    try {
      // Keep the previous four-batch limit of 120 simultaneous DAG lookups.
      const fleet = await evaluateFleet(await fleetInventory(), 120);
      await writeSnapshot("fleet", fleet, 900);
      await fleetHeartbeat(fleet.status);
      return { status: fleet.status, dags: fleet.active_dags_total };
    } catch (error) {
      await fleetHeartbeat("unknown");
      throw error;
    }
  }
  const apps = await computeApps();
  await writeSnapshot("apps", apps, 300);
  return { apps: apps.rows.length };
}
