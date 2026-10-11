import { mapConcurrent, requestJson, UpstreamError } from "./http";
import type { Dag, Fleet } from "./types";
import { readSnapshot, unknownCount } from "./cache";
type Run = { state?: string; dag_run_id?: string };
type Evaluation = {
  latest: string;
  terminal: string;
  id: string;
  hasRuns: boolean;
};
const terminal = (state = "") =>
  ["success", "failed"].includes(state.toLowerCase());
async function airflow<T>(path: string) {
  const base = process.env.AIRFLOW_API_BASE_URL,
    token = process.env.AIRFLOW_API_TOKEN;
  if (!base || !token) throw new Error("Airflow not configured");
  return requestJson<T>(
    `${base.replace(/\/$/, "")}${path}`,
    {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    },
    "Airflow",
  );
}
// Each Airflow request may wait for its full 30-second timeout.
const REQUEST_TIMEOUT = 30_000;
async function lastRun(id: string, deadline: number): Promise<Evaluation> {
  let latest = "",
    hasRuns = false;
  for (let offset = 0; ; offset += 10) {
    if (Date.now() + REQUEST_TIMEOUT > deadline)
      throw Object.assign(new Error("Fleet deadline"), { name: "Deadline" });
    const payload = await airflow<{
      dag_runs?: Run[];
      dagRuns?: Run[];
      total_entries?: number;
    }>(
      `/dags/${encodeURIComponent(id)}/dagRuns?limit=10&offset=${offset}&order_by=-logical_date`,
    );
    const runs = payload.dag_runs || payload.dagRuns || [];
    if (!hasRuns && runs.length) {
      latest = (runs[0].state || "").toLowerCase();
      hasRuns = true;
    }
    const last = runs.find((run) => terminal(run.state));
    if (last)
      return {
        latest,
        hasRuns,
        terminal: last.state!.toLowerCase(),
        id: last.dag_run_id || "",
      };
    if (
      !runs.length ||
      (payload.total_entries !== undefined
        ? offset + runs.length >= payload.total_entries
        : runs.length < 10)
    )
      return { latest, hasRuns, terminal: "", id: "" };
  }
}
export function fleetStats(
  ids: string[],
  runs: (Evaluation | null)[],
  now = Date.now(),
): Fleet {
  const evaluated = runs.filter((run) => run?.terminal).length;
  const failed: Dag[] = ids.flatMap((id, i) =>
    runs[i]?.terminal === "failed"
      ? [{ dag_id: id, state: "failed", dag_run_id: runs[i]!.id }]
      : [],
  );
  const ratio = evaluated ? failed.length / evaluated : 0,
    unchecked = runs.filter((run) => !run).length;
  return {
    // A known outage wins. Otherwise, health is only claimed when at least 90% of
    // active DAGs could be checked; missing data is unknown, not healthy.
    status:
      evaluated >= 20 && ratio >= 0.1
        ? "degraded"
        : ids.length && unchecked > ids.length * 0.1
          ? "unknown"
          : "healthy",
    checked_at: new Date(now).toISOString(),
    active_dags_total: ids.length,
    evaluated_dags: evaluated,
    failed_fetches: unchecked,
    dags_without_runs: runs.filter((run) => run && !run.hasRuns).length,
    non_terminal_dags: runs.filter(
      (run) => run?.hasRuns && !terminal(run.latest),
    ).length,
    failed_runs: failed.length,
    failure_ratio: ratio,
    threshold_ratio: 0.1,
    dags: ids.map((id, i) => ({
      dag_id: id,
      state: !runs[i] ? "unknown" : runs[i]!.latest || "no_runs",
      dag_run_id: runs[i] && terminal(runs[i]!.latest) ? runs[i]!.id : "",
    })),
    failed_dags: failed,
    top_failed_dags: failed.slice(0, 10),
  };
}
export async function fleetInventory() {
  const ids = new Set<string>();
  let offset = 0;
  while (true) {
    const payload = await airflow<{
      dags: { dag_id: string; is_paused: boolean }[];
      total_entries?: number;
    }>(`/dags?limit=100&offset=${offset}&only_active=true`);
    if (
      !Array.isArray(payload.dags) ||
      (!payload.dags.length && (payload.total_entries || 0) > offset)
    )
      throw new Error("Incomplete Airflow inventory");
    for (const dag of payload.dags)
      if (!dag.is_paused && dag.dag_id) ids.add(dag.dag_id);
    offset += payload.dags.length;
    if (
      !payload.dags.length ||
      (payload.total_entries !== undefined
        ? offset >= payload.total_entries
        : payload.dags.length < 100)
    )
      break;
  }
  return [...ids].sort();
}
const reason = (error: unknown) =>
  error instanceof UpstreamError
    ? `HTTP ${error.status}`
    : error instanceof Error
      ? `${error.name}${(error.cause as { code?: string })?.code ? ` ${(error.cause as { code: string }).code}` : ""}`
      : "unknown";
export async function evaluateFleet(
  ids: string[],
  concurrency = 30,
  budget = 230_000,
) {
  // An absolute deadline keeps every request (including pages and retries) inside
  // the 300-second Cron limit, so an unknown snapshot and heartbeat still get written.
  const deadline = Date.now() + budget,
    errors: Record<string, number> = {};
  const attempt = (id: string) =>
    lastRun(id, deadline).catch((error) => {
      errors[reason(error)] = (errors[reason(error)] || 0) + 1;
      return null;
    });
  const runs = await mapConcurrent(ids, concurrency, attempt);
  // Lookups fail transiently (connection resets); retry each once.
  const retry = ids.flatMap((id, i) => (runs[i] ? [] : [i]));
  if (retry.length) {
    await mapConcurrent(retry, concurrency, async (i) => {
      runs[i] = await attempt(ids[i]);
    });
    console.warn("Airflow DAG lookups failed", {
      retried: retry.length,
      unchecked: runs.filter((run) => !run).length,
      reasons: errors,
    });
  }
  return fleetStats(ids, runs);
}
export async function fleetHeartbeat(status: Fleet["status"]) {
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
export async function fleetDashboard() {
  const cached = await readSnapshot<Fleet>("fleet", 180);
  if (cached) return cached;
  if (
    process.env.NODE_ENV === "development" &&
    process.env.AIRFLOW_API_BASE_URL &&
    process.env.AIRFLOW_API_TOKEN
  )
    return evaluateFleet(await fleetInventory());
  return null;
}
export function astroURL(id: string) {
  const base = (process.env.AIRFLOW_API_BASE_URL || "").replace(
    /\/api\/v[12]\/?$/,
    "",
  );
  return /^https?:\/\//.test(base)
    ? `${base}/dags/${encodeURIComponent(id)}/grid`
    : "#";
}
