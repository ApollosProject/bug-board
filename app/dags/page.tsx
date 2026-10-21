import {
  Checked,
  Heading,
  Unavailable,
  TableScroll,
} from "@/components/dashboard";
import { fleetDashboard, astroURL } from "@/lib/fleet";
import { param } from "@/lib/window";
import type { Search } from "@/lib/types";
export const metadata = { title: "DAGs" };
export default async function DAGs({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams,
    query = param(search, "q"),
    state = param(search, "state");
  const fleet = await fleetDashboard().catch(() => null);
  // Failures first, successes last; the stable sort keeps each group alphabetical.
  const rank = (s: string) => ({ failed: 0, unknown: 1, success: 3 })[s] ?? 2;
  const rows =
    fleet?.dags
      .filter(
        (dag) =>
          dag.dag_id.toLowerCase().includes(query.toLowerCase()) &&
          (!state || dag.state === state),
      )
      .sort((a, b) => rank(a.state) - rank(b.state)) || [];
  return (
    <>
      <Heading
        title="DAGs"
        detail="Active Airflow inventory, latest run state, and fleet outage signals."
      />
      {fleet ? (
        <>
          <div className="cards">
            <article>
              <small>Fleet health</small>
              <strong
                className={`metric-value ${fleet.status === "healthy" ? "high" : "low"}`}
              >
                {fleet.status}
              </strong>
              {!!fleet.failed_fetches && (
                <small>
                  {fleet.failed_fetches} of {fleet.active_dags_total} DAGs not
                  checked
                </small>
              )}
            </article>
            <article>
              <small>Active DAGs</small>
              <strong className="metric-value">
                {fleet.active_dags_total}
              </strong>
            </article>
            <article>
              <small>Failed terminal runs</small>
              <strong className="metric-value">{fleet.failed_runs}</strong>
              <small>
                {(fleet.failure_ratio * 100).toFixed(1)}% of{" "}
                {fleet.evaluated_dags} evaluated
              </small>
            </article>
          </div>
          <p className="muted">
            <Checked at={fleet.checked_at} /> · {fleet.failed_fetches} fetch
            errors · {fleet.non_terminal_dags} non-terminal ·{" "}
            {fleet.dags_without_runs} without runs. Degraded at ≥10% failed with
            at least 20 evaluated DAGs.
          </p>
        </>
      ) : (
        <Unavailable
          name="Fleet health"
          detail="Fresh fleet data requires Airflow credentials, Upstash Redis, and the scheduled refresh. Page requests never scan the production fleet."
        />
      )}
      {/* Stops the browser restoring an unsubmitted selection on Back. */}
      <form method="get" className="filter-form" autoComplete="off">
        <label>
          Search DAGs
          <input
            name="q"
            defaultValue={query}
            type="search"
            placeholder="DAG ID"
          />
        </label>
        <label>
          Run state
          <select name="state" defaultValue={state}>
            <option value="">All states</option>
            {[
              "failed",
              "success",
              "running",
              "queued",
              "unknown",
              "no_runs",
            ].map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <button type="submit">Filter DAGs</button>
      </form>
      {fleet && (
        <TableScroll label="DAGs">
          <table>
            <caption>{rows.length} DAGs</caption>
            <thead>
              <tr>
                <th>DAG / latest run</th>
                <th>Latest state</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((dag) => (
                <tr key={dag.dag_id}>
                  <th>
                    <a href={astroURL(dag.dag_id)}>{dag.dag_id}</a>
                    <small>{dag.dag_run_id || "No run"}</small>
                  </th>
                  <td>
                    <span
                      className={`badge ${dag.state === "failed" ? "low" : ""}`}
                    >
                      {dag.state}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!rows.length && <p>No matching DAGs.</p>}
        </TableScroll>
      )}
    </>
  );
}
