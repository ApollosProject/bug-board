import { Heading, Unavailable } from "@/components/dashboard";
import { fleetDashboard, astroURL } from "@/lib/fleet";
import { param } from "@/lib/window";
import type { Search } from "@/lib/types";
export default async function DAGs({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams,
    query = param(search, "q"),
    state = param(search, "state");
  const fleet = await fleetDashboard().catch(() => null);
  const rows =
    fleet?.dags.filter(
      (dag) =>
        dag.dag_id.toLowerCase().includes(query.toLowerCase()) &&
        (!state || dag.state === state),
    ) || [];
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
            Checked {fleet.checked_at} · {fleet.failed_fetches} fetch errors ·{" "}
            {fleet.non_terminal_dags} non-terminal · {fleet.dags_without_runs}{" "}
            without runs. Degraded at ≥10% failed with at least 20 evaluated
            DAGs.
          </p>
        </>
      ) : (
        <Unavailable
          name="Fleet health"
          detail="Fresh fleet data requires Airflow credentials, Upstash Redis, and the scheduled refresh. Page requests never scan the production fleet."
        />
      )}
      <form method="get" className="filter-form">
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
        <article className="table-scroll">
          <table>
            <caption>{rows.length} DAGs</caption>
            <thead>
              <tr>
                <th>DAG</th>
                <th>Latest state</th>
                <th>Run ID</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((dag) => (
                <tr key={dag.dag_id}>
                  <th>
                    <a href={astroURL(dag.dag_id)}>{dag.dag_id}</a>
                  </th>
                  <td>
                    <span
                      className={`badge ${dag.state === "failed" ? "low" : ""}`}
                    >
                      {dag.state}
                    </span>
                  </td>
                  <td>{dag.dag_run_id || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!rows.length && <p>No matching DAGs.</p>}
        </article>
      )}
    </>
  );
}
