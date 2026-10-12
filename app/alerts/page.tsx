import { Heading, TableScroll, Unavailable } from "@/components/dashboard";
import { alertStats, recentIncidents } from "@/lib/alerts";
export const metadata = { title: "Alerts" };
const duration = (minutes: number | null) =>
  minutes === null
    ? "—"
    : minutes < 60
      ? `${Math.round(minutes)}m`
      : `${(minutes / 60).toFixed(1)}h`;
export default async function Alerts() {
  // null: no token configured; undefined: Better Stack failed or was incomplete.
  const incidents = await recentIncidents().catch(() => undefined);
  const stats = incidents ? alertStats(incidents) : [];
  const total = stats.reduce((n, s) => n + s.incidents, 0),
    silent = stats.reduce((n, s) => n + s.unanswered, 0),
    flapping = stats.filter((s) => s.flapping);
  return (
    <>
      <Heading
        title="Alerts"
        detail="Better Stack incidents over 30 days: which monitors fire most, clear by themselves, and get no response. Flapping monitors are candidates for tuning."
      />
      {!incidents ? (
        <Unavailable
          name="Alert history"
          detail={
            incidents === null
              ? "Alert history requires a read-only Better Stack Uptime API token in BETTERSTACK_API_TOKEN."
              : undefined
          }
        />
      ) : (
        <>
          <div className="cards">
            <article>
              <small>Incidents (30 days)</small>
              <strong className="metric-value">{total}</strong>
            </article>
            <article>
              <small>No response</small>
              <strong className="metric-value">
                {total ? `${Math.round((silent / total) * 100)}%` : "—"}
              </strong>
            </article>
            <article>
              <small>Flapping monitors</small>
              <strong
                className={`metric-value ${flapping.length ? "low" : ""}`}
              >
                {flapping.length}
              </strong>
            </article>
          </div>
          <section>
            <h2>By monitor</h2>
            <TableScroll label="Incidents by monitor">
              <table className="wide numeric">
                <thead>
                  <tr>
                    <th>Monitor</th>
                    <th>Incidents</th>
                    <th>No response</th>
                    <th>Median to resolve</th>
                    <th>Peak in one hour</th>
                    <th>Last</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.map((s) => (
                    <tr key={s.name}>
                      <th>
                        {s.name}
                        {s.flapping && (
                          <small>
                            <span className="badge low">flapping</span>
                          </small>
                        )}
                        {!!s.ongoing && <small>{s.ongoing} ongoing</small>}
                      </th>
                      <td>{s.incidents}</td>
                      <td>{s.unanswered}</td>
                      <td>{duration(s.medianMinutes)}</td>
                      <td>{s.peakHour}</td>
                      <td>
                        <time dateTime={s.last}>{s.last.slice(5, 10)}</time>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!stats.length && <p>No incidents in the last 30 days.</p>}
            </TableScroll>
          </section>
          <p className="muted">
            Flapping: at least 10 resolved incidents, 80% or more of them with
            no one acknowledging or resolving them, and a median of 30 minutes
            or less to clear.{" "}
            <a href="https://uptime.betterstack.com/incidents">
              Open Better Stack incidents
            </a>
          </p>
        </>
      )}
    </>
  );
}
