import {
  Checked,
  Heading,
  TableScroll,
  Unavailable,
} from "@/components/dashboard";
import { deploys, type Stage } from "@/lib/deploys";
export const metadata = { title: "Deploys" };
const when = (iso: string) =>
  new Date(iso).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZone: "UTC",
  }) + " UTC";
const tone = (run: Stage["latest"]) =>
  !run || run.status !== "completed"
    ? ""
    : run.conclusion === "success"
      ? "high"
      : run.conclusion === "failure"
        ? "low"
        : "";
export default async function Deploys() {
  const data = await deploys().catch(() => null);
  return (
    <>
      <Heading
        title="Deploys"
        detail="What is on staging and production for each service, and what has merged but not shipped. From GitHub Actions, cached for up to five minutes."
      />
      {/* The Data Cache can serve the previous copy while it refreshes. */}
      {data && (
        <p className="muted">
          <Checked at={data.checked_at} />
        </p>
      )}
      {!data ? (
        <Unavailable
          name="Deploy status"
          detail="Deploy status requires GITHUB_TOKEN with Actions read access."
        />
      ) : (
        data.pipelines.map((p) => (
          <section key={p.name}>
            <h2>
              {p.name} <small>{p.repo}</small>
            </h2>
            {p.error ? (
              <Unavailable
                name={`${p.name} status`}
                detail="GitHub Actions data for this pipeline is unavailable."
              />
            ) : (
              <>
                {!p.stages.some((s) => s.live) && (
                  <p className="muted">
                    Job status only: published store builds are verified on{" "}
                    <a href="/apps">Apps</a>.
                  </p>
                )}
                {p.waiting && (
                  <p className="muted">
                    {p.waiting.count ? (
                      <>
                        <a href={p.waiting.url}>
                          {p.waiting.count} commit
                          {p.waiting.count === 1 ? "" : "s"} on {p.branch} not
                          in production
                        </a>
                        {": "}
                        {p.waiting.titles.join(" · ")}
                        {p.waiting.count > p.waiting.titles.length && " · …"}
                      </>
                    ) : (
                      `Production is up to date with ${p.branch}.`
                    )}
                  </p>
                )}
                <TableScroll label={`${p.name} pipeline`}>
                  <table className="wide stages">
                    <thead>
                      <tr>
                        <th>Stage</th>
                        <th>Last success</th>
                        <th>Latest run</th>
                        <th>Failures (7 days)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {p.stages.map((s) => (
                        <tr key={s.label}>
                          <th>
                            {s.label}
                            {s.live && <small>what users run</small>}
                          </th>
                          <td>
                            {s.success ? (
                              <>
                                <a href={s.success.html_url}>
                                  <code>{s.success.head_sha.slice(0, 7)}</code>
                                </a>{" "}
                                {s.success.head_branch &&
                                  s.success.head_branch !== p.branch && (
                                    <code>{s.success.head_branch}</code>
                                  )}
                                <small>
                                  <time dateTime={s.success.created_at}>
                                    {when(s.success.created_at)}
                                  </time>
                                </small>
                              </>
                            ) : (
                              "None in the last 20 runs"
                            )}
                          </td>
                          <td>
                            {s.latest ? (
                              <>
                                <a
                                  className={`badge ${tone(s.latest)}`}
                                  href={s.latest.html_url}
                                >
                                  {s.latest.conclusion || s.latest.status}
                                </a>
                                <small>
                                  <time dateTime={s.latest.created_at}>
                                    {when(s.latest.created_at)}
                                  </time>{" "}
                                  · {s.latest.display_title}
                                </small>
                              </>
                            ) : (
                              "—"
                            )}
                          </td>
                          <td>
                            {s.failures ? (
                              <span className="badge low">{s.failures}</span>
                            ) : (
                              <span className="muted">0</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </TableScroll>
              </>
            )}
          </section>
        ))
      )}
    </>
  );
}
