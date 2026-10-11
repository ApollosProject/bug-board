import { Heading, TableScroll, Unavailable } from "@/components/dashboard";
import { openWork, projectDashboard } from "@/lib/reports";
import { linearHygiene } from "@/lib/hygiene";
import { linearListUrl } from "@/lib/metrics";
import type { Issue } from "@/lib/types";
export const metadata = { title: "Linear" };
const open = (issues: Issue[]) =>
  linearListUrl(
    "issues",
    issues.map((i) => i.identifier),
  );
function Count({ issues, label }: { issues: Issue[]; label?: string }) {
  return issues.length ? (
    <a href={open(issues)}>{label ?? issues.length}</a>
  ) : (
    <span className="muted">0</span>
  );
}
export default async function Linear() {
  const data = await Promise.all([openWork(), projectDashboard()]).catch(
    () => null,
  );
  if (!data)
    return (
      <>
        <Heading title="Linear" detail="Upkeep for the Linear workspace." />
        <Unavailable name="Linear snapshot" />
      </>
    );
  const h = linearHygiene(...data);
  const totals = [
    ["Triage", h.triage],
    ["Triage > 7 days", h.triageOver(7)],
    ["Triage > 30 days", h.triageOver(30)],
    ["In progress", h.started.flatMap((s) => s.issues)],
    ["No update in 14 days", h.started.flatMap((s) => s.stale)],
    ["Waiting in Merged", h.merged],
  ] as const;
  return (
    <>
      <Heading
        title="Linear"
        detail="Upkeep for the Linear workspace: old triage, stale work in progress, and projects that hold nothing. Each number opens those issues in Linear."
      />
      <div className="cards">
        {totals.map(([label, issues]) => (
          <article key={label}>
            <small>{label}</small>
            <strong className="metric-value">
              <Count issues={[...issues]} />
            </strong>
          </article>
        ))}
      </div>
      <section>
        <h2>
          Oldest triage <small>{h.triage.length}</small>
        </h2>
        <TableScroll label="Oldest triage">
          <table className="wide">
            <thead>
              <tr>
                <th>Issue</th>
                <th>Age</th>
                <th>Labels</th>
                <th>Assignee</th>
              </tr>
            </thead>
            <tbody>
              {h.triage.slice(0, 25).map((issue) => (
                <tr key={issue.id}>
                  <td>
                    <a href={issue.url}>{issue.title}</a>
                    <small>{issue.identifier}</small>
                  </td>
                  <td>{h.age(issue.createdAt)}d</td>
                  <td>{issue.labels.nodes.map((l) => l.name).join(", ")}</td>
                  <td>{h.owner(issue) || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {h.triage.length > 25 && (
            <p>
              <Count
                issues={h.triage}
                label={`Open all ${h.triage.length} in Linear`}
              />
            </p>
          )}
          {!h.triage.length && <p>Triage is empty.</p>}
        </TableScroll>
      </section>
      <section>
        <h2>Work in progress by person</h2>
        <TableScroll label="Work in progress by person">
          <table className="numeric">
            <thead>
              <tr>
                <th>Person</th>
                <th>In progress</th>
                <th>No update in 14 days</th>
              </tr>
            </thead>
            <tbody>
              {h.started.map((row) => (
                <tr key={row.who}>
                  <th>{row.who}</th>
                  <td>
                    <Count issues={row.issues} />
                  </td>
                  <td>
                    <Count issues={row.stale} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableScroll>
      </section>
      <section>
        <h2>
          Waiting in Merged <small>{h.merged.length}</small>
        </h2>
        <p className="muted">
          Issues in the Merged status, waiting to be released or closed. The
          oldest last changed{" "}
          {h.merged[0] ? `${h.age(h.merged[0].updatedAt)} days` : "—"} ago.{" "}
          {!!h.merged.length && (
            <Count issues={h.merged} label="Open them in Linear" />
          )}
        </p>
      </section>
      <section>
        <h2>Open issues without an owner or project</h2>
        <TableScroll label="Open issues without an owner or project">
          <table className="numeric">
            <thead>
              <tr>
                <th>Label</th>
                <th>No assignee</th>
                <th>No project</th>
              </tr>
            </thead>
            <tbody>
              {[
                ...new Set(
                  [...h.unassigned, ...h.noProject].map(([label]) => label),
                ),
              ].map((label) => (
                <tr key={label}>
                  <th>{label}</th>
                  <td>
                    <Count
                      issues={
                        h.unassigned.find(([l]) => l === label)?.[1] || []
                      }
                    />
                  </td>
                  <td>
                    <Count
                      issues={h.noProject.find(([l]) => l === label)?.[1] || []}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableScroll>
      </section>
      <section>
        <h2>
          Not Now and paused projects with at most one open issue{" "}
          <small>{h.emptyProjects.length}</small>
        </h2>
        <article>
          {h.emptyProjects.length ? (
            <ul className="link-list">
              {h.emptyProjects.map(({ project, open }) => (
                <li key={project.id}>
                  <a href={project.url}>{project.name}</a>{" "}
                  <small>
                    {project.status.name} · {open} open
                  </small>
                </li>
              ))}
            </ul>
          ) : (
            <p className="muted">None.</p>
          )}
        </article>
      </section>
    </>
  );
}
