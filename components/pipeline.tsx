import { Suspense } from "react";
import { Loading, TableScroll, Unavailable } from "./dashboard";
import { openWork, reportFor } from "@/lib/reports";
import { cachedReviewQueue } from "@/lib/reviews";
import {
  approvalAge, duration, durationStats, firstReviewSeconds, mergeBlocker,
  stageSummary, teamIssue, teamPR,
} from "@/lib/pipeline";
import type { Window } from "@/lib/types";

async function LinearStages({ window, everyone }: { window: Window; everyone: boolean }) {
  const data = await Promise.all([reportFor(window), openWork()]).catch(() => null);
  if (!data) return <Unavailable name="Linear stage timing" />;
  const [report, open] = data;
  const summary = stageSummary(
    report.completed.filter((issue) => teamIssue(issue, everyone)),
    open.filter((issue) => teamIssue(issue, everyone)),
  );
  return (
    <section>
      <h3>Time in Linear status</h3>
      <p className="muted">
        Completed issues · {report.window.label}. Median and p85 sum repeat visits
        to each status per issue. Current queues are live, regardless of the selected window.
        Includes project, support, and unassigned work; calendar time, not hands-on coding time.
      </p>
      <TableScroll label="Linear stage timing">
        <table className="numeric">
          <thead><tr>
            <th>Status</th><th>Completed samples</th><th>Median</th><th>p85</th>
            <th>Current work</th><th>Oldest current visit</th>
          </tr></thead>
          <tbody>{summary.rows.map((row) => (
            <tr key={row.state.id || row.state.name}>
              <th>{row.state.name}</th><td>{row.count}</td>
              <td>{duration(row.median)}</td><td>{duration(row.p85)}</td>
              <td>{row.current}</td><td>{row.oldest.length ? duration(row.oldest[0].seconds) : "—"}</td>
            </tr>
          ))}</tbody>
        </table>
        {!summary.rows.length && <p>No work in this cohort.</p>}
      </TableScroll>
      <p className="muted">
        Unknown or inconsistent history: {summary.unknownCompleted} completed,
        {" "}{summary.unknownOpen} current. Unknown durations are excluded, never treated as zero.
      </p>
      {summary.rows.filter((row) => row.current > 0).map((row) => (
        <details key={row.state.id || row.state.name}>
          <summary>{row.state.name} · {row.current} current · oldest work</summary>
          <ul className="issue-list">{row.oldest.map(({ issue, seconds }) => (
            <li key={issue.id}>
              <div><a href={issue.url}>{issue.identifier} · {issue.title}</a>
                <small>{issue.project?.name || "Support / no project"} · {issue.assignee?.displayName || issue.assignee?.name || "Unassigned"}</small>
              </div>
              <span>{duration(seconds)} in status</span>
            </li>
          ))}</ul>
          {row.current > 5 && <p className="muted">Showing the five oldest items with known age first.</p>}
        </details>
      ))}
    </section>
  );
}
async function FirstReviews({ window, everyone }: { window: Window; everyone: boolean }) {
  const report = await reportFor(window).catch(() => null);
  if (!report) return <Unavailable name="First-review timing" />;
  const prs = report.prs.filter((pr) => teamPR(pr, everyone));
  const values = prs.map(firstReviewSeconds).filter((n): n is number => n !== null);
  const stats = durationStats(values);
  return (
    <section>
      <h3>First-review wait</h3>
      <p className="muted">
        PRs merged · {report.window.label}. First ready-for-review event (or creation
        when initially non-draft) → first submitted human review. Excludes author
        and bot reviews; ordinary conversation comments are not reviews.
      </p>
      <div className="cards">
        <article><small>Median first-review wait</small><strong className="metric-value">{duration(stats.median)}</strong></article>
        <article><small>p85 first-review wait</small><strong className="metric-value">{duration(stats.p85)}</strong></article>
        <article><small>PRs with review timing</small><strong className="metric-value">{stats.count} / {prs.length}</strong></article>
      </div>
      <p className="muted">Merged PRs only: these durations do not include work still waiting or abandoned.</p>
    </section>
  );
}
async function Approved({ everyone }: { everyone: boolean }) {
  const queue = await cachedReviewQueue("only").catch(() => null);
  if (!queue) return <Unavailable name="Approved PR aging" />;
  const approved = queue.filter((row) => row.section === "approved" && teamPR(row.pr, everyone))
    .map((row) => ({ ...row, age: approvalAge(row.pr) }))
    .sort((a, b) => (b.age ?? -1) - (a.age ?? -1));
  return (
    <section>
      <h3>Approved but unmerged <span className="badge">{approved.length}</span></h3>
      <p className="muted">Current open, non-draft PRs. Age since latest current approval, not the last review request. Queue refreshes every 60 seconds.</p>
      <TableScroll label="Approved PR aging">
        {approved.length ? <table className="wide">
          <thead><tr><th>PR</th><th>Repository</th><th>Since approval</th><th>Readiness</th></tr></thead>
          <tbody>{approved.slice(0, 10).map(({ pr, age }) => (
            <tr key={pr.id}>
              <td><a href={pr.url}>{pr.title}</a></td><td>{pr.repository.nameWithOwner}</td>
              <td>{duration(age)}</td><td>{mergeBlocker(pr)}</td>
            </tr>
          ))}</tbody>
        </table> : <p>No approved PRs waiting to merge.</p>}
      </TableScroll>
      {approved.length > 10 && <p className="muted">Showing the ten oldest. See the full approved queue on <a href="/reviews?approved=1">Reviews</a>.</p>}
    </section>
  );
}
export function Pipeline({ window, everyone }: { window: Window; everyone: boolean }) {
  return (
    <section aria-label="Shipping pipeline">
      <h2>Shipping pipeline</h2>
      <p>Where work waits across {everyone ? "configured teammates" : "engineering"}.</p>
      <Suspense fallback={<Loading window={window} />}><LinearStages window={window} everyone={everyone} /></Suspense>
      <Suspense fallback={<Loading window={window} />}><FirstReviews window={window} everyone={everyone} /></Suspense>
      <Suspense fallback={<Loading />}><Approved everyone={everyone} /></Suspense>
    </section>
  );
}
