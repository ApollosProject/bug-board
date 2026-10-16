import { Suspense } from "react";
import { Loading, TableScroll, Unavailable } from "./dashboard";
import { openWork, reportFor } from "@/lib/reports";
import { cachedReviewQueue } from "@/lib/reviews";
import {
  approvalAge, customerImpact, duration, durationStats, feedbackSeconds, firstReviewSeconds,
  mergeBlocker, mergeToProduction, recovery, stageSummary, teamIssue, teamPR,
} from "@/lib/pipeline";
import { fixingURLs, regressionDashboard } from "@/lib/regressions";
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
function Timing({ label, rows }: {
  label: string;
  rows: { name: string; values: (number | null)[]; detail: string }[];
}) {
  return (
    <TableScroll label={label}>
      <table className="numeric">
        <thead><tr><th>Measure</th><th>Median</th><th>p85</th><th>Samples</th><th>From → to</th></tr></thead>
        <tbody>{rows.map(({ name, values, detail }) => {
          const stats = durationStats(values.filter((n): n is number => n !== null));
          return (
            <tr key={name}>
              <th>{name}</th><td>{duration(stats.median)}</td><td>{duration(stats.p85)}</td>
              <td>{stats.count} / {values.length}</td><td>{detail}</td>
            </tr>
          );
        })}</tbody>
      </table>
    </TableScroll>
  );
}
async function FirstReviews({ window, everyone }: { window: Window; everyone: boolean }) {
  const report = await reportFor(window).catch(() => null);
  if (!report) return <Unavailable name="First-review timing" />;
  const prs = report.prs.filter((pr) => teamPR(pr, everyone));
  const feedback = prs.map(feedbackSeconds);
  const both = feedback.filter((f) => f.automated !== null && f.decision !== null);
  return (
    <section>
      <h3>First-review wait</h3>
      <p className="muted">
        PRs merged · {report.window.label}, timed from the first ready-for-review event (or creation
        when initially non-draft). Automated feedback is the CI verdict (every GitHub Actions workflow
        from the PR&apos;s first push finished) or the first bot/AI review, whichever comes first;
        feedback that already existed when the PR became ready counts as 0. Human reviews exclude the author, bots, and configured AI reviewers.
      </p>
      <Timing label="First-review timing" rows={[
        { name: "First automated feedback", values: feedback.map((f) => f.automated), detail: "Ready → CI verdict or bot/AI review" },
        { name: "First human review", values: prs.map(firstReviewSeconds), detail: "Ready → any submitted human review" },
        { name: "First human decision", values: feedback.map((f) => f.decision), detail: "Ready → human approval or changes requested" },
      ]} />
      <p className="muted">
        Automated feedback arrived before the first human decision on{" "}
        {both.filter((f) => f.automated! <= f.decision!).length} of {both.length} PRs with both.
        Merged PRs only: these durations do not include work still waiting or abandoned.
      </p>
    </section>
  );
}
async function Delivery({ window, everyone }: { window: Window; everyone: boolean }) {
  const report = await reportFor(window).catch(() => null);
  if (!report) return <Unavailable name="Delivery timing" />;
  const prs = report.prs.filter((pr) => teamPR(pr, everyone));
  const shipped = report.completed.filter((issue) => teamIssue(issue, everyone) && fixingURLs(issue).length);
  return (
    <section>
      <h3>CI and production</h3>
      <p className="muted">
        CI times use GitHub Actions runs on each merged PR&apos;s final commit; reruns extend wall time.
        Production times use Linear production releases, which deploy workflows complete only after
        verifying the live runtime. Issues without a completed production release, including
        repositories that don&apos;t sync Linear releases, are excluded and never counted as zero.
      </p>
      <Timing label="CI and production timing" rows={[
        { name: "Runner queue", values: prs.map((pr) => pr.delivery?.queue ?? null), detail: "Workflow created → first job started (slowest workflow)" },
        { name: "Longest build/test job", values: prs.map((pr) => pr.delivery?.job ?? null), detail: "Job started → finished" },
        { name: "CI wall time", values: prs.map((pr) => pr.delivery?.ci ?? null), detail: "First workflow created → last job finished" },
        { name: "Merge → verified production", values: shipped.map((issue) => mergeToProduction(issue, report.prs)), detail: "Last linked PR merged → Linear production release completed" },
      ]} />
    </section>
  );
}
async function Failures({ window, everyone }: { window: Window; everyone: boolean }) {
  const [report, summary] = await Promise.all([reportFor(window), regressionDashboard()]).catch(() => [null, null]);
  if (!report) return <Unavailable name="Change failures and recovery" />;
  const { bugs, fixed, production } = recovery(report.completed.filter((issue) => teamIssue(issue, everyone)));
  const customer = bugs.filter(customerImpact);
  const introduced = new Set([...(summary?.author_metrics || []), ...(summary?.reviewer_metrics || [])]
    .flatMap((m) => m.attributions.map((a) => a.identifier)));
  return (
    <section>
      <h3>Change failures and recovery</h3>
      <p className="muted">
        Change failures are fixed Urgent/High bugs attributed to a merged engineering PR (labeled 30-day
        attribution window). Customer impact means the Linear issue has a customer request or support ticket.
        Recovery covers Urgent/High bugs completed · {report.window.label}.
      </p>
      <div className="cards">
        <article><small>Change failure rate <span className="badge">30d</span></small>
          <strong className="metric-value">{summary?.author_regression_rate == null ? "Unknown" : `${summary.author_regression_rate}%`}</strong>
          <small>{summary ? `${summary.authored_regression_count} introduced bugs in engineering PRs` : "Attribution refreshing"}</small></article>
        <article><small>Customer-impacting priority bugs</small>
          <strong className="metric-value">{customer.length} / {bugs.length}</strong>
          <small>{summary ? `${customer.filter((i) => introduced.has(i.identifier)).length} attributed to a merged PR` : "Attribution refreshing"}</small></article>
      </div>
      <Timing label="Recovery timing" rows={[
        { name: "Recovery to fix completed", values: fixed, detail: "Bug created → Linear completed" },
        { name: "Recovery to verified production", values: production, detail: "Bug created → Linear production release completed" },
      ]} />
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
      {approved.length > 10 && <p className="muted">Showing the ten oldest. <a href="/reviews?approved=1">View full approved queue</a>.</p>}
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
      <Suspense fallback={<Loading window={window} />}><Delivery window={window} everyone={everyone} /></Suspense>
      <Suspense fallback={<Loading window={window} />}><Failures window={window} everyone={everyone} /></Suspense>
      <Suspense fallback={<Loading />}><Approved everyone={everyone} /></Suspense>
    </section>
  );
}
