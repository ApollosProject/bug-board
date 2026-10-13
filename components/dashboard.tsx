import Link from "next/link";
import type { Issue, Search, Window } from "@/lib/types";
import { param, queryString } from "@/lib/window";
import { priorityLabels, slugFor, personName } from "@/lib/config";
import { daysSince } from "@/lib/reports";
import { platformFor, slaText } from "@/lib/metrics";
export function Heading({
  title,
  detail,
  path = title.toLowerCase(),
}: {
  title: string;
  detail: string;
  path?: string;
}) {
  return (
    <div className="page-heading">
      <span className="eyebrow">~/{path}</span>
      <h1>{title}</h1>
      <p>{detail}</p>
    </div>
  );
}
export function Unavailable({
  name,
  detail,
}: {
  name: string;
  detail?: string;
}) {
  return (
    <article className="unavailable">
      <div role="status">
        <strong>{name} unavailable</strong>
        <p>
          {detail ||
            "Integration data is unavailable or refreshing. Configure the service credentials and Vercel Cron."}
        </p>
      </div>
    </article>
  );
}
// Keyboard users can only scroll a wide table if its region can take focus.
export function TableScroll({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <article
      className="table-scroll"
      tabIndex={0}
      role="region"
      aria-label={label}
    >
      {children}
    </article>
  );
}
export function Checked({ at }: { at: string }) {
  return (
    <time dateTime={at}>
      Checked{" "}
      {new Date(at).toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
        timeZone: "UTC",
      })}{" "}
      UTC
    </time>
  );
}
// Production's 30-day window uses Cron; previews read through their own cache.
export function Loading({ window }: { window?: Window }) {
  return (
    <article aria-busy="true">
      Loading live data…
      {window && (window.preset_days !== 30 || process.env.VERCEL_ENV === "preview") && (
        <p className="muted">
          {process.env.VERCEL_ENV === "preview"
            ? "Preview data loads on demand from Linear and GitHub, with a separate cache. The first load can take a few minutes."
            : "Only the 30-day preset uses the refreshed snapshot. Other windows query Linear and GitHub directly and can take a few minutes."}
        </p>
      )}
    </article>
  );
}
export function WindowControls({
  window,
  search = {},
  presets = [7, 30, 90],
}: {
  window: Window;
  search?: Search;
  presets?: number[];
}) {
  const extra: Record<string, string> = Object.fromEntries(
    ["sort", "everyone"]
      .map((key) => [key, param(search, key)])
      .filter(([, value]) => value),
  );
  return (
    <section aria-label="Time window" className="window-controls">
      <div className="presets">
        {presets.map((days) => (
          <Link
            key={days}
            className={window.preset_days === days ? "selected" : ""}
            aria-current={window.preset_days === days ? "page" : undefined}
            href={`?${queryString({ ...extra, days: String(days) })}`}
          >
            {days}d
          </Link>
        ))}
      </div>
      <form method="get">
        {Object.entries(extra).map(([key, value]) => (
          <input type="hidden" key={key} name={key} value={value} />
        ))}
        <label>
          Start
          <input
            type="date"
            name="start"
            defaultValue={window.start}
            key={window.start}
            required
          />
        </label>
        <label>
          End
          <input
            type="date"
            name="end"
            defaultValue={window.end}
            key={window.end}
            required
          />
        </label>
        <button type="submit" className="outline">
          Apply dates
        </button>
      </form>
      {!presets.includes(window.preset_days ?? 0) && (
        <small>{window.label}</small>
      )}
    </section>
  );
}
export function IssueList({
  issues,
  empty = "No work in this section.",
}: {
  issues: Issue[];
  empty?: string;
}) {
  return issues.length ? (
    <ul className="issue-list">
      {issues.map((issue) => {
        const slug = slugFor(issue.assignee?.name, issue.assignee?.displayName);
        return (
          <li key={issue.id}>
            <span className={`badge priority-${issue.priority}`}>
              {priorityLabels[issue.priority] || "No priority"}
            </span>
            <div>
              <a href={issue.url}>{issue.title}</a>
              <small>
                {issue.identifier}
                {platformFor(issue) ? ` · ${platformFor(issue)}` : ""} ·{" "}
                {slaText(issue) ||
                  `${daysSince(issue.updatedAt)}d since update`}
              </small>
            </div>
            {slug ? (
              <Link href={`/team/${slug}`}>
                {personName(slug).split(" ")[0]}
              </Link>
            ) : (
              <span className="muted">
                {issue.assignee?.displayName || "Unassigned"}
              </span>
            )}
          </li>
        );
      })}
    </ul>
  ) : (
    <p className="muted">{empty}</p>
  );
}
export function GroupedIssues({
  issues,
  by = (issue) => issue.project?.name || "No Project",
}: {
  issues: Issue[];
  by?: (issue: Issue) => string;
}) {
  const groups = new Map<string, Issue[]>();
  for (const issue of issues) {
    const key = by(issue);
    groups.set(key, [...(groups.get(key) || []), issue]);
  }
  return groups.size ? (
    [...groups].map(([name, group]) => (
      <details key={name}>
        <summary>
          {name} <span className="badge">{group.length}</span>
        </summary>
        <IssueList issues={group} />
      </details>
    ))
  ) : (
    <p className="muted">No work in this section.</p>
  );
}
