import { Fragment, Suspense, type CSSProperties } from "react";
import Link from "next/link";
import { Heading, Loading, Unavailable } from "@/components/dashboard";
import { LongPressCards } from "@/components/long-press";
import { engineers, personName, slugFor, normalize, priorityLabels } from "@/lib/config";
import { inactive, done, blocked } from "@/lib/metrics";
import { projectDashboard } from "@/lib/reports";
import { ptoCalendar } from "@/lib/pto";
import { readSnapshot } from "@/lib/cache";
import { DAY, date, timeWindow } from "@/lib/window";
export const metadata = { title: "Projects" };
const health: Record<string, string> = { onTrack: "On track", atRisk: "At risk", offTrack: "Off track" };
// Linear's priority icons: signal bars for High to Very Low, a badge for Urgent, dashes for none.
function ProjectPriority({ label = "No priority" }: { label?: string }) {
  const filled = { High: 3, Medium: 2, Low: 1, "Very Low": 0 }[label];
  return (
    <svg
      className="project-priority"
      viewBox="0 0 16 16"
      role="img"
      aria-label={`Priority: ${label}`}
    >
      <title>{`Priority: ${label}`}</title>
      {label === "Urgent" ? (
        <path
          fillRule="evenodd"
          d="M3 1h10a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V3a2 2 0 0 1 2-2ZM7 3.5v6h2v-6ZM7 11v2h2v-2Z"
        />
      ) : filled !== undefined ? (
        [0, 1, 2].map((i) => (
          <rect
            key={i}
            x={1.5 + i * 5}
            y={10 - i * 4}
            width="3"
            height={5 + i * 4}
            rx="1"
            opacity={i < filled ? 1 : 0.35}
          />
        ))
      ) : (
        [0, 1, 2].map((i) => (
          <rect key={i} x={1.5 + i * 5} y="7" width="3" height="2" rx="1" opacity="0.6" />
        ))
      )}
    </svg>
  );
}
export async function Timeline() {
  const data = await Promise.all([
    projectDashboard(),
    readSnapshot<Awaited<ReturnType<typeof ptoCalendar>>>("pto", 900).then(
      (cached) =>
        cached ||
        (process.env.VERCEL_ENV
          ? {
              configured: !!process.env.RIPPLING_PTO_CALENDAR_URL,
              available: false,
              events: [],
            }
          : ptoCalendar()),
    ),
  ]).catch(() => null);
  if (!data) return <Unavailable name="Projects" />;
  const [projects, calendar] = data;
  const today = Date.parse(timeWindow().before.slice(0, 10)),
    start = today - ((new Date(today).getUTCDay() + 6) % 7) * DAY,
    end = start + 42 * DAY;
  const ready = projects.filter(
    (p) => p.status.name.trim().toLowerCase() === "ready" && !p.lead?.displayName && !blocked(p),
  );
  const label = (value: number) =>
    new Date(value).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      timeZone: "UTC",
    });
  const rows = engineers
    .slice()
    .sort((a, b) => personName(a).localeCompare(personName(b)))
    .map((slug) => {
      const bars = projects
        .filter(
          (p) =>
            (!inactive(p) || done(p)) &&
            [p.lead, ...p.members.nodes].some(
              (member) => slugFor(member?.displayName) === slug,
            ),
        )
        .flatMap((p) => {
          const lead = slugFor(p.lead?.displayName) === slug;
          // Linear's status type, not its custom name, says whether work has started.
          const idle = !done(p) && p.status.type !== "started";
          const undated = !p.startDate && !p.targetDate && !p.completedAt;
          if (undated && inactive(p)) return [];
          const a = Date.parse(p.startDate || date(start)),
            hint = done(p) ? p.completedAt || p.targetDate : p.targetDate;
          const overdue =
            !inactive(p) && p.targetDate && Date.parse(p.targetDate) < today;
          const b = Math.max(
            Date.parse(hint?.slice(0, 10) || date(end - DAY)),
            overdue ? today : -Infinity,
            a,
          );
          if (b < start || a >= end) return [];
          const members = p.members.nodes
            .map((m) => m.displayName)
            .filter((name) => name && name !== p.lead?.displayName);
          return [
            {
              id: p.id,
              name: p.name,
              href: p.url,
              priorityLabel: p.priorityLabel,
              start: Math.max(a, start),
              end: Math.min(b, end - DAY),
              className: `${done(p) ? "completed" : overdue ? "overdue" : p.health || "neutral"}${idle ? " idle" : ""}${lead ? " lead" : ""}`,
              summary: p.description,
              progress: p.progress,
              details: [
                [
                  "Status",
                  [p.status.name, health[p.health || ""], overdue && "Overdue", blocked(p) && "Blocked"]
                    .filter(Boolean)
                    .join(" · "),
                ],
                ["Priority", p.priorityLabel || "No priority"],
                ["Dates", undated ? "Not set" : `${p.startDate || "Earlier"} – ${hint?.slice(0, 10) || "Ongoing"}`],
                ["Lead", p.lead?.displayName || "None"],
                ["Members", members.join(", ")],
                ["Initiatives", p.initiatives.nodes.map((i) => i.name).join(", ")],
                ["Updated", p.lastUpdate?.createdAt.slice(0, 10)],
              ].filter((row): row is [string, string] => !!row[1]),
              lane: 1,
            },
          ];
        });
      const ooo = calendar.events.flatMap((event, i) => {
        const matches = engineers.filter(
          (person) =>
            normalize(event.summary).startsWith(
              normalize(personName(person)),
            ) ||
            event.summary.split(/\s/)[0].toLowerCase() === person.toLowerCase(),
        );
        const a = Date.parse(event.start),
          b = Date.parse(event.end);
        if (
          matches.length !== 1 ||
          matches[0] !== slug ||
          b < start ||
          a >= end
        )
          return [];
        return [
          {
            id: `ooo-${i}`,
            name: "OOO",
            href: "",
            priorityLabel: undefined,
            start: Math.max(a, start),
            end: Math.min(b, end - DAY),
            className: event.allDay ? "ooo" : "ooo partial",
            summary: `${event.allDay ? "All day" : "Partial day"} · ${event.start} – ${event.end}`,
            progress: undefined,
            details: [],
            lane: 1,
          },
        ];
      });
      const ends: number[] = [];
      for (const bar of [...bars, ...ooo].sort(
        (a, b) => a.start - b.start || a.end - b.end,
      )) {
        const lane = ends.findIndex((end) => end < bar.start);
        bar.lane = lane === -1 ? ends.length + 1 : lane + 1;
        ends[bar.lane - 1] = bar.end;
      }
      return { slug, bars: [...bars, ...ooo], lanes: Math.max(ends.length, 1) };
    });
  return (
    <>
      <h2>Timeline</h2>
      <p className="muted">
        Six-week view · {label(start)} – {label(end - DAY)}
        {calendar.available
          ? " · OOO from Rippling"
          : calendar.configured
            ? " · Rippling OOO unavailable"
            : ""}
      </p>
      <ul className="legend" aria-label="Timeline key">
        {[
          ["neutral", "No health update"],
          ["onTrack", "On track"],
          ["atRisk", "At risk"],
          ["offTrack", "Off track or overdue"],
          ["completed", "Completed"],
          ["neutral idle", "Not started or paused"],
          ["ooo", "Out of office"],
          ["ooo partial", "Partial day out"],
          ["neutral lead", "Project lead"],
        ].map(([className, name]) => (
          <li key={name}>
            <i className={`timeline-bar ${className}`} aria-hidden="true" />
            {name}
          </li>
        ))}
        <li>
          <i className="today" aria-hidden="true" />
          Today
        </li>
      </ul>
      <ul className="legend" aria-label="Priority key">
        {[...Object.values(priorityLabels).slice(1), priorityLabels[0]].map((name) => (
          <li key={name}>
            <ProjectPriority label={name} />
            {name}
          </li>
        ))}
      </ul>
      {!!ready.length && (
        <article>
          <h3>Ready · Unassigned projects</h3>
          <div className="toolbar">
            {ready.map((p) => (
              <a
                href={p.url}
                key={p.id}
                target="_blank"
                rel="noopener noreferrer"
                title="Opens in a new tab"
              >
                <ProjectPriority label={p.priorityLabel} /> {p.name}
              </a>
            ))}
          </div>
        </article>
      )}
      <LongPressCards
        className="timeline-scroll"
        tabIndex={0}
        role="region"
        aria-label="Developer project timeline"
      >
        <div className="timeline">
          <div className="timeline-row">
            <strong>Developer</strong>
            <div className="weeks">
              {Array.from({ length: 6 }, (_, i) => (
                <span key={i}>
                  {label(start + i * 7 * DAY)} –{" "}
                  {label(start + (i * 7 + 6) * DAY)}
                </span>
              ))}
            </div>
          </div>
          {rows.map((row, r) => (
            <div className="timeline-row" key={row.slug}>
              <Link href={`/team/${row.slug}`}>
                {personName(row.slug).split(" ")[0]}
              </Link>
              <div
                className="timeline-track"
                style={
                  {
                    "--lanes": row.lanes,
                    "--today": `${(((today - start) / DAY + 0.5) / 42) * 100}%`,
                  } as CSSProperties
                }
              >
                <i className="today" aria-hidden="true" />
                {row.bars.map((bar, b) => {
                  // Projects can appear in several rows, so ids come from position.
                  const card = `card-${r}-${b}`;
                  const style = {
                    gridColumn: `${Math.floor((bar.start - start) / DAY) + 1} / span ${Math.floor((bar.end - bar.start) / DAY) + 1}`,
                    gridRow: bar.lane,
                    anchorName: `--${card}`,
                  };
                  return (
                    <Fragment key={bar.id}>
                      {bar.href ? (
                        <a
                          href={bar.href}
                          target="_blank"
                          rel="noopener noreferrer"
                          className={`timeline-bar ${bar.className}`}
                          aria-describedby={card}
                          style={style}
                        >
                          <ProjectPriority label={bar.priorityLabel} />
                          <span className="timeline-label">{bar.name}</span>
                        </a>
                      ) : (
                        <span
                          className={`timeline-bar ${bar.className}`}
                          aria-label={`${bar.name} · ${bar.summary}`}
                          style={style}
                        >
                          {bar.name}
                        </span>
                      )}
                      <div
                        className="project-card"
                        id={card}
                        role="tooltip"
                        popover="manual"
                        style={{ positionAnchor: `--${card}` }}
                      >
                        <strong>{bar.name}</strong>
                        {bar.summary && <p>{bar.summary}</p>}
                        {bar.progress !== undefined && (
                          <p>
                            <meter value={bar.progress} />
                            {Math.round(bar.progress * 100)}%
                          </p>
                        )}
                        {!!bar.details.length && (
                          <dl>
                            {bar.details.map(([name, value]) => (
                              <div key={name}>
                                <dt>{name}</dt>
                                <dd>{value}</dd>
                              </div>
                            ))}
                          </dl>
                        )}
                        {bar.href && <small>Opens in Linear in a new tab</small>}
                      </div>
                    </Fragment>
                  );
                })}
                {!row.bars.length && (
                  <small className="timeline-empty">No projects in this window</small>
                )}
              </div>
            </div>
          ))}
        </div>
      </LongPressCards>
    </>
  );
}
export default function Projects() {
  return (
    <>
      <Heading
        title="Projects"
        detail="A six-week view of engineering commitments, ownership, and time away."
      />
      <Suspense fallback={<Loading />}>
        <Timeline />
      </Suspense>
    </>
  );
}
