import { Suspense, type CSSProperties } from "react";
import Link from "next/link";
import { Heading, Loading, Unavailable } from "@/components/dashboard";
import { engineers, personName, slugFor, normalize } from "@/lib/config";
import { inactive, done, blocked } from "@/lib/metrics";
import { projectDashboard } from "@/lib/reports";
import { ptoCalendar } from "@/lib/pto";
import { readSnapshot } from "@/lib/cache";
import { DAY, date, timeWindow } from "@/lib/window";
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
          if (!p.startDate && !p.targetDate && !p.completedAt) return [];
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
          return [
            {
              id: p.id,
              name: p.name,
              href: p.url,
              start: Math.max(a, start),
              end: Math.min(b, end - DAY),
              className: done(p)
                ? "completed"
                : overdue
                  ? "overdue"
                  : p.health || "neutral",
              title: `${p.name} · Priority: ${p.priorityLabel || "No priority"} · ${p.startDate || "Earlier"} – ${hint?.slice(0, 10) || "Ongoing"}`,
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
            start: Math.max(a, start),
            end: Math.min(b, end - DAY),
            className: event.allDay ? "ooo" : "ooo partial",
            title: `${event.allDay ? "OOO" : "Partial-day OOO"} · ${event.start} – ${event.end}`,
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
        Six-week view · {label(start)} – {label(end - DAY)} · Red line is today
        {calendar.available
          ? " · OOO from Rippling"
          : calendar.configured
            ? " · Rippling OOO unavailable"
            : ""}
      </p>
      {!!ready.length && (
        <article>
          <h3>Ready · Unassigned projects</h3>
          <div className="toolbar">
            {ready.map((p) => (
              <a href={p.url} key={p.id}>
                {p.name} <small>{p.priorityLabel}</small>
              </a>
            ))}
          </div>
        </article>
      )}
      <div
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
          {rows.map((row) => (
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
                {row.bars.map((bar) => {
                  const style = {
                    gridColumn: `${Math.floor((bar.start - start) / DAY) + 1} / span ${Math.floor((bar.end - bar.start) / DAY) + 1}`,
                    gridRow: bar.lane,
                  };
                  return bar.href ? (
                    <a
                      key={bar.id}
                      href={bar.href}
                      className={`timeline-bar ${bar.className}`}
                      title={bar.title}
                      style={style}
                    >
                      {bar.name}
                    </a>
                  ) : (
                    <span
                      key={bar.id}
                      className={`timeline-bar ${bar.className}`}
                      title={bar.title}
                      aria-label={bar.title}
                      style={style}
                    >
                      {bar.name}
                    </span>
                  );
                })}
                {!row.bars.length && (
                  <small className="timeline-empty">No dated projects</small>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
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
