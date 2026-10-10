import ical from "node-ical";
import { DAY, date } from "./window";
export type PTO = {
  summary: string;
  start: string;
  end: string;
  allDay: boolean;
};
export function calendarURL(value: string) {
  const url = new URL(value);
  if (
    !["https:", "webcal:"].includes(url.protocol) ||
    url.hostname !== "app.rippling.com" ||
    url.port ||
    url.username ||
    url.password ||
    url.hash ||
    !url.pathname.startsWith("/api/feed/calendar/pto/")
  )
    throw new Error("Invalid Rippling PTO URL");
  return new URL(url.href.replace(/^webcal:/, "https:"));
}
export function parseCalendar(
  text: string,
  timezone = "America/New_York",
): PTO[] {
  if (!text.includes("BEGIN:VCALENDAR") || !text.includes("END:VCALENDAR"))
    throw new Error("Invalid calendar");
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const localDate = (d: Date) => formatter.format(d);
  const zoned = text.replace(
    /^(DTSTART|DTEND):(\d{8}T\d{6})\r?$/gm,
    `$1;TZID=${timezone}:$2`,
  );
  return Object.values(ical.sync.parseICS(zoned)).flatMap((event) => {
    if (
      !event ||
      event.type !== "VEVENT" ||
      event.status === "CANCELLED" ||
      !event.summary ||
      !event.start ||
      !event.end ||
      event.end < event.start
    )
      return [];
    const allDay = event.datetype === "date";
    const start = allDay ? date(event.start) : localDate(event.start);
    const endInstant =
      event.end.getTime() > event.start.getTime()
        ? event.end.getTime() - (allDay ? DAY : 1)
        : event.end.getTime();
    return [
      {
        summary:
          typeof event.summary === "string" ? event.summary : event.summary.val,
        start,
        end: allDay ? date(endInstant) : localDate(new Date(endInstant)),
        allDay,
      },
    ];
  });
}
export async function ptoCalendar() {
  const value = process.env.RIPPLING_PTO_CALENDAR_URL;
  if (!value)
    return { configured: false, available: false, events: [] as PTO[] };
  try {
    const response = await fetch(calendarURL(value), {
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) throw new Error("PTO unavailable");
    const bytes = await response.arrayBuffer();
    if (bytes.byteLength > 1_000_000) throw new Error("Calendar too large");
    return {
      configured: true,
      available: true,
      events: parseCalendar(
        new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        process.env.RIPPLING_PTO_TIMEZONE || "America/New_York",
      ),
    };
  } catch {
    return { configured: true, available: false, events: [] as PTO[] };
  }
}
