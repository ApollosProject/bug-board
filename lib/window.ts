import type { Search, Window } from "./types";
export const DAY = 86_400_000;
export const param = (s: Search, key: string) =>
  typeof s[key] === "string" ? s[key] : "";
export const date = (n: number | Date) =>
  new Date(n).toISOString().slice(0, 10);
function validDate(s: string) {
  return (
    /^\d{4}-\d{2}-\d{2}$/.test(s) &&
    Number.isFinite(Date.parse(s)) &&
    date(Date.parse(s)) === s
  );
}
export function timeWindow(search: Search = {}, now = Date.now()): Window {
  let start = param(search, "start"),
    end = param(search, "end");
  if (validDate(start) && validDate(end)) {
    if (start > end) [start, end] = [end, start];
    const days = (Date.parse(end) - Date.parse(start)) / DAY + 1;
    if (days > 366) throw new Error("Choose a window of at most 366 days.");
    return {
      start,
      end,
      days,
      preset_days: null,
      label: `${start} – ${end}`,
      after: new Date(start).toISOString(),
      before: new Date(Date.parse(end) + DAY).toISOString(),
      query: { start, end },
    };
  }
  const raw = Number(param(search, "days"));
  const days = Number.isInteger(raw) && raw >= 1 && raw <= 366 ? raw : 30;
  const after = new Date(now - days * DAY).toISOString(),
    before = new Date(now).toISOString();
  return {
    start: after.slice(0, 10),
    end: date(now - 1),
    days,
    preset_days: days,
    label: `${days}d`,
    after,
    before,
    query: { days: String(days) },
  };
}
export const inWindow = (value: string | undefined, w: Window) =>
  !!value &&
  Date.parse(value) >= Date.parse(w.after) &&
  Date.parse(value) < Date.parse(w.before);
export const queryString = (search: Record<string, string>) =>
  new URLSearchParams(search).toString();
