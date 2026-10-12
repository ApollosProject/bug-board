import { unstable_cache } from "next/cache";
import { requestJson } from "./http";
import { DAY } from "./window";
export type Incident = {
  name: string;
  started_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
  resolved_by?: string | null;
  // Current state; a reopened incident keeps its old resolved_at.
  status?: string;
  // Team plus monitor or heartbeat ID; names can repeat or change.
  source?: string;
};
// A person acknowledged or resolved it. Better Stack documents a person's name
// in resolved_by; an "Automatically"-style value is treated as no response.
const isResolved = (i: Incident) =>
  i.status ? i.status.toLowerCase() === "resolved" : !!i.resolved_at;
const answered = (i: Incident) =>
  !!i.acknowledged_at || !!(i.resolved_by && !/^auto/i.test(i.resolved_by));
const HOUR = 3_600_000;
// Per-monitor noise: how often it fires, how often anyone responds, and how
// quickly it clears on its own.
export function alertStats(incidents: Incident[], now = Date.now()) {
  const groups = new Map<string, Incident[]>();
  for (const i of incidents)
    if (now - Date.parse(i.started_at) < 30 * DAY) {
      const key = i.source || i.name;
      groups.set(key, [...(groups.get(key) || []), i]);
    }
  return [...groups]
    .map(([source, list]) => {
      const starts = list
        .map((i) => Date.parse(i.started_at))
        .sort((a, b) => a - b);
      const resolved = list.filter(isResolved);
      const minutes = resolved
        .map(
          (i) =>
            (Date.parse(i.resolved_at!) - Date.parse(i.started_at)) / 60_000,
        )
        .sort((a, b) => a - b);
      const unanswered = list.filter((i) => !answered(i)).length;
      // Self-clearing is judged on resolved incidents only.
      const cleared = resolved.filter((i) => !answered(i)).length;
      const mid = minutes.length / 2;
      const median = !minutes.length
        ? null
        : minutes.length % 2
          ? minutes[Math.floor(mid)]
          : (minutes[mid - 1] + minutes[mid]) / 2;
      // Most incidents that started within any one hour.
      let peak = 0;
      for (let a = 0, b = 0; b < starts.length; b++) {
        while (starts[b] - starts[a] >= HOUR) a++;
        peak = Math.max(peak, b - a + 1);
      }
      const newest = list.reduce((a, b) =>
        a.started_at > b.started_at ? a : b,
      );
      return {
        source,
        // Shown when two rows share a name.
        team: source.includes("|") ? source.split("|")[0] : null,
        name: newest.name,
        incidents: list.length,
        unanswered,
        ongoing: list.filter((i) => !isResolved(i)).length,
        medianMinutes: median,
        peakHour: peak,
        last: new Date(Math.max(...starts)).toISOString(),
        // Fires often, clears by itself, and almost nobody responds.
        flapping:
          resolved.length >= 10 &&
          cleared / resolved.length >= 0.8 &&
          median !== null &&
          median <= 30,
      };
    })
    .sort((a, b) => b.incidents - a.incidents);
}
type Page = {
  data: {
    attributes: Incident & { team_name?: string };
    relationships?: Record<string, { data?: { id: string; type: string } }>;
  }[];
  pagination?: { next?: string | null };
};
// Better Stack Uptime incidents for the last 30 days, cached for 15 minutes.
export const recentIncidents = unstable_cache(
  async () => {
    const token = process.env.BETTERSTACK_API_TOKEN;
    if (!token) return null;
    const from = new Date(Date.now() - 30 * DAY).toISOString().slice(0, 10);
    let url: string | null =
      `https://uptime.betterstack.com/api/v3/incidents?from=${from}&per_page=50`;
    const incidents: Incident[] = [];
    for (let page = 0; url && page < 40; page++) {
      const body: Page = await requestJson<Page>(
        url,
        { headers: { Authorization: `Bearer ${token}` } },
        "Better Stack",
      );
      incidents.push(
        ...body.data.map(({ attributes: a, relationships }) => {
          const ref = Object.values(relationships || {}).find(
            (r) => r.data,
          )?.data;
          return {
            name: a.name,
            started_at: a.started_at,
            acknowledged_at: a.acknowledged_at,
            resolved_at: a.resolved_at,
            resolved_by: a.resolved_by,
            status: a.status,
            source: ref && `${a.team_name}|${ref.type}:${ref.id}`,
          };
        }),
      );
      url = body.pagination?.next || null;
    }
    // Partial history would understate every count.
    if (url) throw new Error("Better Stack has more than 2,000 incidents");
    return incidents;
  },
  ["alert-incidents-v3"],
  { revalidate: 900 },
);
