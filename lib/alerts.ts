import { unstable_cache } from "next/cache";
import { requestJson } from "./http";
import { DAY } from "./window";
export type Incident = {
  name: string;
  started_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
};
const HOUR = 3_600_000;
// Per-monitor noise: how often it fires, how often anyone responds, and how
// quickly it clears on its own.
export function alertStats(incidents: Incident[], now = Date.now()) {
  const groups = new Map<string, Incident[]>();
  for (const i of incidents)
    groups.set(i.name, [...(groups.get(i.name) || []), i]);
  return [...groups]
    .map(([name, list]) => {
      const starts = list.map((i) => Date.parse(i.started_at)).sort();
      const minutes = list
        .filter((i) => i.resolved_at)
        .map(
          (i) =>
            (Date.parse(i.resolved_at!) - Date.parse(i.started_at)) / 60_000,
        )
        .sort((a, b) => a - b);
      const unacknowledged = list.filter((i) => !i.acknowledged_at).length;
      const median = minutes.length
        ? minutes[Math.floor(minutes.length / 2)]
        : null;
      // Most incidents that started within any one hour.
      let peak = 0;
      for (let a = 0, b = 0; b < starts.length; b++) {
        while (starts[b] - starts[a] >= HOUR) a++;
        peak = Math.max(peak, b - a + 1);
      }
      return {
        name,
        incidents: list.length,
        unacknowledged,
        ongoing: list.filter((i) => !i.resolved_at).length,
        medianMinutes: median,
        peakHour: peak,
        last: new Date(Math.max(...starts)).toISOString(),
        // Fires often, clears by itself, and almost nobody responds.
        flapping:
          list.length >= 10 &&
          unacknowledged / list.length >= 0.8 &&
          median !== null &&
          median <= 30,
      };
    })
    .filter((s) => now - Date.parse(s.last) < 30 * DAY)
    .sort((a, b) => b.incidents - a.incidents);
}
type Page = {
  data: { attributes: Incident }[];
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
        ...body.data.map(({ attributes: a }) => ({
          name: a.name,
          started_at: a.started_at,
          acknowledged_at: a.acknowledged_at,
          resolved_at: a.resolved_at,
        })),
      );
      url = body.pagination?.next || null;
    }
    return incidents;
  },
  ["alert-incidents-v1"],
  { revalidate: 900 },
);
