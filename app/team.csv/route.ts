import { csv } from "@/lib/metrics";
import { reportFor, sortedTeam } from "@/lib/reports";
import { timeWindow } from "@/lib/window";
export const maxDuration = 60;
export async function GET(request: Request) {
  const query = Object.fromEntries(new URL(request.url).searchParams);
  try {
    const window = timeWindow(query),
      report = await reportFor(window);
    return new Response(csv(sortedTeam(report.rows, query)), {
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Cache-Control": "no-store",
        "Content-Disposition": `attachment; filename="team-metrics-${window.preset_days ? window.label : window.start + "-to-" + window.end}.csv"`,
      },
    });
  } catch {
    return new Response("Team metrics are unavailable or refreshing.\n", {
      status: 503,
    });
  }
}
