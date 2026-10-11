import { start } from "workflow/api";
import { refresh } from "@/workflows/refresh";
import { refreshSnapshot } from "@/lib/refresh";
import { refreshJobs, type Job } from "@/lib/types";
import { equal } from "@/lib/auth";
import { claim, release } from "@/lib/cache";
export const maxDuration = 300;
export async function GET(
  request: Request,
  { params }: { params: Promise<{ job: string }> },
) {
  const { job } = await params;
  if (!refreshJobs.some((name) => name === job))
    return Response.json({ error: "unknown_job" }, { status: 404 });
  if (!process.env.CRON_SECRET)
    return Response.json({ error: "cron_not_configured" }, { status: 503 });
  if (
    !equal(
      request.headers.get("authorization") || "",
      `Bearer ${process.env.CRON_SECRET}`,
    )
  )
    return Response.json({ error: "unauthorized" }, { status: 401 });
  let token: string | null = null;
  try {
    const name = job as Job;
    const durable = name === "regressions" || name === "notifications";
    token = await claim(`refresh:${job}`, durable ? 8 * 3600 : maxDuration + 60);
    if (!token) return Response.json({ status: "already_running" });
    if (name !== "regressions" && name !== "notifications") {
      const result = await refreshSnapshot(name);
      await release(`refresh:${job}`, token);
      token = null;
      return Response.json({ status: "refreshed", result });
    }
    const run = await start(refresh, [name, token]);
    return Response.json(
      { status: "queued", runId: run.runId },
      { status: 202 },
    );
  } catch {
    if (token) await release(`refresh:${job}`, token).catch(() => {});
    return Response.json({ error: "refresh_unavailable" }, { status: 503 });
  }
}
