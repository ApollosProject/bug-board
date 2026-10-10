import { start } from "workflow/api";
import { refresh } from "@/workflows/refresh";
import { refreshJobs, type Job } from "@/lib/types";
import { equal } from "@/lib/auth";
import { claim, release } from "@/lib/cache";
export const maxDuration = 60;
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
    token = await claim(`refresh:${job}`, 8 * 3600);
    if (!token) return Response.json({ status: "already_running" });
    const run = await start(refresh, [job as Job, token]);
    return Response.json(
      { status: "queued", runId: run.runId },
      { status: 202 },
    );
  } catch {
    if (token) await release(`refresh:${job}`, token).catch(() => {});
    return Response.json({ error: "refresh_unavailable" }, { status: 503 });
  }
}
