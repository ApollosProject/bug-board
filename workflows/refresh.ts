import { release, writeSnapshot } from "@/lib/cache";
import {
  blameFile,
  fixingContext,
  mergeAttribution,
  regressionIssues,
  summarizeRegressions,
  type Attribution,
  type Candidate,
  type FixContext,
  type FixFile,
} from "@/lib/regressions";
import { notify, scheduledNotifications } from "@/lib/notifications";
import { timeWindow, date } from "@/lib/window";
import type { Job, Window } from "@/lib/types";
async function windowStep(days = 30) {
  "use step";
  return timeWindow({ days: String(days) });
}
async function unlockStep(job: Job, token: string) {
  "use step";
  await release(`refresh:${job}`, token);
}
async function regressionIssuesStep(window: Window) {
  "use step";
  return regressionIssues(window);
}
async function contextStep(url: string) {
  "use step";
  return fixingContext(url);
}
async function blameStep(context: Omit<FixContext, "files">, file: FixFile) {
  "use step";
  return blameFile(context, file);
}
async function mergeStep(
  issue: { identifier: string; url: string },
  urls: string[],
  results: { candidates: Candidate[]; complete: boolean }[],
) {
  "use step";
  return mergeAttribution(issue, urls, results);
}
async function regressionSummaryStep(records: Attribution[], window: Window) {
  "use step";
  const summary = await summarizeRegressions(records, window);
  await writeSnapshot("regressions", summary, 86400);
  return { regressions: summary.regression_count, complete: summary.complete };
}
async function scheduleStep() {
  "use step";
  return {
    names:
      process.env.VERCEL_ENV === "production" ? scheduledNotifications() : [],
    day: date(Date.now()),
  };
}
async function notifyStep(name: string, day: string) {
  "use step";
  return notify(name, day);
}
export async function refresh(
  job: Extract<Job, "regressions" | "notifications">,
  token: string,
) {
  "use workflow";
  try {
    if (job === "regressions") {
      const window = await windowStep(),
        issues = await regressionIssuesStep(window),
        results = new Map<
          string,
          { candidates: Candidate[]; complete: boolean }
        >();
      const urls = [...new Set(issues.flatMap((issue) => issue.fixing_urls))];
      for (const url of urls) {
        const context = await contextStep(url);
        if (!context) {
          results.set(url, { candidates: [], complete: false });
          continue;
        }
        const { files, ...metadata } = context;
        const evidence: { candidates: Candidate[]; complete: boolean }[] = [];
        for (let i = 0; i < files.length; i += 4)
          evidence.push(
            ...(await Promise.all(
              files.slice(i, i + 4).map((file) => blameStep(metadata, file)),
            )),
          );
        results.set(url, {
          candidates: evidence.flatMap((f) => f.candidates),
          complete: context.complete && evidence.every((f) => f.complete),
        });
      }
      const records: Attribution[] = [];
      for (const issue of issues)
        records.push(
          await mergeStep(
            issue,
            issue.fixing_urls,
            issue.fixing_urls.map((url) => results.get(url)!),
          ),
        );
      return await regressionSummaryStep(records, window);
    }
    const schedule = await scheduleStep();
    for (const name of schedule.names) await notifyStep(name, schedule.day);
    return { notifications: schedule.names.length };
  } finally {
    await unlockStep(job, token);
  }
}
