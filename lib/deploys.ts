import { unstable_cache } from "next/cache";
import { config } from "./config";
import { githubRest } from "./github";
import { mapConcurrent } from "./http";
import { DAY } from "./window";
const pipelines = config.deploys;
type Run = {
  head_sha: string;
  head_branch: string | null;
  status: string;
  conclusion: string | null;
  created_at: string;
  updated_at: string;
  html_url: string;
  display_title: string;
};
type Compare = {
  ahead_by: number;
  html_url: string;
  commits: { sha: string; commit: { message: string } }[];
};
export type Stage = {
  label: string;
  live: boolean;
  latest: Run | null;
  success: Run | null;
  failures: number;
};
export type Pipeline = {
  name: string;
  repo: string;
  branch: string;
  stages: Stage[];
  waiting: { count: number; url: string; titles: string[] } | null;
  error?: boolean;
};
const repoPath = (repo: string) => `/repos/ApollosProject/${repo}`;
// Latest run, last success (possibly older), and GitHub's exact count of failed
// runs in the last seven days. GitHub lists runs by creation, but a re-run of an
// older run (a rollback) finishes later, so take the most recently updated of
// the newest 100 (one request). Times shown are update times for the same reason.
export async function stage(repo: string, workflow: string, now = Date.now()) {
  const runs = (query: string, size = 100) =>
    githubRest<{ total_count: number; workflow_runs: Run[] }>(
      `${repoPath(repo)}/actions/workflows/${workflow}/runs?exclude_pull_requests=true&per_page=${size}${query}`,
    );
  const newest = ({ workflow_runs: r }: { workflow_runs: Run[] }) =>
    r.reduce<Run | null>(
      (a, b) => (!a || b.updated_at > a.updated_at ? b : a),
      null,
    );
  const since = new Date(now - 7 * DAY).toISOString().slice(0, 19) + "Z";
  const [latest, success, failed] = await Promise.all([
    runs(""),
    runs("&status=success"),
    // Only total_count is used.
    runs(`&status=failure&created=>=${since}`, 1),
  ]);
  return {
    latest: newest(latest),
    success: newest(success),
    failures: failed.total_count,
  };
}
async function pipeline(p: (typeof pipelines)[number]): Promise<Pipeline> {
  const base = { name: p.name, repo: p.repo, branch: p.branch };
  try {
    const stages: Stage[] = await mapConcurrent(
      [...p.stages],
      4,
      async ({ label, workflow, live }) => ({
        label,
        live: !!live,
        ...(await stage(p.repo, workflow)),
      }),
    );
    const live = stages.find((s) => s.live)?.success;
    const compare = live
      ? await githubRest<Compare>(
          `${repoPath(p.repo)}/compare/${live.head_sha}...${p.branch}`,
        )
      : null;
    return {
      ...base,
      stages,
      waiting: compare && {
        count: compare.ahead_by,
        url: compare.html_url,
        // Newest first; the first line of each commit message.
        titles: compare.commits
          .slice(-5)
          .reverse()
          .map((c) => c.commit.message.split("\n")[0]),
      },
    };
  } catch {
    return { ...base, stages: [], waiting: null, error: true };
  }
}
// About five small REST calls per pipeline; the REST quota is separate from the
// GraphQL budget the metrics refresh uses.
export const deploys = unstable_cache(
  async () => {
    if (!process.env.GITHUB_TOKEN) return null;
    const checked_at = new Date().toISOString();
    return {
      checked_at,
      pipelines: await mapConcurrent([...pipelines], 3, pipeline),
    };
  },
  ["deploys-v2"],
  { revalidate: 300 },
);
