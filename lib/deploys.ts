import { unstable_cache } from "next/cache";
import { githubRest } from "./github";
import { mapConcurrent } from "./http";
import { DAY } from "./window";
// Release pipelines, in promotion order. Confidence gates are reusable workflows
// that run inside the staging publish, so they have no runs of their own. The stage marked `live` is what users run;
// commits on the default branch after its last success are still waiting.
const pipelines = [
  {
    name: "Cluster",
    repo: "apollos-cluster",
    branch: "master",
    stages: [
      ["Staging", "publish-staging-image.yml"],
      ["Release tag", "release.yml"],
      ["Production", "publish-production-image.yml", "live"],
    ],
  },
  {
    name: "Admin",
    repo: "apollos-admin",
    branch: "main",
    stages: [
      ["Staging", "admin-publish-staging.yml"],
      ["Release tag", "create-admin-release-tag.yml"],
      ["Production", "admin-publish-production.yml", "live"],
    ],
  },
  {
    name: "Web",
    repo: "apollos-admin",
    branch: "main",
    stages: [
      ["Staging", "web-publish-staging.yml"],
      ["Release tag", "create-web-release-tag.yml"],
      ["Production", "web-publish-production.yml", "live"],
    ],
  },
  {
    name: "Web Embeds",
    repo: "apollos-admin",
    branch: "main",
    stages: [["Production", "publish.yml", "live"]],
  },
  {
    name: "Mobile",
    repo: "apollos-platforms",
    branch: "master",
    stages: [
      ["Weekly pre-release", "pre-release.yml"],
      ["Weekly release", "release.yml", "live"],
      ["Deploy apps", "deploy-apps.yml"],
    ],
  },
  {
    name: "Shovel",
    repo: "apollos-shovel",
    branch: "master",
    stages: [["Production", "deploy.yml", "live"]],
  },
  {
    name: "Infrastructure",
    repo: "apollos-infrastructure",
    branch: "main",
    stages: [
      ["Deploy", "deploy-infrastructure.yml", "live"],
      ["Review apps", "review-app.yml"],
    ],
  },
] as const;
type Run = {
  head_sha: string;
  head_branch: string | null;
  status: string;
  conclusion: string | null;
  created_at: string;
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
export function summarize(runs: Run[], now = Date.now()) {
  return {
    latest: runs[0] || null,
    success: runs.find((r) => r.conclusion === "success") || null,
    failures: runs.filter(
      (r) =>
        r.conclusion === "failure" && now - Date.parse(r.created_at) < 7 * DAY,
    ).length,
  };
}
async function pipeline(p: (typeof pipelines)[number]): Promise<Pipeline> {
  const base = { name: p.name, repo: p.repo, branch: p.branch };
  try {
    const stages: Stage[] = await mapConcurrent(
      [...p.stages],
      4,
      async ([label, workflow, live]) => ({
        label,
        live: live === "live",
        ...summarize(
          (
            await githubRest<{ workflow_runs: Run[] }>(
              `${repoPath(p.repo)}/actions/workflows/${workflow}/runs?per_page=20&exclude_pull_requests=true`,
            )
          ).workflow_runs,
        ),
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
    return mapConcurrent([...pipelines], 3, pipeline);
  },
  ["deploys-v1"],
  { revalidate: 300 },
);
