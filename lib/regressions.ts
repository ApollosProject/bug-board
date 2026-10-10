import fs from "node:fs";
import path from "node:path";
import YAML from "yaml";
import { github, githubRest, merged, prCounts } from "./github";
import { completed } from "./linear";
import { engineers, people } from "./config";
import { isPriorityBug } from "./metrics";
import { DAY, inWindow, timeWindow } from "./window";
import { readSnapshot } from "./cache";
import { mapConcurrent } from "./http";
import type { Issue, Review, Window } from "./types";
export type Candidate = {
  url: string;
  merged_at: string;
  author: string | null;
  reviewers: string[];
  line_count: number;
  score: number;
};
export type Attribution = {
  identifier: string;
  issue_url: string;
  fixing_urls: string[];
  complete: boolean;
  candidates: Candidate[];
  attribution: Candidate | null;
  manual_override?: boolean;
};
export type RegressionMetric = {
  slug: string;
  github_username: string;
  regression_count: number;
  pr_count: number;
  rate: number | null;
  pull_requests: { url: string; label: string }[];
  attributions: Attribution[];
};
export type RegressionSummary = {
  days: number;
  configured: boolean;
  complete: boolean;
  regression_count: number;
  attributed_count: number;
  authored_regression_count: number;
  approved_regression_count: number;
  author_regression_rate: number | null;
  reviewer_escape_rate: number | null;
  author_metrics: RegressionMetric[];
  reviewer_metrics: RegressionMetric[];
};
export function parsePR(url: string) {
  const match =
    /^https:\/\/github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)\/pull\/(\d+)\/?$/i.exec(
      url,
    );
  return match
    ? { owner: match[1], repo: match[2], number: Number(match[3]) }
    : null;
}
export function deletedLines(patch = "") {
  const result: number[] = [];
  let line: number | null = null;
  for (const text of patch.split("\n")) {
    if (text.startsWith("@@")) {
      const match = /^@@ -(\d+)(?:,\d+)? \+\d+(?:,\d+)? @@/.exec(text);
      line = match ? Number(match[1]) : null;
    } else if (line !== null && !text.startsWith("\\")) {
      if (text.startsWith("-")) result.push(line++);
      else if (!text.startsWith("+")) line++;
    }
  }
  return result;
}
export function fixingURLs(issue: Issue) {
  return [
    ...new Set(
      issue.attachments?.nodes
        .map((a) => a.metadata)
        .filter(
          (m) =>
            m?.status === "merged" &&
            ["closes", "contributes"].includes(m.linkKind || "") &&
            parsePR(m.url || ""),
        )
        .map((m) => m.url!.replace(/\/$/, "")) || [],
    ),
  ].sort();
}
const bot = (login: string) =>
  /\[bot\]$/i.test(login) || login.toLowerCase() === "dependabot";
export type FixFile = {
  filename: string;
  previous_filename?: string;
  status: string;
  patch?: string;
};
export type FixContext = {
  owner: string;
  repo: string;
  url: string;
  oid: string;
  mergedAt: string;
  files: FixFile[];
  complete: boolean;
};
export async function fixingContext(url: string): Promise<FixContext | null> {
  const parsed = parsePR(url);
  if (!parsed) return null;
  const { owner, repo, number } = parsed;
  const pr = await githubRest<{
    merged: boolean;
    merged_at: string;
    merge_commit_sha: string;
    changed_files: number;
  }>(`/repos/${owner}/${repo}/pulls/${number}`);
  if (!pr.merged || !pr.merged_at || !pr.merge_commit_sha) return null;
  const commit = await githubRest<{ parents: { sha: string }[] }>(
    `/repos/${owner}/${repo}/commits/${pr.merge_commit_sha}`,
  );
  if (!commit.parents[0]) return null;
  const files = (
    await githubRest<FixFile[]>(
      `/repos/${owner}/${repo}/pulls/${number}/files?per_page=100`,
    )
  ).slice(0, 50);
  return {
    owner,
    repo,
    url,
    oid: commit.parents[0].sha,
    mergedAt: pr.merged_at,
    files,
    complete: pr.changed_files <= files.length,
  };
}
export async function blameFile(
  context: Omit<FixContext, "files">,
  file: FixFile,
): Promise<{ candidates: Candidate[]; complete: boolean }> {
  const allLines = deletedLines(file.patch),
    lines = allLines.slice(0, 500);
  if (!lines.length) return { candidates: [], complete: true };
  type PR = {
    url: string;
    mergedAt: string;
    author: { login: string } | null;
    reviews: { nodes: Review[]; pageInfo: { hasNextPage: boolean } };
  };
  type Associated = {
    nodes: PR[];
    pageInfo: { hasNextPage: boolean };
  };
  type Range = {
    startingLine: number;
    endingLine: number;
    commit: { oid: string; committedDate: string };
  };
  const data = await github<{
    repository: { object: { blame: { ranges: Range[] } } };
  }>(
    `query Blame($owner: String!, $repo: String!, $oid: GitObjectID!, $path: String!) { repository(owner: $owner, name: $repo) { object(oid: $oid) { ... on Commit { blame(path: $path) { ranges { startingLine endingLine commit { oid committedDate } } } } } } }`,
    {
      owner: context.owner,
      repo: context.repo,
      oid: context.oid,
      path:
        file.status === "renamed"
          ? file.previous_filename || file.filename
          : file.filename,
    },
  );
  const ranges = data.repository?.object?.blame?.ranges || [];
  const overlap = (range: Range) =>
    lines.filter((n) => n >= range.startingLine && n <= range.endingLine).length;
  const matched = ranges.filter(overlap);
  // Fetch PR/review history only for removed lines, not every commit in the file.
  const metadata = new Map(
    await mapConcurrent(
      [...new Set(matched.map((r) => r.commit.oid))],
      4,
      async (oid): Promise<[string, Associated | null]> => {
        const result = await github<{
          repository: { object: { associatedPullRequests: Associated } | null };
        }>(
          `query BlamePRs($owner: String!, $repo: String!, $oid: GitObjectID!) { repository(owner: $owner, name: $repo) { object(oid: $oid) { ... on Commit { associatedPullRequests(first: 10) { pageInfo { hasNextPage } nodes { url mergedAt author { login } reviews(first: 100, states: [APPROVED]) { pageInfo { hasNextPage } nodes { author { login } state } } } } } } } }`,
          { owner: context.owner, repo: context.repo, oid },
        );
        return [oid, result.repository?.object?.associatedPullRequests || null];
      },
    ),
  );
  const candidates: Candidate[] = [];
  for (const range of matched) {
    const prs = (metadata.get(range.commit.oid)?.nodes || []).filter(
      (pr) => pr.mergedAt && Date.parse(pr.mergedAt) <= Date.parse(context.mergedAt),
    );
    const afterCommit = prs.filter(
      (pr) => Date.parse(pr.mergedAt) >= Date.parse(range.commit.committedDate),
    );
    const inducing = (afterCommit.length ? afterCommit : prs).sort((a, b) =>
      a.mergedAt.localeCompare(b.mergedAt),
    )[0];
    if (!inducing || inducing.url === context.url) continue;
    const author = inducing.author?.login || null;
    const reviewers = [
      ...new Set(
        inducing.reviews.nodes
          .map((r) => r.author?.login || "")
          .filter(
            (login) =>
              login && !bot(login) && login.toLowerCase() !== author?.toLowerCase(),
          ),
      ),
    ];
    const age = Math.max(
      0,
      Math.trunc(
        (Date.parse(context.mergedAt) - Date.parse(inducing.mergedAt)) / DAY,
      ),
    );
    candidates.push({
      url: inducing.url,
      merged_at: inducing.mergedAt,
      author,
      reviewers,
      line_count: overlap(range),
      score: overlap(range) / (1 + age / 30),
    });
  }
  return {
    candidates,
    complete:
      allLines.length <= 500 &&
      lines.every((n) => ranges.some((r) => n >= r.startingLine && n <= r.endingLine)) &&
      [...metadata.values()].every(
        (m) =>
          m && !m.pageInfo.hasNextPage &&
          m.nodes.every((pr) => !pr.reviews.pageInfo.hasNextPage),
      ),
  };
}
export function mergeAttribution(
  issue: Pick<Issue, "identifier" | "url">,
  fixing_urls: string[],
  results: { candidates: Candidate[]; complete: boolean }[],
): Attribution {
  const candidates = new Map<string, Candidate>();
  for (const incoming of results.flatMap((r) => r.candidates)) {
    if (fixing_urls.includes(incoming.url)) continue;
    const current = candidates.get(incoming.url);
    candidates.set(
      incoming.url,
      current
        ? {
            ...current,
            line_count: current.line_count + incoming.line_count,
            score: current.score + incoming.score,
          }
        : { ...incoming },
    );
  }
  const ranked = [...candidates.values()].sort(
    (a, b) => b.score - a.score || b.line_count - a.line_count,
  );
  return {
    identifier: issue.identifier,
    issue_url: issue.url,
    fixing_urls,
    complete: results.length > 0 && results.every((r) => r.complete),
    candidates: ranked,
    attribution: ranked[0] || null,
  };
}
export async function regressionIssues(window: Window) {
  return (await completed(window))
    .filter(isPriorityBug)
    .filter((i) => fixingURLs(i).length)
    .map((i) => ({
      identifier: i.identifier,
      url: i.url,
      fixing_urls: fixingURLs(i),
    }));
}
async function overrideMetadata(url: string): Promise<Candidate | null> {
  const parsed = parsePR(url);
  if (!parsed) return null;
  const { owner, repo, number } = parsed;
  const pr = await githubRest<{
    merged: boolean;
    merged_at: string;
    user: { login: string };
  }>(`/repos/${owner}/${repo}/pulls/${number}`);
  if (!pr.merged) return null;
  const reviews: { user: { login: string }; state: string }[] = [];
  for (let page = 1; ; page++) {
    const batch = await githubRest<typeof reviews>(
      `/repos/${owner}/${repo}/pulls/${number}/reviews?per_page=100&page=${page}`,
    );
    reviews.push(...batch);
    if (batch.length < 100) break;
  }
  return {
    url,
    merged_at: pr.merged_at,
    author: pr.user.login,
    reviewers: [
      ...new Set(
        reviews
          .filter(
            (r) =>
              r.state === "APPROVED" &&
              !bot(r.user.login) &&
              r.user.login.toLowerCase() !== pr.user.login.toLowerCase(),
          )
          .map((r) => r.user.login),
      ),
    ],
    score: 0,
    line_count: 0,
  };
}
export const rate = (count: number, total: number) =>
  total ? Math.round((count / total) * 1000) / 10 : null;
export function regressionMetrics(
  records: Attribution[],
  counts: Record<string, { prs_merged: number; prs_reviewed: number }>,
  window: Window,
  reviewing: boolean,
): RegressionMetric[] {
  return engineers.map((slug) => {
    const username = people[slug].github_username;
    const attributions = records.filter(
      (r) =>
        r.attribution &&
        inWindow(r.attribution.merged_at, window) &&
        (reviewing
          ? r.attribution.reviewers.some(
              (login) => login.toLowerCase() === username.toLowerCase(),
            )
          : r.attribution.author?.toLowerCase() === username.toLowerCase()),
    );
    const urls = [...new Set(attributions.map((a) => a.attribution!.url))];
    const total =
      counts[slug]?.[reviewing ? "prs_reviewed" : "prs_merged"] || 0;
    return {
      slug,
      github_username: username,
      regression_count: attributions.length,
      pr_count: total,
      rate: rate(attributions.length, total),
      pull_requests: urls.map((url) => ({
        url,
        label: `${parsePR(url)?.repo}#${parsePR(url)?.number}`,
      })),
      attributions,
    };
  });
}
export async function summarizeRegressions(
  records: Attribution[],
  window: Window,
): Promise<RegressionSummary> {
  const overrides =
    ((
      YAML.parse(
        fs.readFileSync(
          path.join(process.cwd(), "regression_overrides.yml"),
          "utf8",
        ),
      ) || {}
    ).overrides as Record<
      string,
      { ignored?: boolean; inducing_pr?: string }
    >) || {};
  const corrected: Attribution[] = [];
  for (const record of records) {
    const override = overrides[record.identifier];
    if (override?.ignored) continue;
    if (override?.inducing_pr) {
      const candidate =
        record.candidates.find((c) => c.url === override.inducing_pr) ||
        (await overrideMetadata(override.inducing_pr).catch(() => null));
      if (candidate) {
        record.attribution = candidate;
        record.manual_override = true;
      } else record.complete = false;
    }
    corrected.push(record);
  }
  const prs = await merged(window),
    counts = Object.fromEntries(
      engineers.map((slug) => [
        slug,
        prCounts(prs, people[slug].github_username),
      ]),
    );
  const author_metrics = regressionMetrics(corrected, counts, window, false),
    reviewer_metrics = regressionMetrics(corrected, counts, window, true);
  const sum = (
    rows: RegressionMetric[],
    key: "pr_count" | "regression_count",
  ) => rows.reduce((n, r) => n + r[key], 0);
  return {
    days: window.days,
    configured: true,
    complete: corrected.every((r) => r.complete),
    regression_count: corrected.length,
    attributed_count: corrected.filter((r) => r.attribution).length,
    authored_regression_count: sum(author_metrics, "regression_count"),
    approved_regression_count: sum(reviewer_metrics, "regression_count"),
    author_regression_rate: rate(
      sum(author_metrics, "regression_count"),
      sum(author_metrics, "pr_count"),
    ),
    reviewer_escape_rate: rate(
      sum(reviewer_metrics, "regression_count"),
      sum(reviewer_metrics, "pr_count"),
    ),
    author_metrics,
    reviewer_metrics,
  };
}
export const regressionDashboard = () =>
  readSnapshot<RegressionSummary>("regressions", 86400);
export const regressionWindow = () => timeWindow({ days: "30" });
