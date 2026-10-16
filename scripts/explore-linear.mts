// Nightly `e2e explore` findings to Linear Triage. Modes:
//   known    print earlier finding titles for the explorer to skip
//   preview  list what would be filed, without writing
//   file     create one unassigned, Low-priority Bug in Triage per new finding
// Low priority keeps findings out of priority-bug metrics and Slack digests until someone raises it.
import fs from "node:fs";
import path from "node:path";
import { config } from "../lib/config";
import { graphql } from "../lib/http";

const mode = process.argv[2], label = "e2e explore", root = ".e2e/explore", prefix = "Bug Board: ";
const linear = <T>(query: string, variables: object = {}) =>
  graphql<T>("https://api.linear.app/graphql", process.env.LINEAR_API_KEY || "", query, variables);
type Finding = { kind: string; title: string; severity: number; expected: string; actual: string; reproduction: string[]; path?: string; artifactId?: string };
type Artifact = { id: string; path: string };

async function knownTitles() {
  if (!process.env.LINEAR_API_KEY && mode !== "file") return new Set<string>();
  const { issues } = await linear<{ issues: { nodes: { title: string }[] } }>(
    `query Known($key: String!, $label: String!) { issues(first: 100, orderBy: createdAt, filter: {
      team: { key: { eq: $key } }, labels: { some: { name: { eq: $label } } } }) { nodes { title } } }`,
    { key: config.linear_team_key, label },
  );
  return new Set(issues.nodes.map((issue) => issue.title.replace(prefix, "")));
}
async function team() {
  const labels = [label, "Bug"];
  const { teams } = await linear<{ teams: { nodes: { id: string; labels: { nodes: { id: string }[] }; states: { nodes: { id: string }[] } }[] } }>(
    `query Team($key: String!, $labels: [String!]!) { teams(filter: { key: { eq: $key } }) { nodes { id
      labels(filter: { name: { in: $labels } }) { nodes { id } }
      states(filter: { type: { eq: "triage" } }) { nodes { id } } } } }`,
    { key: config.linear_team_key, labels },
  );
  const found = teams.nodes[0];
  if (!found) throw new Error(`Linear team ${config.linear_team_key} not found`);
  // Labels are team configuration, not something a nightly job should create.
  if (found.labels.nodes.length !== labels.length) throw new Error(`Linear team needs the ${labels.join(" and ")} labels`);
  return { teamId: found.id, stateId: found.states.nodes[0]?.id, labelIds: found.labels.nodes.map((l) => l.id) };
}
// Screenshots are of fixture data. A failed upload files the issue without one.
async function upload(file: string) {
  try {
    const bytes = fs.readFileSync(file);
    const { fileUpload } = await linear<{ fileUpload: { uploadFile: { uploadUrl: string; assetUrl: string; headers: { key: string; value: string }[] } } }>(
      `mutation Upload($size: Int!, $name: String!) { fileUpload(contentType: "image/png", filename: $name, size: $size) {
        uploadFile { uploadUrl assetUrl headers { key value } } } }`,
      { size: bytes.length, name: path.basename(file) },
    );
    const { uploadUrl, assetUrl, headers } = fileUpload.uploadFile;
    const response = await fetch(uploadUrl, {
      method: "PUT", body: bytes,
      headers: { "Content-Type": "image/png", "Cache-Control": "public, max-age=31536000", ...Object.fromEntries(headers.map((h) => [h.key, h.value])) },
    });
    return response.ok ? assetUrl : null;
  } catch {
    return null;
  }
}

const known = await knownTitles();
if (mode === "known") {
  console.log([...known].join("; ") || "none");
  process.exit(0);
}
if (mode !== "preview" && mode !== "file") throw new Error("Usage: explore-linear.mts known|preview|file");
const target = mode === "file" ? await team() : null;
const run = `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`;
for (const dir of fs.existsSync(root) ? fs.readdirSync(root) : []) {
  const report = path.join(root, dir, "report.json");
  if (!fs.existsSync(report)) continue;
  const result = JSON.parse(fs.readFileSync(report, "utf8")).run;
  const artifacts: Artifact[] = result.results.flatMap((r: { attempts: { artifacts: Artifact[] }[] }) => r.attempts.flatMap((a) => a.artifacts));
  for (const finding of (result.explore?.findings || []) as Finding[]) {
    if (finding.kind !== "issue" || known.has(finding.title)) continue;
    if (!target) {
      console.log(`- Would file: ${finding.title}`);
      continue;
    }
    const shot = artifacts.find((a) => a.id === finding.artifactId);
    const image = shot && (await upload(path.join(root, dir, "results", shot.path)));
    const description = [
      `Unverified: reported by the nightly \`e2e explore\` run against fake fixture data at ${process.env.GITHUB_SHA || "a local build"}. Confirm it reproduces before fixing; raise the priority if it matters, or cancel it as a false positive (canceled titles are not reported again).`,
      `**Path:** \`${finding.path || "unknown"}\` · **Severity:** ${finding.severity}/5 · [Run, trace, and video](${run})`,
      `**Expected:** ${finding.expected}`,
      `**Actual:** ${finding.actual}`,
      `**Steps**\n${finding.reproduction.map((step, i) => `${i + 1}. ${step}`).join("\n")}`,
      ...(image ? [`![Screenshot](${image})`] : []),
    ].join("\n\n");
    const { issueCreate } = await linear<{ issueCreate: { issue: { url: string } } }>(
      `mutation File($input: IssueCreateInput!) { issueCreate(input: $input) { issue { url } } }`,
      { input: { ...target, title: `${prefix}${finding.title}`, description, priority: 4, assigneeId: null } },
    );
    console.log(`- ${issueCreate.issue.url}`);
  }
}
