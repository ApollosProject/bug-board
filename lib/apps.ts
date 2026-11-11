import { BigQuery } from "@google-cloud/bigquery";
import fs from "node:fs";
import path from "node:path";
import {
  githubRest,
  releaseTags,
  rokuTagPattern,
  stableTagPattern,
} from "./github";
import { mapConcurrent } from "./http";
import {
  compareVersions,
  mobileReleases,
  storePlatforms,
  type StoreRelease,
} from "./stores";
import type { AppRow } from "./types";
import { readSnapshot } from "./cache";
export type Apps = {
  rows: AppRow[];
  checked_at: string;
  lookback_days: number;
};
const fieldCandidates: Record<string, string[]> = {
  church: ["church", "group_id", "groupId"],
  build_church: ["build_church", "buildChurch"],
  apollos_platform: ["apollos_platform", "apollosPlatform", "apollosplatform"],
  apollos_version: ["apollos_version", "apollosVersion"],
  app_version: ["app_version", "appVersion"],
  native_build: ["context_app_build"],
  native_version: ["context_app_version"],
  app_update_id: ["app_update_id", "appUpdateId"],
  bundle_id: ["bundle_id", "bundleId"],
  application_name: ["application_name", "applicationName"],
  source_revision: ["source_revision", "sourceRevision"],
  source_version: ["source_version", "sourceVersion"],
  deployment_track: ["deployment_track", "deploymentTrack"],
};
// Native TV apps ship from stable tags; Amazon builds from the Android TV template.
const tvTemplates: Record<string, string> = {
  tvos: "tvos",
  androidtv: "androidtv",
  amazon: "androidtv",
};
const runtimePattern = /^\d+(?:\.\d+)*$/;
const internalTracks = new Set([
  "beta",
  "development",
  "internal",
  "preview",
  "prerelease",
]);
const sha = /^[0-9a-f]{7,40}$/i;
export const revisionsMatch = (a: string, b: string) =>
  sha.test(a) &&
  sha.test(b) &&
  (a.toLowerCase().startsWith(b.toLowerCase()) ||
    b.toLowerCase().startsWith(a.toLowerCase()));
export const appIdentity = (row: AppRow) =>
  `${["unknown", "roku"].includes(row.bundle_id.toLowerCase()) ? row.church : ""}|${row.apollos_platform}|${row.bundle_id.toLowerCase()}`;
const envList = (name: string, defaults: string[]) =>
  process.env[name]
    ?.split(",")
    .map((s) => s.trim())
    .filter(Boolean) || defaults;
const positiveIntEnv = (name: string, fallback: number) => {
  const value = Number(process.env[name]);
  return Number.isSafeInteger(value) && value > 0 ? value : fallback;
};
const identifier = (s: string) => {
  if (!/^[A-Za-z0-9_-]+$/.test(s))
    throw new Error("Invalid BigQuery identifier");
  return s;
};
export async function appObservations(): Promise<AppRow[]> {
  const encoded = process.env.BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64;
  if (!encoded)
    throw new Error(
      "Configure BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64 for app analytics",
    );
  const credentials = JSON.parse(Buffer.from(encoded, "base64").toString());
  const project = identifier(
    process.env.BIGQUERY_ANALYTICS_PROJECT_ID || "apollos-project",
  );
  const datasets = envList("BIGQUERY_ANALYTICS_DATASETS", [
    "apollos",
    "apollos_tv",
    "apollos_roku",
  ]).map(identifier);
  const tables = envList("BIGQUERY_ANALYTICS_TABLES", [
    "identifies",
    "screens",
    "app_became_active",
    "app_became_backgrounded",
    "app_became_inactive",
  ]).map(identifier);
  const client = new BigQuery({ projectId: project, credentials });
  const [schema] = await client.query({
    query: datasets
      .map(
        (dataset) =>
          `SELECT '${dataset}' AS dataset_name, table_name, column_name FROM \`${project}.${dataset}.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name IN UNNEST(@tables)`,
      )
      .join(" UNION ALL "),
    params: { tables },
  });
  const groups = new Map<string, Map<string, string>>();
  for (const row of schema as {
    dataset_name: string;
    table_name: string;
    column_name: string;
  }[]) {
    const key = `${row.dataset_name}.${row.table_name}`,
      columns = groups.get(key) || new Map<string, string>();
    columns.set(row.column_name.toLowerCase(), identifier(row.column_name));
    groups.set(key, columns);
  }
  const selects: string[] = [];
  let usesPartitionPruning = false;
  for (const [name, columns] of groups) {
    const [dataset, table] = name.split(".");
    const choose = (candidates: string[]) =>
      candidates.map((c) => columns.get(c.toLowerCase())).find(Boolean);
    const timestamp = choose([
      "timestamp",
      "received_at",
      "sent_at",
      "original_timestamp",
      "loaded_at",
    ]);
    const version =
      choose(fieldCandidates.apollos_version) ||
      (dataset === "apollos_roku" ? choose(["context_library_version"]) : null);
    if (!timestamp || !version) continue;
    const partition = choose(["_PARTITIONTIME"]);
    if (partition) usesPartitionPruning = true;
    // Segment ingestion partitions need their own predicate; event time alone cannot prune them.
    const partitionFilter = partition
      ? ` AND \`${partition}\` >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @partition_lookback_days DAY), DAY)`
      : "";
    const fields = Object.entries(fieldCandidates)
      .filter(([field]) => field !== "apollos_version")
      .map(([field, candidates]) => {
        const column = choose(candidates);
        return `${column ? `NULLIF(CAST(\`${column}\` AS STRING), '')` : "CAST(NULL AS STRING)"} AS ${field}`;
      });
    selects.push(
      `SELECT CAST(\`${timestamp}\` AS TIMESTAMP) AS seen_at, NULLIF(CAST(\`${version}\` AS STRING), '') AS apollos_version, ${fields.join(", ")}, '${table}' AS source_table, '${choose(fieldCandidates.apollos_version) ? "runtime" : "analytics_library"}' AS version_source, '${dataset}' AS source_dataset FROM \`${project}.${dataset}.${table}\` WHERE \`${timestamp}\` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)${partitionFilter}`,
    );
  }
  if (!selects.length)
    throw new Error("No compatible Segment analytics tables");
  const query = fs
    .readFileSync(path.join(process.cwd(), "lib/app-versions.sql"), "utf8")
    .replace("__SELECTS__", selects.join(" UNION ALL "));
  const params: Record<string, number> = { lookback_days: lookbackDays() };
  if (usesPartitionPruning)
    params.partition_lookback_days =
      params.lookback_days +
      positiveIntEnv("APP_VERSIONS_PARTITION_BUFFER_DAYS", 3);
  const [rows] = await client.query({ query, params });
  return (
    rows as (Omit<AppRow, "latest_seen_at"> & {
      latest_seen_at?: { value: string } | string;
    })[]
  ).map((row) => ({
    ...row,
    latest_seen_at:
      typeof row.latest_seen_at === "object"
        ? row.latest_seen_at.value
        : row.latest_seen_at,
  }));
}
export const lookbackDays = () =>
  Math.min(positiveIntEnv("APP_VERSIONS_LOOKBACK_DAYS", 30), 366);
export type Source = {
  revisions: Record<string, string>;
  mobile: string | null;
  // Template → oldest stable tag that already has the newest tag's code.
  tv: Record<string, string>;
  roku: string | null;
  roku_tag: string | null;
};
const platformsRepo = "/repos/ApollosProject/apollos-platforms";
async function mobileRuntime(ref: string) {
  const file = await githubRest<{ encoding: string; content: string }>(
    `${platformsRepo}/contents/templates/mobile/app.config.ts?ref=${ref}`,
  );
  return file.encoding === "base64"
    ? /^\s*runtimeVersion:\s*['"](\d+)['"]/m.exec(
        Buffer.from(file.content, "base64").toString(),
      )?.[1] || null
    : null;
}
// Releases without TV changes (most hotfixes) carry the same TV code, so an app
// on any tag from the first one with today's code onward is up to date.
// shortcut: walks the newest 20 tags; code unchanged for longer reports the 20th.
export async function tvReleases(tags: string[]) {
  const newest: Record<string, string | undefined> = {},
    first: Record<string, string> = {};
  let open = ["tvos", "androidtv"];
  for (const tag of tags.slice(0, 20)) {
    if (!open.length) break;
    const entries = await githubRest<{ name: string; sha: string }[]>(
      `${platformsRepo}/contents/templates?ref=${tag}`,
    );
    open = open.filter((name) => {
      const tree = entries.find((entry) => entry.name === name)?.sha;
      if (!(name in newest)) newest[name] = tree;
      if (!tree || tree !== newest[name]) return false;
      first[name] = tag;
      return true;
    });
  }
  return first;
}
export async function appSource(): Promise<Source> {
  const tags = await releaseTags().catch(() => []);
  const stable = tags
      .filter((tag) => stableTagPattern.test(tag.name))
      .map((tag) => tag.name),
    rokuTag = tags.find((tag) => rokuTagPattern.test(tag.name))?.name || null;
  const [mobile, tv, commits] = await Promise.all([
    stable[0] ? mobileRuntime(stable[0]).catch(() => null) : null,
    tvReleases(stable).catch(() => ({})),
    // The Roku release's last app-code change; builds containing it are current.
    rokuTag
      ? githubRest<{ sha: string }[]>(
          `${platformsRepo}/commits?sha=${rokuTag}&path=templates/roku/src&per_page=1`,
        ).catch(() => [])
      : [],
  ]);
  return {
    revisions: Object.fromEntries(
      tags.map((tag) => [tag.name, tag.commit.sha]),
    ),
    mobile,
    tv,
    roku: commits[0]?.sha || null,
    roku_tag: rokuTag,
  };
}
export function selectMobile(
  observations: AppRow[],
  release?: StoreRelease,
): AppRow {
  const selected: AppRow = {
    ...observations
      .slice()
      .sort((a, b) =>
        (b.latest_seen_at || "").localeCompare(a.latest_seen_at || ""),
      )[0],
    apollos_version: null,
    live_status_detail:
      release?.live_status_detail || "Store release could not be verified",
  };
  if (release && "deploy_target_count" in release) {
    selected.build_church = release.build_church;
    selected.deploy_target_count = release.deploy_target_count;
  }
  if (release?.builds?.length === 0)
    selected.live_status_detail = "No published store build";
  if (!release?.builds?.length) return selected;
  const runtimes = new Set<string>();
  for (const build of release.builds) {
    const matches = new Set(
      observations
        .filter(
          (row) =>
            row.native_build === build.native_build &&
            (!row.app_version ||
              !row.native_version ||
              row.app_version === row.native_version) &&
            (row.apollos_platform !== "ios" ||
              row.native_version === build.native_version),
        )
        .map((row) => row.apollos_version),
    );
    const runtime = [...matches][0];
    if (matches.size !== 1 || !runtime || !runtimePattern.test(runtime)) {
      selected.live_status_detail = "Live build has no unique runtime match";
      return selected;
    }
    runtimes.add(runtime);
  }
  selected.live_runtime_display = [...runtimes]
    .sort(compareVersions)
    .join(", ");
  selected.apollos_version = runtimes.size === 1 ? [...runtimes][0] : null;
  selected.live_status_detail =
    "Published store build matched by native build ID";
  return selected;
}
export function selectObserved(rows: AppRow[], source: Source) {
  const selected = new Map<string, AppRow>();
  // Other platforms (unknown, legacy `tv`, web) have no release to compare.
  for (const row of rows.filter(
    (r) =>
      Object.hasOwn(tvTemplates, r.apollos_platform) ||
      r.apollos_platform === "roku",
  )) {
    const version = row.source_version || "",
      alpha = /^(v\d{4}\.\d{2}\.\d{2}\.\d{2})-alpha\.\d+$/.exec(version);
    const canonical = stableTagPattern.test(version)
      ? version
      : alpha &&
          revisionsMatch(
            row.source_revision || "",
            source.revisions[alpha[1]] || "",
          )
        ? alpha[1]
        : null;
    if (
      internalTracks.has((row.deployment_track || "").toLowerCase()) ||
      version.toLowerCase() === "master" ||
      (alpha && !canonical)
    )
      continue;
    const candidate = {
      ...row,
      canonical_source_version: canonical,
      apollos_version:
        row.apollos_platform === "roku"
          ? row.source_version || null
          : canonical,
    };
    const key = appIdentity(candidate),
      current = selected.get(key);
    if (
      !current ||
      compareVersions(
        candidate.apollos_version || "",
        current.apollos_version || "",
      ) > 0 ||
      (candidate.apollos_version === current.apollos_version &&
        (compareVersions(
          candidate.app_version || "",
          current.app_version || "",
        ) > 0 ||
          (candidate.latest_seen_at || "") > (current.latest_seen_at || "")))
    )
      selected.set(key, candidate);
  }
  return [...selected.values()];
}
export async function annotateApps(rows: AppRow[], source: Source) {
  return mapConcurrent(rows, 8, async (row) => {
    const platform = row.apollos_platform;
    let live = row.apollos_version,
      latest: string | null | undefined,
      cmp: number | null = null,
      detail = row.live_status_detail;
    if (storePlatforms.has(platform)) {
      latest = source.mobile;
      live = row.live_runtime_display || live;
      if (row.apollos_version && runtimePattern.test(row.apollos_version))
        cmp = latest ? compareVersions(row.apollos_version, latest) : null;
      else if (row.live_runtime_display) detail = "Multiple live runtimes";
    } else if (platform === "roku") {
      latest = source.roku_tag;
      live = row.source_revision?.slice(0, 7) || null;
      if (source.roku && row.source_revision && sha.test(row.source_revision)) {
        const status = revisionsMatch(source.roku, row.source_revision)
          ? "identical"
          : (
              await githubRest<{ status: string }>(
                `${platformsRepo}/compare/${source.roku}...${row.source_revision}`,
              ).catch(() => null)
            )?.status;
        // Diverged builds also lack the release's last Roku change.
        cmp = ["behind", "diverged"].includes(status || "")
          ? -1
          : ["identical", "ahead"].includes(status || "")
            ? 0
            : null;
      }
    } else {
      latest = source.tv[tvTemplates[platform]];
      live = row.canonical_source_version || row.source_version || null;
      if (!row.canonical_source_version) detail = "Not a release build";
      else if (latest)
        cmp = Math.min(
          compareVersions(row.canonical_source_version, latest),
          0,
        );
    }
    return {
      ...row,
      version_status_label:
        cmp === null
          ? "Unknown"
          : cmp < 0
            ? "Behind"
            : cmp > 0
              ? "Ahead"
              : "Up to date",
      freshness_display: live || "Unknown",
      comparison_display: latest || "Unknown",
      is_outdated: cmp !== null && cmp < 0,
      live_status_detail:
        cmp !== null
          ? undefined
          : !latest
            ? "Latest release unavailable"
            : detail || "Comparison unavailable",
    };
  });
}
export async function buildApps(
  rows: AppRow[],
  source: Source,
  releases: Map<string, StoreRelease>,
): Promise<Apps> {
  const groups = new Map<string, AppRow[]>();
  for (const row of rows.filter((r) => storePlatforms.has(r.apollos_platform)))
    groups.set(appIdentity(row), [
      ...(groups.get(appIdentity(row)) || []),
      row,
    ]);
  const selected = [
    ...selectObserved(rows, source),
    ...[...groups.values()].map((observations) =>
      selectMobile(
        observations,
        releases.get(
          `${observations[0].apollos_platform}:${observations[0].bundle_id.toLowerCase()}`,
        ),
      ),
    ),
  ];
  const annotated = (await annotateApps(selected, source)).sort(
    (a, b) =>
      Number(b.is_outdated) - Number(a.is_outdated) ||
      Number(b.version_status_label === "Unknown") -
        Number(a.version_status_label === "Unknown") ||
      a.apollos_platform.localeCompare(b.apollos_platform) ||
      a.church.localeCompare(b.church),
  );
  return {
    rows: annotated.slice(0, Number(process.env.APP_VERSIONS_LIMIT) || 1000),
    checked_at: new Date().toISOString(),
    lookback_days: lookbackDays(),
  };
}
export async function computeApps() {
  const [rows, source] = await Promise.all([appObservations(), appSource()]);
  return buildApps(rows, source, await mobileReleases(rows));
}
export async function appsDashboard() {
  return (
    (await readSnapshot<Apps>("apps", 300)) ||
    (process.env.NODE_ENV === "development" &&
    process.env.BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64
      ? await computeApps()
      : null)
  );
}
export function deployTarget(
  rows: AppRow[],
  platform: string,
  bundle: string,
  church: string,
) {
  const matches = rows.filter(
    (r) =>
      r.apollos_platform === platform &&
      r.bundle_id === bundle &&
      (r.build_church || r.church) === church,
  );
  return matches.length === 1 &&
    matches[0].deploy_target_count === 1 &&
    ["ios", "android", "tvos", "androidtv", "amazon", "roku"].includes(
      platform,
    ) &&
    /^[A-Za-z0-9_-]+$/.test(church)
    ? matches[0]
    : null;
}
export async function dispatchDeploy(church: string, platform: string) {
  const token = process.env.GITHUB_ACTIONS_TOKEN;
  if (!token) throw new Error("Deployment token not configured");
  // The same release the Apps page compares against.
  const pattern = platform === "roku" ? rokuTagPattern : stableTagPattern;
  const tag = (await releaseTags(token)).find((t) =>
    pattern.test(t.name),
  )?.name;
  if (!tag) throw new Error("No release tag found");
  const { githubHeaders } = await import("./github");
  // requestJson cannot consume GitHub's successful empty 204 body.
  const response = await fetch(
    `https://api.github.com/repos/ApollosProject/apollos-platforms/actions/workflows/${encodeURIComponent(process.env.GITHUB_DEPLOY_WORKFLOW_ID || "173574865")}/dispatches`,
    {
      method: "POST",
      headers: { ...githubHeaders(token), "Content-Type": "application/json" },
      body: JSON.stringify({
        ref: tag,
        inputs: { church, platform, track: "production" },
      }),
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    },
  );
  if (response.status !== 204)
    throw new Error("GitHub did not confirm deployment dispatch");
}
