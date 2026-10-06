import { BigQuery } from "@google-cloud/bigquery";
import fs from "node:fs";
import path from "node:path";
import { githubRest, stableTags, stableTagPattern } from "./github";
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
const releasePlatforms = new Set(["amazon", "tv", "tvos"]);
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
    const fields = Object.entries(fieldCandidates)
      .filter(([field]) => field !== "apollos_version")
      .map(([field, candidates]) => {
        const column = choose(candidates);
        return `${column ? `NULLIF(CAST(\`${column}\` AS STRING), '')` : "CAST(NULL AS STRING)"} AS ${field}`;
      });
    selects.push(
      `SELECT CAST(\`${timestamp}\` AS TIMESTAMP) AS seen_at, NULLIF(CAST(\`${version}\` AS STRING), '') AS apollos_version, ${fields.join(", ")}, '${table}' AS source_table, '${choose(fieldCandidates.apollos_version) ? "runtime" : "analytics_library"}' AS version_source, '${dataset}' AS source_dataset FROM \`${project}.${dataset}.${table}\` WHERE \`${timestamp}\` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)`,
    );
  }
  if (!selects.length)
    throw new Error("No compatible Segment analytics tables");
  const query = fs
    .readFileSync(path.join(process.cwd(), "lib/app-versions.sql"), "utf8")
    .replace("__SELECTS__", selects.join(" UNION ALL "));
  const [rows] = await client.query({
    query,
    params: { lookback_days: lookbackDays() },
  });
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
  Math.min(
    Math.max(Number(process.env.APP_VERSIONS_LOOKBACK_DAYS) || 30, 1),
    366,
  );
export type Source = {
  revisions: Record<string, string>;
  mobile: string | null;
  tv: string | null;
  roku: string | null;
};
export async function appSource(): Promise<Source> {
  const tags = await stableTags().catch(() => []);
  const ref = tags[0]?.name;
  const runtimes = await Promise.all(
    ["mobile", "tv"].map(async (template) => {
      if (!ref) return null;
      try {
        const file = await githubRest<{ encoding: string; content: string }>(
          `/repos/ApollosProject/apollos-platforms/contents/templates/${template}/app.config.ts?ref=${ref}`,
        );
        return file.encoding === "base64"
          ? /^\s*runtimeVersion:\s*['"](\d+)['"]/m.exec(
              Buffer.from(file.content, "base64").toString(),
            )?.[1] || null
          : null;
      } catch {
        return null;
      }
    }),
  );
  const commits = await githubRest<{ sha: string }[]>(
    "/repos/ApollosProject/apollos-platforms/commits?sha=master&path=templates/roku&per_page=1",
  ).catch(() => []);
  return {
    revisions: Object.fromEntries(
      tags.map((tag) => [tag.name, tag.commit.sha]),
    ),
    mobile: runtimes[0],
    tv: runtimes[1],
    roku: commits[0]?.sha || null,
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
  for (const row of rows.filter(
    (r) => !storePlatforms.has(r.apollos_platform),
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
      apollos_version: releasePlatforms.has(row.apollos_platform)
        ? canonical
        : row.apollos_platform === "roku"
          ? row.source_version || null
          : row.apollos_version,
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
  const latest = new Map<string, string>();
  for (const row of rows)
    if (
      releasePlatforms.has(row.apollos_platform) &&
      row.canonical_source_version &&
      compareVersions(
        row.canonical_source_version,
        latest.get(row.apollos_platform) || "",
      ) > 0
    )
      latest.set(row.apollos_platform, row.canonical_source_version);
  return mapConcurrent(rows, 8, async (row) => {
    const platform = row.apollos_platform,
      store = storePlatforms.has(platform);
    let label = "Unverified",
      display = row.live_runtime_display || row.apollos_version || "Unknown",
      target = store
        ? platform === "androidtv"
          ? source.tv
          : source.mobile
        : latest.get(platform);
    let outdated = false;
    if (
      store &&
      row.apollos_version &&
      runtimePattern.test(row.apollos_version) &&
      target
    ) {
      const cmp = compareVersions(row.apollos_version, target);
      outdated = cmp < 0;
      label =
        cmp < 0
          ? "Behind release"
          : cmp > 0
            ? "Ahead of release"
            : "At release";
    } else if (store && row.live_runtime_display && !row.apollos_version)
      label = "Multiple live runtimes";
    else if (releasePlatforms.has(platform)) {
      display = row.canonical_source_version || row.source_version || "TBD";
      if (row.canonical_source_version && target) {
        outdated = compareVersions(row.canonical_source_version, target) < 0;
        label = outdated ? "Observed: Behind top seen" : "Observed: Top seen";
      }
    } else if (platform === "roku") {
      display = row.source_revision?.slice(0, 7) || "TBD";
      target = source.roku;
      if (
        source.roku &&
        row.source_revision &&
        sha.test(source.roku) &&
        sha.test(row.source_revision)
      ) {
        const status = revisionsMatch(source.roku, row.source_revision)
          ? "identical"
          : (
              await githubRest<{ status: string }>(
                `/repos/ApollosProject/apollos-platforms/compare/${source.roku}...${row.source_revision}`,
              ).catch(() => null)
            )?.status;
        outdated = status === "behind";
        label =
          (
            {
              behind: "Observed: Behind source",
              identical: "Observed: At source",
              ahead: "Observed: Ahead of source",
            } as Record<string, string>
          )[status || ""] || "Unverified";
      }
    }
    return {
      ...row,
      version_status_label: label,
      freshness_display: display,
      comparison_display:
        platform === "roku"
          ? target?.slice(0, 7) || "unknown"
          : target || "unknown",
      is_outdated: outdated,
      live_status_detail: store
        ? row.live_status_detail
        : "Store publication not verified; analytics observation only.",
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
  const tag = (await stableTags(token))[0]?.name;
  if (!tag) throw new Error("No stable release tag found");
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
