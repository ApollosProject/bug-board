import { importPKCS8, SignJWT } from "jose";
import { GoogleAuth } from "google-auth-library";
import { readSnapshot, writeSnapshot } from "./cache";
import { mapConcurrent, requestJson, UpstreamError } from "./http";
import type { AppRow } from "./types";
export const storePlatforms = new Set(["ios", "android", "androidtv"]);
export type StoreBuild = { native_build: string; native_version?: string };
export type StoreRelease = {
  builds?: StoreBuild[];
  live_status_detail?: string;
  build_church?: string | null;
  deploy_target_count?: number;
};
const clusterHeaders = () => ({
  "x-api-key": process.env.APOLLOS_API_KEY || "",
});
async function config(church: string, key: string): Promise<unknown> {
  try {
    return (
      await requestJson<{ value: unknown }>(
        `https://cluster.apollos.app/api/config/${church}/${key}`,
        { headers: clusterHeaders() },
        "Cluster",
      )
    ).value;
  } catch (error) {
    if (error instanceof UpstreamError && error.status === 404) return null;
    throw error;
  }
}
function decode(value: unknown) {
  return typeof value === "string"
    ? (JSON.parse(value) as Record<string, string | boolean>)
    : (value as Record<string, string | boolean>);
}
export function compareVersions(a: string, b: string) {
  return a.localeCompare(b, "en", { numeric: true, sensitivity: "base" });
}
type AppleVersion = {
  id: string;
  type: string;
  attributes: {
    bundleId?: string;
    appVersionState?: string;
    appStoreState?: string;
    versionString: string;
    version: string;
  };
  relationships: { build: { data: { id: string } } };
};
export function publishedApple(payload: {
  data: AppleVersion[];
  included?: AppleVersion[];
  links?: { next?: string };
}): StoreBuild[] {
  if (payload.links?.next) throw new Error("Incomplete store release list");
  const latest = payload.data
    .filter((v) =>
      ["READY_FOR_DISTRIBUTION", "READY_FOR_SALE"].includes(
        v.attributes.appVersionState || v.attributes.appStoreState || "",
      ),
    )
    .sort((a, b) =>
      compareVersions(b.attributes.versionString, a.attributes.versionString),
    )[0];
  if (!latest) return [];
  const build = payload.included?.find(
    (b) => b.type === "builds" && b.id === latest.relationships.build.data.id,
  );
  if (!build?.attributes.version) throw new Error("Store build unavailable");
  return [
    {
      native_build: build.attributes.version,
      native_version: latest.attributes.versionString,
    },
  ];
}
export function publishedAndroid(
  payload: {
    releases?: {
      track: string;
      releaseLifecycleState: string;
      activeArtifacts: { versionCode: string | number }[];
    }[];
  },
  track: string,
): StoreBuild[] {
  return (payload.releases || [])
    .filter(
      (r) =>
        r.track === track &&
        r.releaseLifecycleState === "RELEASE_LIFECYCLE_STATE_PUBLISHED",
    )
    .flatMap((r) =>
      r.activeArtifacts.map((a) => ({ native_build: String(a.versionCode) })),
    );
}
async function apple(church: string, bundle: string) {
  const encoded = await config(church, "APP.APPLE_API_KEY_B64");
  const key = decode(
    encoded
      ? Buffer.from(String(encoded), "base64").toString()
      : await config(church, "APP.APPLE_API_KEY"),
  );
  const pem = key.is_key_content_base64
    ? Buffer.from(String(key.key), "base64").toString()
    : String(key.key);
  const token = await new SignJWT({})
    .setProtectedHeader({ alg: "ES256", kid: String(key.key_id) })
    .setIssuer(String(key.issuer_id))
    .setAudience("appstoreconnect-v1")
    .setIssuedAt(Math.floor(Date.now() / 1000) - 10)
    .setExpirationTime("10m")
    .sign(await importPKCS8(pem, "ES256"));
  const headers = { Authorization: `Bearer ${token}` };
  const apps = await requestJson<{ data: AppleVersion[] }>(
    `https://api.appstoreconnect.apple.com/v1/apps?${new URLSearchParams({ "filter[bundleId]": bundle })}`,
    { headers },
    "Apple",
  );
  const matches = apps.data.filter((app) => app.attributes.bundleId === bundle);
  if (matches.length !== 1) throw new Error("App identity not unique");
  const payload = await requestJson<Parameters<typeof publishedApple>[0]>(
    `https://api.appstoreconnect.apple.com/v1/apps/${matches[0].id}/appStoreVersions?${new URLSearchParams({ "filter[platform]": "IOS", "filter[appStoreState]": "READY_FOR_SALE", include: "build", limit: "200" })}`,
    { headers },
    "Apple",
  );
  return publishedApple(payload);
}
async function android(church: string, bundle: string, platform: string) {
  const info = JSON.parse(
    Buffer.from(
      String(await config(church, "APP.GOOGLE_API_KEY_B64")),
      "base64",
    ).toString(),
  );
  const auth = new GoogleAuth({
    credentials: info,
    scopes: ["https://www.googleapis.com/auth/androidpublisher"],
  });
  const token = await auth.getAccessToken();
  const track = platform === "androidtv" ? "tv:production" : "production";
  const response = await fetch(
    `https://androidpublisher.googleapis.com/androidpublisher/v3/applications/${encodeURIComponent(bundle)}/tracks/${encodeURIComponent(track)}/releases`,
    {
      headers: { Authorization: `Bearer ${token}` },
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    },
  );
  const payload = await response.json();
  if (
    response.status === 429 ||
    (response.status === 403 &&
      payload.error?.message === "Listing releases quota exceeded.")
  )
    return { live_status_detail: "Store API quota exceeded; retrying later" };
  if (!response.ok) throw new Error("Google store unavailable");
  return { builds: publishedAndroid(payload, track) };
}
export async function appDirectory() {
  if (!process.env.APOLLOS_API_KEY) return [];
  try {
    const payload = await requestJson<{
      data: {
        churches: {
          slug: string;
          appleBundleId?: string;
          androidPkgId?: string;
        }[];
      };
      errors?: unknown[];
    }>(
      "https://cluster.apollos.app/graphql",
      {
        method: "POST",
        headers: { ...clusterHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify({
          query: "query { churches { slug appleBundleId androidPkgId } }",
        }),
      },
      "Cluster",
    );
    if (payload.errors?.length || !Array.isArray(payload.data.churches))
      return [];
    return payload.data.churches;
  } catch {
    return [];
  }
}
export async function lookupRelease(
  platform: string,
  bundle: string,
  churches: string[],
): Promise<StoreRelease> {
  const name = `store:${platform}:${bundle}`;
  const cached = await readSnapshot<StoreRelease>(name, 3600);
  if (cached) return cached;
  if (!process.env.APOLLOS_API_KEY)
    return {
      live_status_detail: "Configure APOLLOS_API_KEY for store verification",
    };
  for (const church of churches
    .filter((s) => /^[A-Za-z0-9_-]+$/.test(s))
    .sort()) {
    try {
      const configured = await config(
        church,
        platform === "ios" ? "APP.APPLE_BUNDLE_ID" : "APP.ANDROID_PKG_ID",
      );
      if (
        typeof configured !== "string" ||
        configured.toLowerCase() !== bundle.toLowerCase()
      )
        continue;
      const release =
        platform === "ios"
          ? { builds: await apple(church, configured) }
          : await android(church, configured, platform);
      if (process.env.UPSTASH_REDIS_REST_URL)
        await writeSnapshot(name, release, release.builds ? 1800 : 3600);
      return release;
    } catch {
      console.warn("Store lookup unavailable:", platform, bundle);
    }
  }
  return { live_status_detail: "Store release could not be verified" };
}
export async function planStores(rows: AppRow[]) {
  const groups = new Map<
    string,
    { platform: string; bundle: string; churches: Set<string> }
  >();
  for (const row of rows)
    if (
      storePlatforms.has(row.apollos_platform) &&
      /^[\w.-]+$/.test(row.bundle_id)
    ) {
      const key = `${row.apollos_platform}:${row.bundle_id.toLowerCase()}`;
      const group = groups.get(key) || {
        platform: row.apollos_platform,
        bundle: row.bundle_id.toLowerCase(),
        churches: new Set<string>(),
      };
      const church = row.build_church || row.church;
      if (/^[A-Za-z0-9_-]+$/.test(church)) group.churches.add(church);
      groups.set(key, group);
    }
  const directory = await appDirectory();
  return [...groups.entries()].map(([key, group]) => {
    const targets = directory
      .filter(
        (c) =>
          (group.platform === "ios"
            ? c.appleBundleId
            : c.androidPkgId
          )?.toLowerCase() === group.bundle,
      )
      .map((c) => c.slug)
      .filter((s) => /^[A-Za-z0-9_-]+$/.test(s));
    return {
      key,
      platform: group.platform,
      bundle: group.bundle,
      churches: [...new Set([...group.churches, ...targets])],
      targets,
    };
  });
}
export async function plannedRelease(
  plan: Awaited<ReturnType<typeof planStores>>[number],
) {
  const release = await lookupRelease(
    plan.platform,
    plan.bundle,
    plan.churches,
  );
  return {
    ...release,
    ...(plan.targets.length
      ? {
          build_church: plan.targets.length === 1 ? plan.targets[0] : null,
          deploy_target_count: plan.targets.length,
        }
      : {}),
  };
}
export async function mobileReleases(rows: AppRow[]) {
  const result = await mapConcurrent(
    await planStores(rows),
    8,
    async (plan) => [plan.key, await plannedRelease(plan)] as const,
  );
  return new Map(result);
}
