import { readSnapshot } from "./cache";
import { clusterChurches } from "./stores";
import type { AppRow, Dag, Issue } from "./types";
// Only operational fields; keys, account IDs, and fingerprints stay out.
export type Church = {
  slug: string;
  name: string;
  altSlugs: string[];
  universalLinkHost?: string | null;
  apollosWebUrl?: string | null;
  mobileAppStoreUrl?: string | null;
  mobilePlayStoreUrl?: string | null;
  appleBundleId?: string | null;
  androidPkgId?: string | null;
  givingEnabled?: boolean | null;
  givingInMaintenance?: boolean | null;
  givingWebProvider?: string | null;
  givingMobileProvider?: string | null;
  timezone?: string | null;
};
const fields =
  "slug name altSlugs universalLinkHost apollosWebUrl mobileAppStoreUrl mobilePlayStoreUrl appleBundleId androidPkgId givingEnabled givingInMaintenance givingWebProvider givingMobileProvider timezone";
export const churchDirectory = async () =>
  (await clusterChurches<Church>(fields)).sort((a, b) =>
    (a.name || a.slug).localeCompare(b.name || b.slug),
  );
// Pages read the snapshot the apps Cron refresh writes; null means unavailable.
export async function churches(): Promise<Church[] | null> {
  const snapshot = await readSnapshot<Church[]>("churches", 3600);
  if (snapshot) return snapshot;
  if (process.env.VERCEL_ENV) return null;
  const live = await churchDirectory();
  return live.length ? live : null;
}
const words = (s: string) =>
  s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
export function matchChurches(list: Church[], query: string) {
  const q = words(query);
  return q
    ? list.filter((c) =>
        [c.name, c.slug, ...c.altSlugs].some((v) => words(v || "").includes(q)),
      )
    : list;
}
// Everything Bug Board already knows about one church, from existing snapshots.
export function churchActivity(
  church: Church,
  data: { apps: AppRow[]; dags: Dag[]; issues: Issue[] },
) {
  const slugs = [church.slug, ...church.altSlugs].map((s) => s.toLowerCase());
  // Shared mobile apps have no single build church; match the church's bundles.
  // Apple and Android IDs are separate namespaces, so match within the platform.
  const bundleFor = (platform: string) =>
    (platform === "ios"
      ? church.appleBundleId
      : church.androidPkgId
    )?.toLowerCase();
  // Match the church name or slug as whole words, so short names don't match inside other words.
  const names = [church.name, ...slugs]
    .filter((v) => v && v.length > 2)
    .map((v) => ` ${words(v)} `);
  const mentions = (text: string) =>
    names.some((n) => ` ${words(text)} `.includes(n));
  return {
    apps: data.apps.filter(
      (r) =>
        slugs.includes((r.build_church || r.church).toLowerCase()) ||
        bundleFor(r.apollos_platform) === r.bundle_id.toLowerCase(),
    ),
    dags: data.dags.filter((d) =>
      slugs.some((s) => d.dag_id.toLowerCase().startsWith(`${s}_`)),
    ),
    issues: data.issues.filter((i) => mentions(i.title)),
  };
}
