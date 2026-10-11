import { unstable_cache } from "next/cache";
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
// One small request an hour; church config changes rarely.
export const churches = unstable_cache(
  async () =>
    (await clusterChurches<Church>(fields)).sort((a, b) =>
      (a.name || a.slug).localeCompare(b.name || b.slug),
    ),
  ["churches-v1"],
  { revalidate: 3600 },
);
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
  // Match the church name or slug as whole words, so short names don't match inside other words.
  const names = [church.name, ...slugs]
    .filter((v) => v && v.length > 2)
    .map((v) => ` ${words(v)} `);
  const mentions = (text: string) =>
    names.some((n) => ` ${words(text)} `.includes(n));
  return {
    apps: data.apps.filter((r) =>
      slugs.includes((r.build_church || r.church).toLowerCase()),
    ),
    dags: data.dags.filter((d) =>
      slugs.some((s) => d.dag_id.toLowerCase().startsWith(`${s}_`)),
    ),
    issues: data.issues.filter((i) => mentions(i.title)),
  };
}
