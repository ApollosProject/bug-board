import Link from "next/link";
import {
  Checked,
  Heading,
  Unavailable,
  TableScroll,
} from "@/components/dashboard";
import { session } from "@/lib/auth";
import { appsDashboard, deployTarget } from "@/lib/apps";
import { param, queryString } from "@/lib/window";
import type { Search } from "@/lib/types";
export const metadata = { title: "Apps" };
const platformNames: Record<string, string> = {
  amazon: "Amazon",
  android: "Android",
  androidtv: "Android TV",
  ios: "iOS",
  roku: "Roku",
  tvos: "tvOS",
};
export default async function Apps({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams,
    platform = param(search, "platform"),
    query = param(search, "q");
  const [data, user] = await Promise.all([
    appsDashboard().catch(() => null),
    session(),
  ]);
  const tabs = [
    ...new Set(data?.rows.map((row) => row.apollos_platform) || []),
  ].sort();
  const rows =
    data?.rows.filter(
      (row) =>
        (!platform || row.apollos_platform === platform) &&
        `${row.build_church || ""} ${row.church} ${row.application_name} ${row.bundle_id}`
          .toLowerCase()
          .includes(query.toLowerCase()),
    ) || [];
  return (
    <>
      <Heading
        title="Apps"
        detail="Which apps run the latest release, with production deploy controls."
      />
      {param(search, "deployed") === "1" && (
        <article role="status">
          Deployment requested. GitHub Actions checks production readiness; this
          is not proof that a new build is live.
        </article>
      )}
      {!data && (
        <Unavailable
          name="App release data"
          detail="Configure BigQuery credentials, APOLLOS_API_KEY, GitHub, and the scheduled refresh. Missing store evidence is never shown as a current release."
        />
      )}
      <nav aria-label="Platform" className="presets platforms">
        {["", ...tabs].map((tab) => (
          <Link
            key={tab}
            href={`?${queryString({ q: query, ...(tab && { platform: tab }) })}`}
            className={platform === tab ? "selected" : ""}
            aria-current={platform === tab ? "page" : undefined}
          >
            {tab ? platformNames[tab] || tab : "All platforms"}
          </Link>
        ))}
      </nav>
      <form method="get" className="filter-form">
        <input type="hidden" name="platform" value={platform} />
        <label>
          Search apps
          <input
            type="search"
            name="q"
            placeholder="Church, app, or bundle ID"
            defaultValue={query}
          />
        </label>
        <button type="submit">Filter apps</button>
      </form>
      {data && (
        <>
          <p className="muted">
            <Checked at={data.checked_at} /> · iOS and Android: published store
            build. TV and Roku: newest release build seen in the last{" "}
            {data.lookback_days} days.
          </p>
          <TableScroll label="Apps">
            <table className="wide">
              <caption>
                {rows.length} apps · {rows.filter((r) => r.is_outdated).length}{" "}
                behind
              </caption>
              <thead>
                <tr>
                  <th>App</th>
                  <th>Status</th>
                  <th>Live</th>
                  <th>Latest</th>
                  {!platform && <th>Platform</th>}
                  <th>Deploy</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => {
                  const church = row.build_church || row.church,
                    target = deployTarget(
                      data.rows,
                      row.apollos_platform,
                      row.bundle_id,
                      church,
                    );
                  return (
                    <tr
                      key={`${row.church}|${row.apollos_platform}|${row.bundle_id}`}
                    >
                      <th>
                        {row.application_name}
                        <small>
                          {church} · {row.bundle_id}
                        </small>
                      </th>
                      <td>
                        <span
                          className={`badge ${row.is_outdated ? "low" : row.version_status_label === "Up to date" ? "high" : ""}`}
                        >
                          {row.version_status_label}
                        </span>
                        {row.live_status_detail && (
                          <small>{row.live_status_detail}</small>
                        )}
                      </td>
                      <td>{row.freshness_display || "Unknown"}</td>
                      <td>{row.comparison_display}</td>
                      {!platform && (
                        <td>
                          {platformNames[row.apollos_platform] ||
                            row.apollos_platform}
                        </td>
                      )}
                      <td>
                        {user && target && process.env.GITHUB_ACTIONS_TOKEN ? (
                          <>
                            {/* A popover renders in the top layer, so the table cell cannot clip it. */}
                            <button
                              type="button"
                              className="outline"
                              popoverTarget={`deploy-${i}`}
                            >
                              Deploy
                            </button>
                            <form
                              id={`deploy-${i}`}
                              popover="auto"
                              className="confirm"
                              method="post"
                              action={`/apps/deploy/${encodeURIComponent(row.apollos_platform)}/${encodeURIComponent(row.bundle_id)}/${encodeURIComponent(church)}`}
                            >
                              <p>
                                Deploy the latest release of{" "}
                                <strong>{row.application_name}</strong> (
                                {row.apollos_platform}) to production?
                              </p>
                              <div className="toolbar">
                                <button type="submit">
                                  Confirm deployment
                                </button>
                                <button
                                  type="button"
                                  className="secondary"
                                  popoverTarget={`deploy-${i}`}
                                  popoverTargetAction="hide"
                                >
                                  Cancel
                                </button>
                              </div>
                            </form>
                          </>
                        ) : (
                          "—"
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {!rows.length && <p>No matching apps.</p>}
          </TableScroll>
        </>
      )}
    </>
  );
}
