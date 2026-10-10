import Link from "next/link";
import { Heading, Unavailable } from "@/components/dashboard";
import { session } from "@/lib/auth";
import { appsDashboard, deployTarget } from "@/lib/apps";
import { storePlatforms } from "@/lib/stores";
import { param, queryString } from "@/lib/window";
import type { Search } from "@/lib/types";
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
        detail="Published store runtimes, source comparisons, and safe production deployment controls."
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
      <div className="toolbar">
        <Link href={`?${queryString({ q: query })}`}>All platforms</Link>
        {tabs.map((tab) => (
          <Link
            key={tab}
            href={`?${queryString({ q: query, platform: tab })}`}
            aria-current={platform === tab ? "page" : undefined}
          >
            {tab}
          </Link>
        ))}
      </div>
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
            Checked {data.checked_at} · Analytics lookback {data.lookback_days}{" "}
            days. Non-mobile store publication is unverified; those badges
            describe observations only.
          </p>
          <article className="table-scroll">
            <table>
              <caption>{rows.length} apps</caption>
              <thead>
                <tr>
                  <th>App</th>
                  <th>Platform</th>
                  <th>Runtime / source</th>
                  <th>Target</th>
                  <th>Status</th>
                  <th>Deploy</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
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
                      <td>{row.apollos_platform}</td>
                      <td>{row.freshness_display || "Unknown"}</td>
                      <td>{row.comparison_display}</td>
                      <td>
                        <span
                          className={`badge ${row.is_outdated ? "low" : storePlatforms.has(row.apollos_platform) && row.version_status_label === "At release" ? "high" : ""}`}
                        >
                          {row.version_status_label}
                        </span>
                        <small>{row.live_status_detail}</small>
                      </td>
                      <td>
                        {user && target && process.env.GITHUB_ACTIONS_TOKEN ? (
                          <details>
                            <summary>Deploy</summary>
                            <form
                              method="post"
                              action={`/apps/deploy/${encodeURIComponent(row.apollos_platform)}/${encodeURIComponent(row.bundle_id)}/${encodeURIComponent(church)}`}
                            >
                              <p>Deploy latest stable source to production?</p>
                              <button type="submit">Confirm deployment</button>
                            </form>
                          </details>
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
          </article>
        </>
      )}
    </>
  );
}
