import { notFound } from "next/navigation";
import {
  Checked,
  Heading,
  IssueList,
  TableScroll,
  Unavailable,
} from "@/components/dashboard";
import { churchActivity, churches } from "@/lib/churches";
import { appsDashboard } from "@/lib/apps";
import { fleetDashboard, astroURL } from "@/lib/fleet";
import { openWork, reportFor } from "@/lib/reports";
import { timeWindow } from "@/lib/window";
// undefined: the directory is unavailable; null: no such church.
const find = async (slug: string) => {
  const list = await churches().catch(() => null);
  return list
    ? list.find((c) => c.slug === slug || c.altSlugs.includes(slug)) || null
    : undefined;
};
export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const church = await find(decodeURIComponent((await params).slug));
  return { title: church?.name || "Not found" };
}
const host = (url?: string | null) => url?.replace(/^https?:\/\//, "") || null;
export default async function ChurchPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const church = await find(decodeURIComponent((await params).slug));
  if (church === null) notFound();
  if (!church)
    return (
      <>
        <Heading title="Church" detail="Church lookup." path="churches" />
        <Unavailable
          name="Church directory"
          detail="The church directory requires APOLLOS_API_KEY and the scheduled apps refresh."
        />
      </>
    );
  // Existing snapshots only: no new Linear, GitHub, Airflow, or store requests.
  const [apps, fleet, open, report] = await Promise.all([
    appsDashboard().catch(() => null),
    fleetDashboard().catch(() => null),
    openWork().catch(() => null),
    reportFor(timeWindow({ days: "30" })).catch(() => null),
  ]);
  const activity = churchActivity(church, {
    apps: apps?.rows || [],
    dags: fleet?.dags || [],
    issues: open || [],
  });
  const done = churchActivity(church, {
    apps: [],
    dags: [],
    issues: report?.completed || [],
  }).issues;
  const config: [string, React.ReactNode][] = [
    [
      "Slug",
      <code key="s">{[church.slug, ...church.altSlugs].join(", ")}</code>,
    ],
    [
      "Deep-link domain",
      host(church.universalLinkHost) ? (
        <code key="d">{host(church.universalLinkHost)}</code>
      ) : (
        "Not set"
      ),
    ],
    [
      "Web",
      church.apollosWebUrl ? (
        <a key="w" href={church.apollosWebUrl}>
          {host(church.apollosWebUrl)}
        </a>
      ) : (
        "Not set"
      ),
    ],
    [
      "App Store",
      church.mobileAppStoreUrl ? (
        <a key="a" href={church.mobileAppStoreUrl}>
          {church.appleBundleId || "Listing"}
        </a>
      ) : (
        church.appleBundleId || "Not set"
      ),
    ],
    [
      "Google Play",
      church.mobilePlayStoreUrl ? (
        <a key="g" href={church.mobilePlayStoreUrl}>
          {church.androidPkgId || "Listing"}
        </a>
      ) : (
        church.androidPkgId || "Not set"
      ),
    ],
    [
      "Giving",
      church.givingInMaintenance
        ? "In maintenance"
        : church.givingEnabled
          ? `Enabled${church.givingMobileProvider || church.givingWebProvider ? ` · ${[church.givingMobileProvider, church.givingWebProvider].filter(Boolean).join(" / ")}` : ""}`
          : "Not enabled",
    ],
    ["Timezone", church.timezone || "Not set"],
  ];
  return (
    <>
      <Heading
        title={church.name || church.slug}
        detail="Configuration, app versions, data pipelines, and open work for this church."
        path={`churches/${church.slug}`}
      />
      <section>
        <h2>Configuration</h2>
        <article>
          <dl className="facts">
            {config.map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        </article>
      </section>
      <section>
        <h2>Apps {apps && <small>{activity.apps.length}</small>}</h2>
        {!apps ? (
          <Unavailable name="App release data" />
        ) : activity.apps.length ? (
          <TableScroll label="Apps">
            <table>
              <caption>
                <Checked at={apps.checked_at} />
              </caption>
              <thead>
                <tr>
                  <th>App</th>
                  <th>Status</th>
                  <th>Runtime / source</th>
                  <th>Target</th>
                </tr>
              </thead>
              <tbody>
                {activity.apps.map((row) => (
                  <tr key={`${row.apollos_platform}|${row.bundle_id}`}>
                    <th>
                      {row.application_name}
                      <small>
                        {row.apollos_platform} · {row.bundle_id}
                      </small>
                    </th>
                    <td>
                      <span
                        className={`badge ${row.is_outdated ? "low" : row.version_status_label === "At release" ? "high" : ""}`}
                      >
                        {row.version_status_label}
                      </span>
                      <small>{row.live_status_detail}</small>
                    </td>
                    <td>{row.freshness_display || "Unknown"}</td>
                    <td>{row.comparison_display}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TableScroll>
        ) : (
          <p className="muted">No app observations for this church.</p>
        )}
      </section>
      <section>
        <h2>DAGs {fleet && <small>{activity.dags.length}</small>}</h2>
        {!fleet ? (
          <Unavailable name="Fleet health" />
        ) : activity.dags.length ? (
          <article>
            <ul className="issue-list">
              {activity.dags.map((dag) => (
                <li key={dag.dag_id}>
                  <span
                    className={`badge ${dag.state === "failed" ? "low" : ""}`}
                  >
                    {dag.state}
                  </span>
                  <div>
                    <a href={astroURL(dag.dag_id)}>{dag.dag_id}</a>
                    <small>{dag.dag_run_id || "No run"}</small>
                  </div>
                </li>
              ))}
            </ul>
          </article>
        ) : (
          <p className="muted">No active DAGs named for this church.</p>
        )}
      </section>
      <section>
        <h2>
          Open issues mentioning {church.name || church.slug}{" "}
          {open && <small>{activity.issues.length}</small>}
        </h2>
        {open ? (
          <article>
            <IssueList
              issues={activity.issues}
              empty="No open issues mention this church."
            />
          </article>
        ) : (
          <Unavailable name="Open work" />
        )}
      </section>
      <section>
        <h2>
          Completed in the last 30 days {report && <small>{done.length}</small>}
        </h2>
        {report ? (
          <article>
            <IssueList
              issues={done}
              empty="No completed issues mention this church."
            />
          </article>
        ) : (
          <Unavailable name="Completed work" />
        )}
      </section>
    </>
  );
}
