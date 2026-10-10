import { Suspense } from "react";
import { notFound } from "next/navigation";
import {
  Heading,
  GroupedIssues,
  Loading,
  Unavailable,
  WindowControls,
} from "@/components/dashboard";
import { people, personName, slugFor } from "@/lib/config";
import {
  openWork,
  personPayload,
  projectDashboard,
  reportFor,
} from "@/lib/reports";
import { supportSlugs, platformFor } from "@/lib/metrics";
import { timeWindow } from "@/lib/window";
import type { Search, Window } from "@/lib/types";
import { Regressions } from "@/components/regressions";
async function Work({ slug }: { slug: string }) {
  const data = await Promise.all([openWork(), projectDashboard()]).catch(
    () => null,
  );
  if (!data) return <Unavailable name="Current work" />;
  const [issues, projects] = data;
  const owned = issues
      .filter(
        (i) => slugFor(i.assignee?.name, i.assignee?.displayName) === slug,
      )
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
    support = supportSlugs(projects).includes(slug);
  const current = owned.filter((i) =>
    support
      ? !i.project || i.project.name === "Customer Success"
      : projects.some((p) => p.id === i.project?.id),
  );
  const other = owned.filter((i) => !current.includes(i));
  return (
    <>
      <h2>{support ? "Current Support Work" : "Current Project Work"}</h2>
      <GroupedIssues issues={current} />
      {!!other.length && (
        <>
          <h2>Other Work</h2>
          <GroupedIssues issues={other} />
        </>
      )}
    </>
  );
}
async function Metrics({ slug, window }: { slug: string; window: Window }) {
  const result = await Promise.all([
    personPayload(slug, window),
    reportFor(window),
  ]).catch(() => null);
  if (!result) return <Unavailable name="Person metrics" />;
  const [data, report] = result;
  const items = report.completed.filter(
      (i) => slugFor(i.assignee?.name, i.assignee?.displayName) === slug,
    ),
    platforms = new Map<string, number>();
  for (const issue of items) {
    const platform = platformFor(issue);
    if (platform) platforms.set(platform, (platforms.get(platform) || 0) + 1);
  }
  return (
    <>
      <h2>Delivery · {window.label}</h2>
      <div className="cards">
        {Object.entries(data.metrics).map(([key, metric]) => (
          <article key={key}>
            <small>
              {key in data.links ? (
                <a href={data.links[key as keyof typeof data.links]}>
                  {metric.label}
                </a>
              ) : (
                metric.label
              )}
            </small>
            <strong className="metric-value">{metric.display}</strong>
            {metric.vs_team && (
              <span
                className={`badge ${metric.vs_team.tone || ""}`}
                title={`Engineering avg ${metric.vs_team.eng_avg} · σ ${metric.vs_team.eng_stdev}`}
              >
                {metric.vs_team.label}
              </span>
            )}
          </article>
        ))}
      </div>
      {!!platforms.size && (
        <section>
          <h2>Work by Platform</h2>
          <article className="platform-bars">
            {[...platforms].map(([platform, value]) => (
              <div key={platform}>
                <span>{platform}</span>
                <meter
                  value={value}
                  min={0}
                  max={Math.max(...platforms.values())}
                />
                <strong>{value}</strong>
              </div>
            ))}
          </article>
        </section>
      )}
      <h2>Completed</h2>
      <GroupedIssues
        issues={items.sort((a, b) =>
          (b.completedAt || "").localeCompare(a.completedAt || ""),
        )}
      />
    </>
  );
}
export default async function Person({
  params,
  searchParams,
}: {
  params: Promise<{ slug: string }>;
  searchParams: Promise<Search>;
}) {
  const { slug } = await params;
  if (!Object.hasOwn(people, slug)) notFound();
  const window = timeWindow(await searchParams);
  return (
    <>
      <Heading
        title={personName(slug)}
        detail="Current work, delivery, and engineering-relative performance."
      />
      <Suspense fallback={<Loading />}>
        <Work slug={slug} />
      </Suspense>
      <WindowControls window={window} presets={[1, 7, 30]} />
      <Suspense fallback={<Loading />}>
        <Regressions slug={slug} />
      </Suspense>
      <Suspense fallback={<Loading />}>
        <Metrics slug={slug} window={window} />
      </Suspense>
    </>
  );
}
