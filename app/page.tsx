import { Suspense } from "react";
import type { Search, Window } from "@/lib/types";
import { timeWindow } from "@/lib/window";
import { openWork, reportFor } from "@/lib/reports";
import { isPriorityBug, isWork, resolution } from "@/lib/metrics";
import { priorityLabels } from "@/lib/config";
import { Regressions } from "@/components/regressions";
import {
  Heading,
  IssueList,
  Loading,
  Unavailable,
  WindowControls,
} from "@/components/dashboard";
async function OpenWork() {
  const data = await openWork().catch(() => null);
  if (!data) return <Unavailable name="Open work" />;
  const issues = data.filter((i) => !i.project);
  return (
    <>
      <section>
        <h2>Priority Bugs</h2>
        <article>
          <IssueList
            issues={issues
              .filter(isPriorityBug)
              .sort((a, b) => a.createdAt.localeCompare(b.createdAt))}
          />
        </article>
      </section>
      <section>
        <h2>Open Assigned Work</h2>
        <article>
          <IssueList
            issues={issues
              .filter((i) => i.assignee && i.priority > 2 && isWork(i))
              .sort((a, b) => b.createdAt.localeCompare(a.createdAt))}
          />
        </article>
      </section>
    </>
  );
}
async function Resolution({ window }: { window: Window }) {
  const data = await reportFor(window).catch(() => null);
  if (!data) return <Unavailable name="Resolution metrics" />;
  const stats = resolution(data.completed);
  return (
    <section>
      <h2>Resolution by Priority</h2>
      <article className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Priority</th>
              <th>Resolved</th>
              <th>Average</th>
              <th>p95</th>
            </tr>
          </thead>
          <tbody>
            {stats.map((row) => (
              <tr key={row.priority}>
                <td>{priorityLabels[row.priority]}</td>
                <td>{row.count}</td>
                <td>{row.avg_days}d</td>
                <td>{row.p95_days}d</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!stats.length && <p>No resolved work in this window.</p>}
      </article>
    </section>
  );
}
export default async function Home({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const window = timeWindow(await searchParams);
  return (
    <>
      <Heading
        title="Bug Board"
        detail="A clear view of engineering health, work, and delivery."
      />
      <WindowControls window={window} />
      <Suspense fallback={<Loading />}>
        <Regressions />
      </Suspense>
      <Suspense fallback={<Loading />}>
        <Resolution window={window} />
      </Suspense>
      <Suspense fallback={<Loading />}>
        <OpenWork />
      </Suspense>
    </>
  );
}
