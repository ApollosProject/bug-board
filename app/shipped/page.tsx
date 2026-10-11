import Link from "next/link";
import { Suspense } from "react";
import {
  Heading,
  Loading,
  Unavailable,
  WindowControls,
} from "@/components/dashboard";
import { personName } from "@/lib/config";
import { reportFor, shipped } from "@/lib/reports";
import { timeWindow } from "@/lib/window";
import type { Search, Window } from "@/lib/types";
export const metadata = { title: "Shipped" };
const updatesNote =
  "Project updates are shown only for windows that end today; Linear keeps only each project's latest update.";
const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;
// https://github.com/owner/repo/pull/123 → repo#123
const pullRequest = (url: string) => {
  const [, , , , repo, , number] = url.split("/");
  return number ? `${repo}#${number}` : url;
};
async function Report({ window }: { window: Window }) {
  // Windows inside the last 30 days filter the refreshed snapshot (no extra
  // requests); older windows load their own data, like the Team page.
  const recent = timeWindow({ days: "30" });
  const data = await reportFor(
    window.after >= recent.after ? recent : window,
  ).catch(() => null);
  if (!data) return <Unavailable name="Shipped work" />;
  const rows = shipped(data, window);
  const markdown = [
    ...(rows[0]?.updatesKnown ? [] : [`_${updatesNote}_`]),
    ...rows.map(({ slug, prs, issues, updates }) =>
      [
        `### ${personName(slug)}`,
        ...issues.map((i) => `- ${i.identifier} ${i.title} (${i.url})`),
        ...updates.map((p) => `- Project update: ${p.name} (${p.url})`),
        ...(prs.length
          ? [`- Merged: ${prs.map((pr) => pullRequest(pr.url)).join(", ")}`]
          : []),
        ...(!issues.length && !prs.length && !updates.length
          ? ["- Nothing shipped in this window"]
          : []),
      ].join("\n"),
    ),
  ].join("\n\n");
  return (
    <>
      {!rows[0]?.updatesKnown && <p className="muted">{updatesNote}</p>}
      <details>
        <summary>Copy as Markdown</summary>
        <textarea readOnly rows={12} value={markdown} aria-label="Markdown" />
      </details>
      {rows.map(({ slug, prs, issues, updates }) => (
        <section key={slug}>
          <h2>
            <Link href={`/team/${slug}`}>{personName(slug)}</Link>{" "}
            <small>
              {plural(issues.length, "issue")} · {plural(prs.length, "PR")}
            </small>
          </h2>
          <article>
            {!issues.length && !prs.length && !updates.length && (
              <p className="muted">Nothing shipped in this window.</p>
            )}
            {!!issues.length && (
              <ul className="shipped">
                {issues.map((i) => (
                  <li key={i.id}>
                    <a href={i.url}>{i.title}</a> <small>{i.identifier}</small>
                  </li>
                ))}
              </ul>
            )}
            {!!updates.length && (
              <p className="muted">
                Project updates:{" "}
                {updates.map((p, n) => (
                  <span key={p.id}>
                    {n > 0 && ", "}
                    <a href={p.url}>{p.name}</a>
                  </span>
                ))}
              </p>
            )}
            {!!prs.length && (
              <p className="muted">
                Merged:{" "}
                {prs.map((pr, n) => (
                  <span key={pr.id}>
                    {n > 0 && ", "}
                    <a href={pr.url}>{pullRequest(pr.url)}</a>
                  </span>
                ))}
              </p>
            )}
          </article>
        </section>
      ))}
    </>
  );
}
export default async function Shipped({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams;
  // Default to the last week; any explicit window wins.
  const window = timeWindow(
    search.days || search.start ? search : { days: "7" },
  );
  return (
    <>
      <Heading
        title="Shipped"
        detail="What each engineer completed and merged, for standups and weekly updates."
      />
      <WindowControls window={window} presets={[7, 14, 30]} />
      <Suspense fallback={<Loading window={window} />}>
        <Report window={window} />
      </Suspense>
    </>
  );
}
