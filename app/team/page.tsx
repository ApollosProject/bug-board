import Link from "next/link";
import { Suspense } from "react";
import {
  Heading,
  Loading,
  Unavailable,
  WindowControls,
} from "@/components/dashboard";
import { reportFor, sortedTeam } from "@/lib/reports";
import { teamColumns, type TeamKey } from "@/lib/metrics";
import { param, queryString, timeWindow } from "@/lib/window";
import type { Search, Window } from "@/lib/types";
async function TeamTable({
  window,
  search,
}: {
  window: Window;
  search: Search;
}) {
  const data = await reportFor(window).catch(() => null);
  if (!data) return <Unavailable name="Team metrics" />;
  const rows = sortedTeam(data.rows, search),
    sort = param(search, "sort") || "-prs_merged";
  const query = {
    ...window.query,
    ...(param(search, "everyone") === "1" ? { everyone: "1" } : {}),
  };
  return (
    <article className="table-scroll">
      <table>
        <caption>Team metrics · {data.window.label}</caption>
        <thead>
          <tr>
            {Object.entries({ person: "Person", ...teamColumns }).map(
              ([key, label]) => (
                <th
                  key={key}
                  aria-sort={
                    sort.replace(/^-/, "") === key
                      ? sort.startsWith("-")
                        ? "descending"
                        : "ascending"
                      : "none"
                  }
                >
                  <Link
                    href={`?${queryString({ ...query, sort: sort === `-${key}` ? key : `-${key}` })}`}
                  >
                    {label}
                    {sort.replace(/^-/, "") === key
                      ? sort.startsWith("-")
                        ? " ↓"
                        : " ↑"
                      : ""}
                  </Link>
                </th>
              ),
            )}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.slug}>
              <th>
                <Link href={`/team/${row.slug}?${queryString(window.query)}`}>
                  {row.person}
                </Link>
              </th>
              {(Object.keys(teamColumns) as TeamKey[]).map((key) => (
                <td key={key}>{row[key]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </article>
  );
}
export default async function Team({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams,
    window = timeWindow(search),
    everyone = param(search, "everyone") === "1";
  return (
    <>
      <Heading
        title="Team"
        detail="Delivery metrics across engineering. Select a person for their current work and comparisons."
      />
      <WindowControls window={window} search={search} />
      <div className="toolbar">
        <Link
          href={`?${queryString({ ...window.query, sort: param(search, "sort") || "-prs_merged", everyone: everyone ? "0" : "1" })}`}
        >
          {everyone ? "Engineering only" : "Show everyone"}
        </Link>
        <a
          href={`/team.csv?${queryString({ ...window.query, sort: param(search, "sort") || "-prs_merged", everyone: everyone ? "1" : "0" })}`}
        >
          Export CSV
        </a>
      </div>
      <Suspense fallback={<Loading />}>
        <TeamTable window={window} search={search} />
      </Suspense>
    </>
  );
}
