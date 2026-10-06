import { Suspense } from "react";
import Link from "next/link";
import { unstable_cache } from "next/cache";
import { Heading, Loading, Unavailable } from "@/components/dashboard";
import { session } from "@/lib/auth";
import { people, personName, priorityLabels } from "@/lib/config";
import { reviewQueue } from "@/lib/reviews";
import { param, queryString } from "@/lib/window";
import type { Search } from "@/lib/types";
export const maxDuration = 300;
const loginFor = (value: string) => people[value]?.github_username || value;
async function Queue({ search }: { search: Search }) {
  const approved = param(search, "approved") === "1",
    author = loginFor(param(search, "author")).toLowerCase(),
    reviewer = loginFor(param(search, "reviewer")).toLowerCase();
  const queue = await unstable_cache(
    () => reviewQueue(approved),
    ["reviews-v1", String(approved)],
    { revalidate: 60 },
  )().catch(() => null);
  if (!queue)
    return (
      <Unavailable
        name="Review queue"
        detail="The complete review queue requires GitHub and Linear credentials. Partial results are not presented as ready."
      />
    );
  const rows = queue.filter(
    (row) =>
      (!author || row.pr.author?.login?.toLowerCase() === author) &&
      (!reviewer ||
        row.reviewers.some((name) => name.toLowerCase() === reviewer)),
  );
  return [
    "ready",
    "running",
    "not_ready",
    ...(approved ? ["approved"] : []),
  ].map((section) => {
    const items = rows.filter((row) => row.section === section);
    const title = (
      {
        ready: "Ready for Review",
        running: "CI Running",
        not_ready: "Not Ready",
        approved: "Approved",
      } as Record<string, string>
    )[section];
    return (
      <section key={section}>
        <h2>
          {title} <span className="badge">{items.length}</span>
        </h2>
        <article className="table-scroll">
          {items.length ? (
            <table>
              <thead>
                <tr>
                  <th>PR</th>
                  <th>Author</th>
                  <th>Priority</th>
                  <th>Size</th>
                  <th>Waiting</th>
                  <th>CI / readiness</th>
                  <th>Reviewers</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row) => (
                  <tr key={row.pr.id}>
                    <td>
                      <a href={row.pr.url}>{row.pr.title}</a>
                      <small>
                        {row.pr.repository.nameWithOwner.split("/")[1]}#
                        {row.pr.number}
                      </small>
                    </td>
                    <td>{row.pr.author?.login}</td>
                    <td>
                      {row.issue ? (
                        <a href={row.issue.url}>
                          {priorityLabels[row.priority]}
                        </a>
                      ) : (
                        "No priority"
                      )}
                    </td>
                    <td title={`+${row.pr.additions} −${row.pr.deletions}`}>
                      {row.size}
                    </td>
                    <td>{row.waiting}</td>
                    <td>{row.reason || row.ci}</td>
                    <td>{row.reviewers.join(", ") || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p>No PRs in this section.</p>
          )}
        </article>
      </section>
    );
  });
}
export default async function Reviews({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams,
    approved = param(search, "approved") === "1",
    viewer = await session();
  const query = Object.fromEntries(
    ["author", "reviewer", "approved"]
      .map((key) => [key, param(search, key)])
      .filter(([, value]) => value),
  );
  return (
    <>
      <Heading
        title="Reviews"
        detail="Review priority first, then quick wins and longest waiting. Stacks, conflicts, and failing checks stay out of ready."
      />
      <form method="get" className="filter-form">
        <label>
          Author
          <select name="author" defaultValue={param(search, "author")}>
            <option value="">Everyone</option>
            {Object.keys(people)
              .sort()
              .map((slug) => (
                <option key={slug} value={slug}>
                  {personName(slug)}
                </option>
              ))}
          </select>
        </label>
        <label>
          Reviewer
          <select name="reviewer" defaultValue={param(search, "reviewer")}>
            <option value="">Anyone</option>
            {Object.keys(people)
              .sort()
              .map((slug) => (
                <option key={slug} value={slug}>
                  {personName(slug)}
                </option>
              ))}
          </select>
        </label>
        <label className="checkbox">
          <input
            type="checkbox"
            name="approved"
            value="1"
            defaultChecked={approved}
          />
          Include approved
        </label>
        <button type="submit">Filter reviews</button>
        <Link href="/reviews">Reset</Link>
        {viewer && (
          <Link
            href={`?${queryString({ ...query, reviewer: String(viewer.login) })}`}
          >
            For me
          </Link>
        )}
      </form>
      <Suspense fallback={<Loading />}>
        <Queue search={search} />
      </Suspense>
    </>
  );
}
