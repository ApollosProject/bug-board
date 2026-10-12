import Link from "next/link";
import { Heading, TableScroll, Unavailable } from "@/components/dashboard";
import { churches, matchChurches } from "@/lib/churches";
import { param } from "@/lib/window";
import type { Search } from "@/lib/types";
export const metadata = { title: "Churches" };
export default async function Churches({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const query = param(await searchParams, "q");
  const list = await churches().catch(() => null);
  const rows = matchChurches(list || [], query);
  return (
    <>
      <Heading
        title="Churches"
        detail="Look up a church's app versions, deep-link domain, store links, giving setup, DAGs, and open issues."
      />
      <form method="get" className="filter-form">
        <label>
          Search churches
          <input
            type="search"
            name="q"
            placeholder="Name or slug"
            defaultValue={query}
          />
        </label>
        <button type="submit">Search</button>
      </form>
      {!list ? (
        <Unavailable
          name="Church directory"
          detail="The church directory requires APOLLOS_API_KEY and the scheduled apps refresh."
        />
      ) : (
        <TableScroll label="Churches">
          <table>
            <caption>
              {rows.length} of {list.length} churches
            </caption>
            <thead>
              <tr>
                <th>Church</th>
                <th>Deep-link domain</th>
                <th>Giving</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 100).map((c) => (
                <tr key={c.slug}>
                  <th>
                    <Link href={`/churches/${encodeURIComponent(c.slug)}`}>
                      {c.name || c.slug}
                    </Link>
                    <small>{c.slug}</small>
                  </th>
                  <td>
                    <code>
                      {c.universalLinkHost?.replace(/^https?:\/\//, "") || "—"}
                    </code>
                  </td>
                  <td>
                    {c.givingInMaintenance
                      ? "Maintenance"
                      : c.givingEnabled
                        ? "Enabled"
                        : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length > 100 && (
            <p>Showing the first 100; refine the search.</p>
          )}
          {!rows.length && <p>No matching churches.</p>}
        </TableScroll>
      )}
    </>
  );
}
