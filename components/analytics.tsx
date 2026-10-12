"use client";
import { Analytics } from "@vercel/analytics/next";
// Person and church slugs, and the search and people filters, carry names.
export function PageAnalytics() {
  return (
    <Analytics
      beforeSend={(event) => {
        const url = new URL(event.url);
        url.pathname = url.pathname.replace(
          /\/(team|churches)\/[^/]+/,
          "/$1/[slug]",
        );
        for (const key of ["q", "author", "reviewer"])
          if (url.searchParams.has(key))
            url.searchParams.set(key, "[redacted]");
        return { ...event, url: url.toString() };
      }}
    />
  );
}
