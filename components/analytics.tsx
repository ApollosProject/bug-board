"use client";
import { Analytics } from "@vercel/analytics/next";
// Person and church slugs are names; send the route pattern instead.
export function PageAnalytics() {
  return (
    <Analytics
      beforeSend={(event) => ({
        ...event,
        url: event.url.replace(/\/(team|churches)\/[^/?#]+/, "/$1/[slug]"),
      })}
    />
  );
}
