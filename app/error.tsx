"use client";
import Link from "next/link";
export default function ErrorPage({ reset }: { reset: () => void }) {
  return (
    <article role="alert">
      <h1>Unable to load this view</h1>
      <p>Check the selected dates (maximum 366 days), then try again.</p>
      <button onClick={reset}>Try again</button>
      <Link href="/">Back to Bug Board</Link>
    </article>
  );
}
