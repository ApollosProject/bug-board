import Link from "next/link";
export default function NotFound() {
  return (
    <article>
      <h1>Page not found</h1>
      <Link href="/">Back to Bug Board</Link>
    </article>
  );
}
