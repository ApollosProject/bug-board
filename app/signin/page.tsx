import Link from "next/link";
import { org, safeNext } from "@/lib/auth";
import type { Search } from "@/lib/types";
import { param } from "@/lib/window";
export default async function SignIn({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const search = await searchParams;
  return (
    <article className="auth-card">
      <h1>Sign in</h1>
      <p>Sign in with GitHub to open Bug Board.</p>
      <Link
        className="button"
        href={`/login?next=${encodeURIComponent(safeNext(param(search, "next")))}`}
      >
        Sign in with GitHub
      </Link>
      <p>Active {org()} organization membership required.</p>
    </article>
  );
}
