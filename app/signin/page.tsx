import { redirect } from "next/navigation";
import { org, safeNext, session } from "@/lib/auth";
import type { Search } from "@/lib/types";
import { param } from "@/lib/window";
export const metadata = { title: "Sign in" };
export default async function SignIn({
  searchParams,
}: {
  searchParams: Promise<Search>;
}) {
  const next = safeNext(param(await searchParams, "next"));
  if (await session()) redirect(next);
  return (
    <article className="auth-card">
      <h1>Sign in</h1>
      <p>Sign in with GitHub to open Bug Board.</p>
      {/* A plain link: prefetching /login would end the current session. */}
      <a className="button" href={`/login?next=${encodeURIComponent(next)}`}>
        Sign in with GitHub
      </a>
      <p>Active {org()} organization membership required.</p>
    </article>
  );
}
