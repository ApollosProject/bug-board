import { NextRequest, NextResponse } from "next/server";
import { authConfigured, oauthEnabled, session, COOKIE } from "./lib/auth";
import { refreshJobs } from "./lib/types";
export async function proxy(request: NextRequest) {
  const path = request.nextUrl.pathname;
  const publicRoute = [
    "/healthz",
    "/login",
    "/logout",
    "/auth/github/callback",
    "/signin",
    "/static/brand-mark.svg",
    "/static/og-image.png",
  ].includes(path);
  const apiRoute =
    /^\/api\/team\/[^/]+$/.test(path) ||
    refreshJobs.some((job) => path === `/api/cron/${job}`);
  if (publicRoute || apiRoute || !oauthEnabled()) return NextResponse.next();
  if (!authConfigured())
    return new NextResponse(
      "Authentication unavailable. Configure GitHub OAuth and AUTH_SECRET.",
      { status: 503, headers: { "Cache-Control": "private, no-store" } },
    );
  if (await session(request.cookies.get(COOKIE)?.value || "")) {
    const response = NextResponse.next();
    response.headers.set("Cache-Control", "private, no-store");
    return response;
  }
  const url = request.nextUrl.clone();
  url.pathname = "/signin";
  url.search = new URLSearchParams({
    next: path + request.nextUrl.search,
  }).toString();
  const response = NextResponse.rewrite(url);
  response.headers.set("Cache-Control", "private, no-store");
  return response;
}
export const config = {
  matcher: ["/((?!_next/static|_next/image|.well-known/workflow/).*)"],
};
