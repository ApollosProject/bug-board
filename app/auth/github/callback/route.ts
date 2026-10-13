import { NextRequest, NextResponse } from "next/server";
import {
  authConfigured,
  appOrigin,
  callbackURL,
  COOKIE,
  FLOW_COOKIE,
  equal,
  org,
  safeNext,
  setCookie,
  sign,
  verify,
} from "@/lib/auth";
import { githubHeaders } from "@/lib/github";
import { requestJson, UpstreamError } from "@/lib/http";
export async function GET(request: NextRequest) {
  const fail = (message: string, status: number) => {
    const response = new NextResponse(message, {
      status,
      headers: { "Cache-Control": "no-store" },
    });
    setCookie(response, FLOW_COOKIE, "", 0);
    setCookie(response, COOKIE, "", 0);
    return response;
  };
  if (!authConfigured()) return fail("Authentication unavailable", 503);
  const origin = appOrigin(request);
  if (!origin) return fail("Invalid preview origin", 400);
  const state = request.nextUrl.searchParams.get("state"),
    code = request.nextUrl.searchParams.get("code");
  if (process.env.VERCEL_ENV === "preview") {
    const relay = await verify(state || undefined, "oauth-preview");
    const target = typeof relay?.origin === "string" && URL.canParse(relay.origin)
      ? new URL(relay.origin) : null;
    if (!target || target.protocol !== "https:" || target.origin !== relay?.origin)
      return fail("Sign-in request expired or could not be verified.", 400);
    if (origin !== target.origin) {
      if (origin !== new URL(callbackURL()).origin)
        return fail("Invalid preview callback origin", 400);
      // The PKCE verifier stays in the initiating host's HTTP-only cookie.
      target.pathname = "/auth/github/callback";
      for (const key of ["state", "code", "error"]) {
        const value = request.nextUrl.searchParams.get(key);
        if (value !== null) target.searchParams.set(key, value);
      }
      const response = NextResponse.redirect(target);
      response.headers.set("Cache-Control", "no-store");
      return response;
    }
  }
  const flow = await verify(request.cookies.get(FLOW_COOKIE)?.value, "oauth");
  if (
    !flow ||
    typeof flow.state !== "string" ||
    typeof flow.verifier !== "string" ||
    !state ||
    !equal(state, flow.state)
  )
    return fail("Sign-in request expired or could not be verified.", 400);
  if (!code || request.nextUrl.searchParams.has("error"))
    return fail("GitHub authorization was not completed.", 403);
  try {
    const token = await requestJson<{ access_token?: string }>(
      "https://github.com/login/oauth/access_token",
      {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          client_id: process.env.GITHUB_OAUTH_CLIENT_ID,
          client_secret: process.env.GITHUB_OAUTH_CLIENT_SECRET,
          code,
          code_verifier: flow.verifier,
          redirect_uri: callbackURL(),
        }),
      },
      "GitHub OAuth",
    );
    if (!token.access_token)
      return fail("GitHub could not complete sign-in.", 502);
    const headers = githubHeaders(token.access_token);
    const user = await requestJson<{ login: string; id: number }>(
      "https://api.github.com/user",
      { headers },
      "GitHub",
    );
    const membership = await requestJson<{ state: string }>(
      `https://api.github.com/user/memberships/orgs/${encodeURIComponent(org())}`,
      { headers },
      "GitHub",
    );
    if (membership.state !== "active")
      return fail("Active organization membership required.", 403);
    if (typeof user.login !== "string" || typeof user.id !== "number")
      return fail("GitHub returned an invalid identity.", 502);
    const response = NextResponse.redirect(
      new URL(
        safeNext(typeof flow.next === "string" ? flow.next : "/"),
        origin,
      ),
    );
    setCookie(response, FLOW_COOKIE, "", 0);
    setCookie(
      response,
      COOKIE,
      await sign(
        { login: user.login, userId: user.id, org: org() },
        30 * 86400,
        "dashboard",
      ),
      30 * 86400,
    );
    response.headers.set("Cache-Control", "no-store");
    return response;
  } catch (error) {
    return fail(
      error instanceof UpstreamError && [403, 404].includes(error.status)
        ? "Access denied"
        : "Sign-in unavailable",
      error instanceof UpstreamError && [403, 404].includes(error.status)
        ? 403
        : 502,
    );
  }
}
