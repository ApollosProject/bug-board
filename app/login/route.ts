import { NextRequest, NextResponse } from "next/server";
import {
  authConfigured,
  appOrigin,
  callbackURL,
  COOKIE,
  FLOW_COOKIE,
  pkce,
  random,
  safeNext,
  setCookie,
  sign,
} from "@/lib/auth";
export async function GET(request: NextRequest) {
  if (!authConfigured())
    return new NextResponse("Authentication unavailable", { status: 503 });
  const origin = appOrigin(request);
  if (!origin) return new NextResponse("Invalid preview origin", { status: 400 });
  const state = process.env.VERCEL_ENV === "preview"
    ? await sign({ origin, nonce: random() }, 600, "oauth-preview")
    : random();
  const verifier = random();
  const params = new URLSearchParams({
    client_id: process.env.GITHUB_OAUTH_CLIENT_ID!,
    redirect_uri: callbackURL(),
    scope: "read:org",
    state,
    code_challenge: pkce(verifier),
    code_challenge_method: "S256",
    allow_signup: "false",
    prompt: "select_account",
  });
  const response = NextResponse.redirect(
    `https://github.com/login/oauth/authorize?${params}`,
  );
  setCookie(response, COOKIE, "", 0);
  setCookie(
    response,
    FLOW_COOKIE,
    await sign(
      {
        state,
        verifier,
        next: safeNext(request.nextUrl.searchParams.get("next")),
      },
      600,
      "oauth",
    ),
    600,
  );
  response.headers.set("Cache-Control", "no-store");
  return response;
}
