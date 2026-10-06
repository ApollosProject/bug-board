import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { SignJWT, jwtVerify } from "jose";
import { cookies } from "next/headers";
import { NextResponse } from "next/server";
export const COOKIE = "bug-board-session";
export const FLOW_COOKIE = "bug-board-oauth";
export const org = () => process.env.GITHUB_OAUTH_ORG || "ApollosProject";
export function oauthEnabled() {
  return (
    ["production", "preview"].includes(process.env.VERCEL_ENV || "") ||
    /^(true|1|yes|on)$/i.test(process.env.GITHUB_OAUTH_ENABLED || "") ||
    !!(
      process.env.GITHUB_OAUTH_CLIENT_ID ||
      process.env.GITHUB_OAUTH_CLIENT_SECRET
    )
  );
}
export const callbackURL = () =>
  process.env.GITHUB_OAUTH_CALLBACK_URL ||
  (process.env.APP_URL
    ? `${process.env.APP_URL.replace(/\/$/, "")}/auth/github/callback`
    : "");
export function authConfigured() {
  try {
    const url = new URL(callbackURL());
    return (
      !!process.env.GITHUB_OAUTH_CLIENT_ID &&
      !!process.env.GITHUB_OAUTH_CLIENT_SECRET &&
      (process.env.AUTH_SECRET?.length || 0) >= 32 &&
      /^[A-Za-z0-9][A-Za-z0-9-]{0,38}$/.test(org()) &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      (url.protocol === "https:" ||
        (!["production", "preview"].includes(process.env.VERCEL_ENV || "") &&
          url.protocol === "http:" &&
          ["localhost", "127.0.0.1"].includes(url.hostname)))
    );
  } catch {
    return false;
  }
}
export function safeNext(value: string | null | undefined) {
  if (
    !value?.startsWith("/") ||
    value.startsWith("//") ||
    /[\\\r\n]/.test(value)
  )
    return "/";
  const url = new URL(value, "https://local.invalid");
  return url.origin === "https://local.invalid"
    ? url.pathname + url.search
    : "/";
}
const key = () => new TextEncoder().encode(process.env.AUTH_SECRET);
export async function sign(
  payload: Record<string, unknown>,
  seconds: number,
  audience: string,
) {
  if (!authConfigured()) throw new Error("Authentication unavailable");
  return new SignJWT(payload)
    .setProtectedHeader({ alg: "HS256" })
    .setIssuer("bug-board")
    .setAudience(audience)
    .setIssuedAt()
    .setExpirationTime(`${seconds}s`)
    .sign(key());
}
export async function verify(
  token: string | undefined,
  audience = "dashboard",
) {
  if (!token || !authConfigured()) return null;
  try {
    return (
      await jwtVerify(token, key(), {
        issuer: "bug-board",
        audience,
        algorithms: ["HS256"],
      })
    ).payload;
  } catch {
    return null;
  }
}
export async function session(token?: string) {
  const payload = await verify(token ?? (await cookies()).get(COOKIE)?.value);
  return payload &&
    typeof payload.login === "string" &&
    typeof payload.userId === "number" &&
    typeof payload.org === "string" &&
    payload.org.toLowerCase() === org().toLowerCase()
    ? payload
    : null;
}
export const random = () => randomBytes(32).toString("base64url");
export const pkce = (verifier: string) =>
  createHash("sha256").update(verifier).digest("base64url");
export function equal(a: string, b: string) {
  const left = Buffer.from(a),
    right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}
export function setCookie(
  response: NextResponse,
  name: string,
  value: string,
  maxAge: number,
) {
  response.cookies.set(name, value, {
    httpOnly: true,
    secure: callbackURL().startsWith("https:"),
    sameSite: "lax",
    path: "/",
    maxAge,
  });
}
export function apiKeyError(request: Request) {
  const configured = process.env.BUG_BOARD_API_KEY?.trim();
  if (!configured)
    return Response.json(
      {
        error: "api_key_not_configured",
        detail: "Set BUG_BOARD_API_KEY to enable the JSON API.",
      },
      { status: 503 },
    );
  const authorization = request.headers.get("authorization")?.trim() || "";
  const presented =
    /^bearer\s+(.+)$/i.exec(authorization)?.[1]?.trim() ||
    request.headers.get("x-api-key")?.trim() ||
    "";
  if (!equal(presented, configured))
    return Response.json(
      { error: "unauthorized" },
      {
        status: 401,
        headers: { "WWW-Authenticate": 'Bearer realm="bug-board"' },
      },
    );
}
export function sameOrigin(request: Request) {
  const origin = request.headers.get("origin");
  try {
    return !!origin && origin === new URL(callbackURL() || request.url).origin;
  } catch {
    return false;
  }
}
