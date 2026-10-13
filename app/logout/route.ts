import { NextRequest, NextResponse } from "next/server";
import {
  appOrigin,
  COOKIE,
  FLOW_COOKIE,
  sameOrigin,
  setCookie,
} from "@/lib/auth";
export async function POST(request: NextRequest) {
  if (!sameOrigin(request))
    return new NextResponse("Invalid origin", { status: 403 });
  const response = NextResponse.redirect(
    new URL("/signin", appOrigin(request)!),
    303,
  );
  setCookie(response, COOKIE, "", 0);
  setCookie(response, FLOW_COOKIE, "", 0);
  return response;
}
