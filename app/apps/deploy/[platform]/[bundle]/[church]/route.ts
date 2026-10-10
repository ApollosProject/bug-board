import { NextRequest, NextResponse } from "next/server";
import { callbackURL, sameOrigin, session } from "@/lib/auth";
import { appsDashboard, deployTarget, dispatchDeploy } from "@/lib/apps";
import { claim } from "@/lib/cache";
export const maxDuration = 60;
export async function POST(
  request: NextRequest,
  {
    params,
  }: { params: Promise<{ platform: string; bundle: string; church: string }> },
) {
  // Deployment is never available in the unauthenticated local-rendering mode.
  if (!sameOrigin(request) || !(await session()))
    return new NextResponse(
      "Verified session and same-origin request required",
      { status: 403 },
    );
  const { platform, bundle, church } = await params;
  const data = await appsDashboard();
  if (!data || !deployTarget(data.rows, platform, bundle, church))
    return new NextResponse("Unknown or ambiguous deployment target", {
      status: 400,
    });
  try {
    if (!(await claim(`deploy:${platform}:${bundle}:${church}`, 60)))
      return new NextResponse("Deployment recently requested", { status: 409 });
    await dispatchDeploy(church, platform);
    return NextResponse.redirect(
      new URL("/apps?deployed=1", callbackURL()),
      303,
    );
  } catch {
    return new NextResponse(
      "Deployment unavailable; check GitHub before retrying",
      { status: 502 },
    );
  }
}
