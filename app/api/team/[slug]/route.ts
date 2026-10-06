import { people } from "@/lib/config";
import { apiKeyError } from "@/lib/auth";
import { personPayload } from "@/lib/reports";
import { timeWindow } from "@/lib/window";
export const maxDuration = 60;
export async function GET(
  request: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const headers = { "Cache-Control": "private, no-store" };
  const auth = apiKeyError(request);
  if (auth) {
    auth.headers.set("Cache-Control", "no-store");
    return auth;
  }
  const { slug } = await params;
  if (!Object.hasOwn(people, slug))
    return Response.json(
      { error: "unknown_person", slug },
      { status: 404, headers },
    );
  let window;
  try {
    window = timeWindow(Object.fromEntries(new URL(request.url).searchParams));
  } catch {
    return Response.json({ error: "invalid_window" }, { status: 400, headers });
  }
  try {
    return Response.json(await personPayload(slug, window), { headers });
  } catch {
    return Response.json(
      {
        error: "metrics_unavailable",
        detail: "Integration data is unavailable or refreshing.",
      },
      { status: 503, headers },
    );
  }
}
