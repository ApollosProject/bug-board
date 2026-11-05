export class UpstreamError extends Error {
  constructor(
    public service: string,
    public status: number,
    public reason = "",
    // Seconds the service asked us to wait (Retry-After or rate-limit reset).
    public retryAfter?: number,
  ) {
    super(
      `${service} unavailable (HTTP ${status})${reason ? `: ${reason}` : ""}`,
    );
  }
}
export async function requestJson<T>(
  url: string | URL,
  init: RequestInit = {},
  service = "Upstream",
): Promise<T> {
  const response = await fetch(url, {
    ...init,
    cache: "no-store",
    redirect: "error",
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    let reason = "";
    if (service === "GitHub") {
      const body = await response.json().catch(() => ({}));
      const message = typeof body?.message === "string" ? body.message : "";
      reason = /rate.limit|abuse/i.test(message)
        ? "rate limit"
        : /user.agent/i.test(message)
          ? "User-Agent required"
          : /SSO|SAML/i.test(message)
            ? "organization SSO required"
            : "request rejected";
    }
    const after = Number(response.headers.get("retry-after"));
    const reset = Number(response.headers.get("x-ratelimit-reset"));
    throw new UpstreamError(
      service,
      response.status,
      reason,
      after > 0
        ? after
        : response.headers.get("x-ratelimit-remaining") === "0" && reset
          ? Math.max(0, reset - Math.floor(Date.now() / 1000))
          : undefined,
    );
  }
  return response.json() as Promise<T>;
}
export async function graphql<T>(
  url: string,
  token: string,
  query: string,
  variables: object = {},
) {
  if (!token) throw new Error("Integration not configured");
  const request = () =>
    requestJson<{ data: T; errors?: unknown[] }>(
      url,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: token },
        body: JSON.stringify({ query, variables }),
      },
      url.includes("linear") ? "Linear" : "GitHub",
    );
  // GitHub search intermittently 502s on large pages; every query here is a read.
  let payload;
  for (let attempt = 1; ; attempt++) {
    try {
      payload = await request();
      break;
    } catch (error) {
      if (
        attempt === 3 ||
        !(error instanceof UpstreamError) ||
        ![502, 503, 504].includes(error.status)
      )
        throw error;
      await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
    }
  }
  if (payload.errors?.length || !payload.data)
    throw new Error("GraphQL query unavailable");
  return payload.data;
}
export async function mapConcurrent<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const output: R[] = new Array(items.length);
  let cursor = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (cursor < items.length) {
        const i = cursor++;
        output[i] = await fn(items[i], i);
      }
    }),
  );
  return output;
}
