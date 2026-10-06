export class UpstreamError extends Error {
  constructor(
    public service: string,
    public status: number,
  ) {
    super(`${service} unavailable (HTTP ${status})`);
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
  if (!response.ok) throw new UpstreamError(service, response.status);
  return response.json() as Promise<T>;
}
export async function graphql<T>(
  url: string,
  token: string,
  query: string,
  variables: object = {},
) {
  if (!token) throw new Error("Integration not configured");
  const payload = await requestJson<{ data: T; errors?: unknown[] }>(
    url,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: token },
      body: JSON.stringify({ query, variables }),
    },
    url.includes("linear") ? "Linear" : "GitHub",
  );
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
