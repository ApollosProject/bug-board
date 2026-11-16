import { Redis } from "@upstash/redis";
import { randomUUID } from "node:crypto";
export const redis = () => {
  const prefix =
    process.env.UPSTASH_REDIS_REST_URL || process.env.UPSTASH_REDIS_REST_TOKEN
      ? "UPSTASH_REDIS_REST"
      : "KV_REST_API";
  const url = process.env[`${prefix}_URL`],
    token = process.env[`${prefix}_TOKEN`];
  return url && token ? new Redis({ url, token }) : null;
};
// Preview and production never share refresh locks or snapshots.
const key = (name: string) =>
  `bug-board:${process.env.VERCEL_ENV || "local"}:${process.env.VERCEL_ENV === "preview" ? process.env.VERCEL_URL || "preview" : "shared"}:v1:${name}`;
export async function readSnapshot<T>(
  name: string,
  maxAge: number,
): Promise<T | null> {
  try {
    const record = await redis()?.get<{ at: number; data: T }>(key(name));
    return record && Date.now() - record.at <= maxAge * 1000
      ? record.data
      : null;
  } catch {
    console.warn("Snapshot read unavailable:", name);
    return null;
  }
}
// Upstash rejects a request body over 10MB. This is the pipeline JSON the client
// posts for one SET, which is larger than the snapshot itself.
const MAX_REDIS_REQUEST_BYTES = 9_000_000;

function setRequestBytes(name: string, data: unknown, ttl: number, nx = false) {
  const command = [
    "set",
    key(name),
    JSON.stringify({ at: Date.now(), data }),
    ...(nx ? ["nx"] : []),
    "ex",
    ttl,
  ];
  return new TextEncoder().encode(JSON.stringify([command])).byteLength;
}

function assertFits(name: string, data: unknown, ttl: number, nx = false) {
  const bytes = setRequestBytes(name, data, ttl, nx);
  if (bytes <= MAX_REDIS_REQUEST_BYTES) return;
  console.warn(
    "Redis snapshot skipped: request would exceed Upstash's 10MB limit",
    name,
    bytes,
  );
  throw new Error(`Redis snapshot exceeds the request size limit (${bytes} bytes)`);
}

export async function writeSnapshot<T>(name: string, data: T, ttl: number) {
  const client = redis();
  if (!client) throw new Error("Configure Upstash Redis REST credentials");
  assertFits(name, data, ttl);
  await client.set(key(name), { at: Date.now(), data }, { ex: ttl });
}
export async function snapshotOnce<T>(name: string, data: T, ttl: number): Promise<T> {
  const client = redis();
  if (!client) throw new Error("Configure Upstash Redis REST credentials");
  assertFits(name, data, ttl, true);
  await client.set(key(name), { at: Date.now(), data }, { nx: true, ex: ttl });
  const saved = await client.get<{ data: T }>(key(name));
  if (!saved) throw new Error("Saved snapshot unavailable");
  return saved.data;
}
export async function claim(name: string, ttl: number) {
  const client = redis();
  if (!client) throw new Error("Configure Upstash Redis REST credentials");
  const token = randomUUID();
  return (await client.set(key(name), token, { nx: true, ex: ttl }))
    ? token
    : null;
}
export async function release(name: string, token: string) {
  await redis()?.eval(
    "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) end return 0",
    [key(name)],
    [token],
  );
}
export async function unknownCount(reset: boolean) {
  const client = redis();
  if (!client) throw new Error("Configure Upstash Redis REST credentials");
  if (reset) {
    await client.set(key("fleet-unknowns"), 0);
    return 0;
  }
  return client.incr(key("fleet-unknowns"));
}
