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
export async function writeSnapshot<T>(name: string, data: T, ttl: number) {
  const client = redis();
  if (!client) throw new Error("Configure Upstash Redis REST credentials");
  await client.set(key(name), { at: Date.now(), data }, { ex: ttl });
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
