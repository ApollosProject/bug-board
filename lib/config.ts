import fs from "node:fs";
import path from "node:path";
import YAML from "yaml";
export type Person = {
  team: string;
  slack_id: string;
  linear_username: string;
  github_username: string;
  linear_display_name?: string;
  platform_whitelist?: string[];
};
type Config = {
  linear_team_key: string;
  github_orgs: string[];
  people: Record<string, Person>;
  platforms: Record<string, { lead?: string; developers: string[] }>;
};
export const config = YAML.parse(
  fs.readFileSync(path.join(process.cwd(), "config.yml"), "utf8"),
) as Config;
export const people = config.people;
export const engineers = Object.keys(people).filter(
  (slug) => people[slug].team === "engineering",
);
export const normalize = (s = "") => s.toLowerCase().replace(/[^a-z0-9]/g, "");
export const displayName = (s: string) =>
  s.replace(/[._-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
export const personName = (slug: string) =>
  displayName(people[slug]?.linear_username || slug);
export function slugFor(...names: (string | undefined)[]) {
  return Object.keys(people).find((slug) =>
    names.some(
      (name) =>
        name &&
        [
          slug,
          people[slug].linear_username,
          people[slug].linear_display_name,
          people[slug].github_username,
        ].some((alias) => alias && normalize(alias) === normalize(name)),
    ),
  );
}
export const priorityLabels: Record<number, string> = {
  0: "No priority",
  1: "Urgent",
  2: "High",
  3: "Medium",
  4: "Low",
  5: "Very Low",
};
export const priorityPoints: Record<number, number> = {
  1: 20,
  2: 10,
  3: 5,
  4: 1,
  5: 1,
};
