import { engineers, people, personName, slugFor } from "./config";
import {
  openWork,
  computeReport,
  projectDashboard,
  daysSince,
} from "./reports";
import {
  comparisons,
  inactive,
  isPriorityBug,
  metricLabels,
  personMetrics,
  platformFor,
  slaText,
  supportSlugs,
  type MetricKey,
} from "./metrics";
import { DAY, date, timeWindow } from "./window";
import { claim, release } from "./cache";
import type { Issue } from "./types";
const escape = (s: string) =>
  s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/[|`]/g, "-");
const link = (url: string, label: string) => `<${url}|${escape(label)}>`;
const mention = (slug: string | undefined) =>
  slug && people[slug]?.slack_id
    ? `<@${people[slug].slack_id}>`
    : "No Assignee";
function bugLine(issue: Issue) {
  const platform = platformFor(issue),
    assignee = slugFor(issue.assignee?.name, issue.assignee?.displayName);
  return `- ${issue.priority === 1 ? "🚨 " : ""}${link(issue.url, issue.title)} (${slaText(issue) || `+${daysSince(issue.createdAt)}d`}${platform ? `, ${platform}` : ""}${assignee ? `, ${mention(assignee)}` : ""})`;
}
export async function priorityMessage() {
  const bugs = (await openWork()).filter(
    (i) =>
      !i.project &&
      isPriorityBug(i) &&
      [
        i.slaType,
        i.slaStartedAt,
        i.slaMediumRiskAt,
        i.slaHighRiskAt,
        i.slaBreachesAt,
      ].some(Boolean),
  );
  const unassigned = bugs.filter((b) => !b.assignee),
    overdue = bugs.filter(
      (b) => Date.parse(b.slaBreachesAt || "") <= Date.now(),
    );
  const atRisk = bugs.filter(
    (b) =>
      !overdue.includes(b) && Date.parse(b.slaHighRiskAt || "") <= Date.now(),
  );
  const sections = [
    ["Unassigned Priority Bugs", unassigned],
    ["At Risk", atRisk],
    ["Overdue", overdue],
  ] as const;
  const text = sections
    .filter(([, items]) => items.length)
    .map(
      ([title, items]) =>
        `*${title}*\n\n${items
          .slice()
          .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
          .map(bugLine)
          .join("\n")}`,
    );
  if (unassigned.length) {
    const assigned = new Set(
      bugs.map((i) => slugFor(i.assignee?.name, i.assignee?.displayName)),
    );
    const available = supportSlugs(await projectDashboard()).filter(
      (slug) =>
        !assigned.has(slug) &&
        (!people[slug].platform_whitelist?.length ||
          unassigned.some((i) =>
            people[slug].platform_whitelist!.includes(
              platformFor(i)?.toLowerCase().replace(/ /g, "-") || "",
            ),
          )),
    );
    if (available.length)
      text[0] += `\n\nAvailable for Support\n\n${available.map(mention).join("\n")}`;
  }
  return text.length
    ? `${text.join("\n\n")}\n\n${link(process.env.APP_URL || "", "View Bug Board")}`
    : null;
}
export async function staleMessage() {
  const issues = (await openWork()).filter(
    (i) =>
      !i.project &&
      daysSince(i.updatedAt) > 21 &&
      engineers.includes(
        slugFor(i.assignee?.name, i.assignee?.displayName) || "",
      ),
  );
  if (!issues.length) return null;
  return `*Stale Open Issues*\n\n${engineers
    .flatMap((slug) => {
      const owned = issues.filter(
        (i) => slugFor(i.assignee?.name, i.assignee?.displayName) === slug,
      );
      return owned.length
        ? [
            `${mention(slug)}:\n${owned.map((i) => `- ${link(i.url, i.title)} (${daysSince(i.updatedAt)}d)`).join("\n")}`,
          ]
        : [];
    })
    .join("\n\n")}\n\n${link(process.env.APP_URL || "", "View Bug Board")}`;
}
export async function projectMessage() {
  const today = date(Date.now()),
    now = Date.parse(today),
    weekday = new Date(now).getUTCDay();
  const sinceFriday = (weekday - 5 + 7) % 7 || 7,
    due = now - sinceFriday * DAY;
  const groups: Record<string, string[]> = {
    "Overdue Projects": [],
    "Projects With Overdue Updates": [],
    "Projects Ending Soon": [],
    "Projects Starting Soon": [],
  };
  for (const p of (await projectDashboard())
    .slice()
    .sort(
      (a, b) =>
        (a.targetDate || "").localeCompare(b.targetDate || "") ||
        a.name.localeCompare(b.name),
    )) {
    const slug = slugFor(p.lead?.displayName);
    if (!slug || !engineers.includes(slug)) continue;
    const line = `${link(p.url, p.name)} - Lead: ${mention(slug)}`,
      target = Date.parse(p.targetDate || ""),
      start = Date.parse(p.startDate || "");
    if (!inactive(p) && Number.isFinite(target)) {
      const days = (target - now) / DAY;
      if (days < 0)
        groups["Overdue Projects"].push(
          `- ${line} (${Math.abs(days)}d overdue)`,
        );
      else if (days <= 3)
        groups["Projects Ending Soon"].push(
          `- ${line} (${days === 0 ? "Today" : p.targetDate})`,
        );
      if (
        p.status.type === "started" &&
        (!Number.isFinite(start) || start < due) &&
        (!p.lastUpdate ||
          Date.parse(p.lastUpdate.createdAt.slice(0, 10)) < due - 2 * DAY)
      )
        groups["Projects With Overdue Updates"].push(
          `- ${line} (Due ${date(due)}; ${p.lastUpdate ? `last update ${p.lastUpdate.createdAt.slice(0, 10)}` : "no updates yet"})`,
        );
    }
    if (!inactive(p) && start > now && start <= now + 3 * DAY)
      groups["Projects Starting Soon"].push(`- ${line} (${p.startDate})`);
  }
  const sections = Object.entries(groups)
    .filter(([, rows]) => rows.length)
    .map(([title, rows]) => `*${title}*\n\n${rows.join("\n")}`);
  return sections.length ? sections.join("\n\n") : null;
}
export async function performanceMessage() {
  const report = await computeReport(timeWindow({ days: "7" }));
  const values = engineers.map((slug) =>
    personMetrics(
      slug,
      report.completed,
      report.prs,
      report.projects,
      report.window,
    ),
  );
  const praise: string[] = [],
    coach: string[] = [];
  engineers.forEach((slug, i) => {
    const compare = comparisons(values[i], values);
    for (const [tone, target] of [
      ["high", praise],
      ["low", coach],
    ] as const) {
      const metrics = (Object.keys(metricLabels) as MetricKey[])
        .filter((key) => compare[key]?.tone === tone)
        .map((key) => `${metricLabels[key]} ${compare[key]?.label}`);
      if (metrics.length)
        target.push(
          `- ${link(`${process.env.APP_URL}/team/${slug}?days=7`, personName(slug))}: ${metrics.join(", ")}`,
        );
    }
  });
  return praise.length || coach.length
    ? [
        `*Who to praise and coach (±1σ, last 7 days)*`,
        ...(praise.length ? [`*Praise*\n\n${praise.join("\n")}`] : []),
        ...(coach.length ? [`*Coach*\n\n${coach.join("\n")}`] : []),
      ].join("\n\n")
    : null;
}
export function scheduledNotifications(now = new Date()) {
  const ny = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    hourCycle: "h23",
  }).format(now);
  return [
    ...(now.getUTCHours() === 12 ? ["priority"] : []),
    ...(ny === "10" ? ["stale"] : []),
    ...(ny === "14" ? ["projects"] : []),
    ...(now.getUTCDay() === 5 && now.getUTCHours() === 13
      ? ["performance"]
      : []),
  ];
}
export async function notify(name: string, day: string) {
  const manager = name === "performance",
    url =
      process.env[manager ? "MANAGER_SLACK_WEBHOOK_URL" : "SLACK_WEBHOOK_URL"];
  if (!url || !process.env.APP_URL)
    throw new Error("Slack webhook and APP_URL required");
  const message = await (
    {
      priority: priorityMessage,
      stale: staleMessage,
      projects: projectMessage,
      performance: performanceMessage,
    } as Record<string, () => Promise<string | null>>
  )[name]();
  if (!message) return { status: "empty" };
  const key = `notification:${name}:${day}`,
    token = await claim(key, 3 * 86400);
  if (!token) return { status: "already-claimed" };
  // Slack webhooks lack idempotency keys. Keep the claim after ambiguous network errors;
  // an operator must inspect delivery before retrying, rather than duplicate a digest.
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text: message }),
    redirect: "error",
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) {
    await release(key, token);
    throw new Error(`Slack rejected notification (HTTP ${response.status})`);
  }
  return { status: "sent" };
}
