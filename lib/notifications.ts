import { setTimeout } from "node:timers/promises";
import { engineers, people, personName, slugFor } from "./config";
import {
  openWork,
  computeReport,
  projectDashboard,
  daysSince,
  type Report,
} from "./reports";
import {
  comparisons,
  inactive,
  isPriorityBug,
  metricLabels,
  metricDisplay,
  personMetrics,
  platformFor,
  slaText,
  supportSlugs,
  type MetricKey,
  type Metrics,
} from "./metrics";
import { DAY, date, timeWindow } from "./window";
import { claim, readSnapshot, release, snapshotOnce } from "./cache";
import { activeChangeRequests, ciState, reviewQueue, type ReviewRow } from "./reviews";
import type { Issue, PullRequest } from "./types";
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
function bugLine(issue: Issue, reviewers: string[]) {
  const platform = platformFor(issue),
    slug = slugFor(issue.assignee?.name, issue.assignee?.displayName),
    assignee = slug && people[slug].slack_id
      ? mention(slug)
      : escape(issue.assignee?.displayName || issue.assignee?.name || "");
  return `- ${issue.priority === 1 ? "🚨 " : ""}${link(issue.url, issue.title)} (${slaText(issue) || `+${daysSince(issue.createdAt)}d`}${platform ? `, ${platform}` : ""}${assignee ? `, ${assignee}` : ""}${reviewers.length ? `, Review requested: ${reviewers.map(mention).join(", ")}` : ""})`;
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
  const waiting = new Map<string, Set<string>>();
  if (bugs.length) {
    for (const { issue, slug } of waitingReviews(await reviewQueue(true))) {
      if (!waiting.has(issue.id)) waiting.set(issue.id, new Set());
      waiting.get(issue.id)!.add(slug);
    }
  }
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
          .map((issue) => bugLine(issue, [...(waiting.get(issue.id) || [])]))
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
// The old review reminder's gates, with Urgent/High Linear priority replacing its 200-line cap.
function waitingReviews(rows: ReviewRow[], now = Date.now()) {
  const waiting: { pr: PullRequest; issue: Issue; slug: string; days: number }[] = [];
  for (const { pr, issue } of rows) {
    if (
      !issue ||
      ![1, 2].includes(issue.priority) ||
      !pr.baseRefName ||
      pr.baseRefName !== pr.repository.defaultBranchRef?.name ||
      pr.mergeable === "CONFLICTING" ||
      pr.reviewDecision === "APPROVED" ||
      !["none", "passing"].includes(ciState(pr))
    )
      continue;
    const requested = new Set(
      pr.reviewRequests.nodes
        .map((r) => r.requestedReviewer?.login?.toLowerCase())
        .filter((login): login is string => !!login),
    );
    const active = activeChangeRequests(pr);
    const reviewers =
      pr.reviewDecision === "CHANGES_REQUESTED" || active.length
        ? active.filter((login) => requested.has(login))
        : [...requested];
    for (const login of reviewers) {
      const requestedAt = Math.max(
        ...(pr.timelineItems?.nodes
          .filter((n) => n.requestedReviewer?.login?.toLowerCase() === login)
          .map((n) => Date.parse(n.createdAt))
          .filter(Number.isFinite) || []),
      );
      if (!Number.isFinite(requestedAt) || now - requestedAt <= DAY) continue;
      const latest = pr.reviews.nodes
        .filter(
          (r) =>
            r.author?.login?.toLowerCase() === login &&
            ["APPROVED", "CHANGES_REQUESTED", "DISMISSED"].includes(r.state) &&
            Date.parse(r.submittedAt || "") >= requestedAt,
        )
        .sort((a, b) => Date.parse(b.submittedAt!) - Date.parse(a.submittedAt!))[0];
      const slug = engineers.find(
        (s) => people[s].github_username.toLowerCase() === login,
      );
      if (latest?.state === "APPROVED" || !slug) continue;
      waiting.push({ pr, issue, slug, days: Math.floor((now - requestedAt) / DAY) });
    }
  }
  return waiting;
}
export function reviewReminders(rows: ReviewRow[], now = Date.now(), metrics: Record<string, Metrics> = {}) {
  const waiting = waitingReviews(rows, now);
  const sections = engineers.flatMap((slug) => {
    const prs = waiting.filter((review) => review.slug === slug);
    return prs.length
      ? [
          `${mention(slug)} (avg review decision: ${metricDisplay("avg_review_decision_time", metrics[slug]?.avg_review_decision_time ?? null)}, last 30d):\n${prs
            .sort((a, b) => b.days - a.days)
            .map(({ pr, days }) => `- ${link(pr.url, pr.title)} (+${days}d)`)
            .join("\n")}`,
        ]
      : [];
  });
  return sections.length
    ? `*PRs - Checks Passing, Waiting for Review (+24h, Urgent/High priority)*\n\n${sections.join("\n\n")}`
    : null;
}
export async function staleMessage() {
  // GitHub's -review:approved search also drops PRs with one approval and another pending review.
  const [queue, open, report] = await Promise.all([
    reviewQueue(true), openWork(), readSnapshot<Report>("metrics", 180),
  ]);
  const metrics = report ? Object.fromEntries(engineers.map((slug) => [slug,
    personMetrics(slug, report.completed, report.prs, report.projects, report.window),
  ])) : {};
  const reviews = reviewReminders(queue, Date.now(), metrics);
  const issues = open.filter(
    (i) =>
      daysSince(i.updatedAt) > 21 &&
      engineers.includes(
        slugFor(i.assignee?.name, i.assignee?.displayName) || "",
      ),
  );
  const stale = issues.length ? `*Stale Open Issues*\n\n${engineers
    .flatMap((slug) => {
      const owned = issues.filter(
        (i) => slugFor(i.assignee?.name, i.assignee?.displayName) === slug,
      );
      return owned.length
        ? [
            `${mention(slug)} (avg completion: ${metricDisplay("avg_all_time_to_fix", metrics[slug]?.avg_all_time_to_fix ?? null)}, last 30d):\n${owned.map((i) => `- ${link(i.url, i.title)} (${daysSince(i.updatedAt)}d)`).join("\n")}`,
          ]
        : [];
    })
    .join("\n\n")}` : null;
  const sections = [reviews, stale].filter(Boolean);
  return sections.length
    ? `${sections.join("\n\n")}\n\n${link(process.env.APP_URL || "", "View Bug Board")}`
    : null;
}
export async function projectMessage() {
  const today = date(Date.now()),
    now = Date.parse(today),
    weekday = new Date(now).getUTCDay();
  const formatDate = (value: string) => {
    const day = Date.parse(value.slice(0, 10));
    if (day === now) return "Today";
    return Math.abs(day - now) < 7 * DAY
      ? new Date(day).toLocaleDateString("en-US", {
          weekday: "short",
          timeZone: "UTC",
        })
      : value.slice(0, 10);
  };
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
          `- ${line} (${formatDate(date(target))})`,
        );
      if (
        p.status.type === "started" &&
        (!Number.isFinite(start) || start < due) &&
        (!p.lastUpdate ||
          Date.parse(p.lastUpdate.createdAt.slice(0, 10)) < due - 2 * DAY)
      )
        groups["Projects With Overdue Updates"].push(
          `- ${line} (Due ${formatDate(date(due))}; ${p.lastUpdate ? `last update ${formatDate(p.lastUpdate.createdAt)}` : "no updates yet"})`,
        );
    }
    if (!inactive(p) && start > now && start <= now + 3 * DAY)
      groups["Projects Starting Soon"].push(`- ${line} (${formatDate(date(start))})`);
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
  const savedKey = `notification-message:${name}:${day}`,
    ttl = 3 * 86400;
  let message = await readSnapshot<string>(savedKey, ttl);
  if (message === null) {
    message = await (
      {
        priority: priorityMessage,
        stale: staleMessage,
        projects: projectMessage,
        performance: performanceMessage,
      } as Record<string, () => Promise<string | null>>
    )[name]();
    if (!message) return { status: "empty" };
    // Freeze the winning digest so refreshes cannot shift part boundaries on retry.
    message = await snapshotOnce(savedKey, message, ttl);
  }
  // Slack truncates text at 40,000 characters. Split between lines to preserve links.
  const messages: string[] = [];
  let remaining = message;
  while (remaining.length > 39_000) {
    const end = remaining.lastIndexOf("\n", 39_000);
    if (end < 0) throw new Error("Slack notification line exceeds message limit");
    messages.push(remaining.slice(0, end));
    remaining = remaining.slice(end + 1);
  }
  messages.push(remaining);
  let status = "already-claimed";
  for (const [index, text] of messages.entries()) {
    // Wait before claiming so cancellation during pacing cannot suppress an unsent part.
    if (status === "sent") await setTimeout(1_000);
    const key = `notification:${name}:${day}${index ? `:part:${index + 1}` : ""}`,
      token = await claim(key, ttl);
    if (!token) continue;
    // Slack webhooks lack idempotency keys. Keep each claim after ambiguous network
    // errors; an operator must inspect delivery before retrying that part.
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) {
      await release(key, token);
      throw new Error(`Slack rejected notification (HTTP ${response.status})`);
    }
    status = "sent";
  }
  return { status };
}
