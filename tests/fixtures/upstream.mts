// Public-safe service double for `npm run start:fixtures`: answers Linear, GitHub,
// Airflow, the PTO feed, and Upstash Redis with fake data relative to now, and
// blocks every other outbound request. Production code runs unchanged.
if (process.env.VERCEL || process.env.VERCEL_ENV) throw new Error("Fixtures never run on Vercel");
// Set before Next loads .env files, which never override existing values.
Object.assign(process.env, {
  BUG_BOARD_CONFIG: "tests/fixtures/config.yml",
  GITHUB_OAUTH_ENABLED: "false",
  LINEAR_API_KEY: "fixture",
  GITHUB_TOKEN: "fixture",
  UPSTASH_REDIS_REST_URL: "https://redis.fixture.invalid",
  UPSTASH_REDIS_REST_TOKEN: "fixture",
  AIRFLOW_API_BASE_URL: "https://airflow.fixture.invalid/api/v1",
  AIRFLOW_API_TOKEN: "fixture",
  CRON_SECRET: "fixture",
  RIPPLING_PTO_CALENDAR_URL: "https://app.rippling.com/api/feed/calendar/pto/fixture",
  ...Object.fromEntries([
    "AIRFLOW_FLEET_HEARTBEAT_URL", "BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64", "APOLLOS_API_KEY",
    "GITHUB_ACTIONS_TOKEN", "SLACK_WEBHOOK_URL", "MANAGER_SLACK_WEBHOOK_URL", "BUG_BOARD_API_KEY",
  ].map((name) => [name, ""])),
});

export {};
type Json = Record<string, unknown>;
const DAY = 86_400_000, now = Date.now();
const iso = (ms: number) => new Date(ms).toISOString();
const ago = (days: number) => iso(now - days * DAY);
const day = (days: number) => ago(days).slice(0, 10);
const page = <T,>(nodes: T[]) => ({ nodes, pageInfo: { hasNextPage: false, endCursor: null } });
const people = [
  ["Avery Example", "avery.example", "avery-example"],
  ["Blake Sample", "blake.sample", "blake-sample"],
  ["Casey Demo", "casey.demo", "casey-demo"],
  ["Drew Placeholder", "drew.placeholder", "drew-placeholder"],
  ["Emery Fixture", "emery.fixture", "emery-fixture"],
];
const assignee = (i: number) => ({ name: people[i][0], displayName: people[i][1] });
const login = (i: number) => people[i % people.length][2];

const states = {
  triage: { id: "s-triage", name: "Triage", type: "triage" },
  backlog: { id: "s-backlog", name: "Backlog", type: "backlog" },
  todo: { id: "s-todo", name: "Todo", type: "unstarted" },
  progress: { id: "s-progress", name: "In Progress", type: "started" },
  review: { id: "s-review", name: "In Review", type: "started" },
  done: { id: "s-done", name: "Done", type: "completed" },
};
type State = keyof typeof states;
const paths: Record<State, State[]> = {
  triage: ["triage"],
  backlog: ["triage", "backlog"],
  todo: ["triage", "todo"],
  progress: ["triage", "todo", "progress"],
  review: ["triage", "todo", "progress", "review"],
  done: ["triage", "todo", "progress", "review", "done"],
};

const project = (id: string, name: string, status: [string, string], extra: Json = {}) => ({
  id, name, url: `https://linear.app/fixture/project/${id}`, health: "onTrack",
  priorityLabel: "Medium", status: { name: status[0], type: status[1] },
  lastUpdate: { createdAt: ago(3) }, lead: null, members: page([]),
  initiatives: page([{ id: "init-1", name: "Fixture initiative" }]),
  inverseRelations: page([]), ...extra,
});
const projects = [
  project("p1", "Checkout redesign", ["In Progress", "started"], {
    priorityLabel: "High", startDate: day(20), targetDate: day(-15),
    lead: { displayName: people[0][1] }, members: page([{ displayName: people[1][1] }]),
  }),
  project("p2", "Offline sync", ["In Progress", "started"], {
    health: "atRisk", startDate: day(40), targetDate: day(3),
    lead: { displayName: people[1][1] }, members: page([{ displayName: people[2][1] }]),
  }),
  project("p3", "A deliberately long project name that keeps going to test how timelines and tables handle overflow", ["In Progress", "started"], {
    health: "offTrack", priorityLabel: "Urgent", startDate: day(5), targetDate: day(-30),
    lead: { displayName: people[3][1] }, members: page([{ displayName: people[0][1] }]),
  }),
  project("p4", "Search relevance", ["Ready", "planned"], { priorityLabel: "Low" }),
  project("p5", "Accessibility audit", ["Ready", "planned"], {
    inverseRelations: page([{ type: "dependency", project: { status: { name: "In Progress", type: "started" } }, projectMilestone: null }]),
  }),
  project("p6", "Release tooling", ["Completed", "completed"], {
    startDate: day(50), targetDate: day(12), completedAt: ago(10),
    lead: { displayName: people[2][1] }, members: page([{ displayName: people[3][1] }]),
  }),
  project("p7", "Legacy API sunset", ["Incomplete", "canceled"], {
    startDate: day(60), targetDate: day(30), lead: { displayName: people[3][1] },
  }),
];

const titles = [
  "Crash when opening settings on older tablets", "Add a dark mode toggle to the profile screen",
  "Migrate push notifications to the new provider", "Giving page shows a blank state after refresh",
  "Support multiple campuses in the event filter", "Upgrade the video player SDK",
  "Sermon notes lose formatting when shared", "Café menu shows ñ and emoji 🎉 incorrectly",
  "Speed up the content feed query", "Login link expires before the email arrives",
  "An intentionally long issue title that describes a problem in far more words than anyone would want to read in a table cell",
];
type Issue = Json & { number: number; state: (typeof states)[State]; project: { id: string; name: string } | null; attachments: { nodes: { metadata: { url: string } }[] } };
const issues: Issue[] = [];
function issue(state: State, i: number, o: { created: number; completed?: number; updated?: number; person: number | null; label: string; priority: number; project?: number }) {
  const number = 100 + issues.length, end = o.completed ?? 0, path = paths[state];
  const at = (k: number) => ago(o.created - ((o.created - end) * (k + 1)) / (path.length + 0.5));
  issues.push({
    id: `issue-${number}`, identifier: `FIX-${number}`, number, title: titles[i % titles.length],
    url: `https://linear.app/fixture/issue/FIX-${number}`, priority: o.priority,
    createdAt: ago(o.created), updatedAt: ago(o.updated ?? end), startedAt: path.includes("progress") ? at(1) : null,
    completedAt: o.completed === undefined ? null : ago(o.completed), dueDate: i % 6 === 0 ? day(-7) : null,
    assignee: o.person === null ? null : assignee(o.person),
    project: o.project === undefined ? null : { id: projects[o.project].id, name: projects[o.project].name },
    state: states[state], labels: page([{ name: o.label }]), attachments: page([]),
    history: { edges: [
      ...(o.person === null ? [] : [{ node: { toAssignee: { displayName: people[o.person][1] }, updatedAt: ago(o.created), createdAt: ago(o.created) } }]),
      ...path.slice(1).map((to, k) => ({ node: {
        toAssignee: null, updatedAt: at(k), createdAt: at(k), fromState: states[path[k]], toState: states[to],
      } })),
    ] },
  });
}
const labels = ["Bug", "Feature Request", "Technical Change"];
for (let i = 0; i < 45; i++) {
  const completed = 0.5 + i * 1.9;
  issue("done", i, {
    completed, created: completed + 1 + ((i * 7) % 12), person: i % 11 === 10 ? null : i % 5,
    label: labels[i % 3], priority: [1, 2, 3, 4, 0][i % 5], project: i % 4 === 0 ? i % 3 : undefined,
  });
}
const open: State[] = ["triage", "backlog", "todo", "progress", "review"];
for (let i = 0; i < 20; i++) {
  const state = open[i % 5], created = 2 + ((i * 5) % 40);
  issue(state, i + 3, {
    created, updated: i % 4 === 0 ? created : 1, person: state === "triage" ? null : i % 4,
    label: i < 6 ? "Bug" : labels[i % 3], priority: i < 6 ? 1 + (i % 2) : 3 + (i % 2),
    project: i >= 6 && i % 3 === 0 ? i % 3 : undefined,
  });
}

const repos = ["apollosproject/apollos-platforms", "apollosproject/apollos-admin"];
const review = (who: number, state: string, at: string) => ({ author: { login: login(who), __typename: "User" }, state, submittedAt: at });
// Authors and approvers skew in opposite directions so every sort order visibly differs.
const authors = [0, 0, 0, 0, 1, 1, 1, 2, 2, 3], approvers = [3, 3, 3, 2, 2, 3, 2, 0, 1, 0];
const merged = Array.from({ length: 50 }, (_, i) => {
  const mergedAt = now - (0.3 + i * 1.7) * DAY, createdAt = mergedAt - (0.5 + (i % 4)) * DAY;
  return {
    id: `pr-merged-${i}`, number: 1000 + i, url: `https://github.com/${repos[i % 2]}/pull/${1000 + i}`,
    createdAt: iso(createdAt), mergedAt: iso(mergedAt),
    author: { login: i % 9 === 8 ? "dependabot[bot]" : login(authors[i % 10]) },
    readiness: page([]),
    reviews: page([review(approvers[i % 10], "APPROVED", iso(mergedAt - 2 * 3600_000))]),
    firstReviews: page([review(approvers[i % 10], i % 3 ? "COMMENTED" : "APPROVED", iso(createdAt + ((i % 6) + 1) * 3600_000))]),
    commits: page([{ commit: { author: { user: { login: login(authors[i % 10]) } }, authors: page([]) } }]),
  };
});
const openPRs = [
  { title: "Fix settings crash", decision: "REVIEW_REQUIRED", ci: "SUCCESS" },
  { title: "Dark mode toggle", decision: "REVIEW_REQUIRED", ci: "PENDING" },
  { title: "Notification provider migration", decision: "REVIEW_REQUIRED", ci: "FAILURE" },
  { title: "Event filter campuses", decision: "REVIEW_REQUIRED", ci: "SUCCESS", mergeable: "CONFLICTING" },
  { title: "Video SDK upgrade (stacked)", decision: "REVIEW_REQUIRED", ci: "SUCCESS", base: "video-sdk-base" },
  { title: "Sermon notes formatting", decision: "CHANGES_REQUESTED", ci: "SUCCESS" },
  { title: "Feed query speedup", decision: "APPROVED", ci: "SUCCESS" },
  { title: "Login link expiry", decision: "APPROVED", ci: null, mergeable: "CONFLICTING" },
  { title: "Bump the lint toolchain", decision: "REVIEW_REQUIRED", ci: "SUCCESS", author: "dependabot[bot]", size: 2400 },
].map((o, i) => {
  const linked = issues[45 + i], createdAt = ago(1 + i * 2.5), url = `https://github.com/${repos[i % 2]}/pull/${2000 + i}`;
  if (i % 2) linked.attachments.nodes.push({ metadata: { url } });
  return {
    id: `pr-open-${i}`, number: 2000 + i, title: o.title, url, createdAt, updatedAt: ago(0.5),
    baseRefName: o.base || "main", headRefName: i % 2 ? `${login(i)}/change-${i}` : `${login(i)}/fix-${linked.number}`,
    additions: o.size || 30 + i * 70, deletions: 10 + i * 5, mergeable: o.mergeable || "MERGEABLE",
    reviewDecision: o.decision, author: { login: o.author || login(i) }, timelineCount: { totalCount: 3 },
    repository: { nameWithOwner: repos[i % 2], defaultBranchRef: { name: "main" } },
    statusCheckRollup: o.ci ? { state: o.ci } : null,
    reviews: page(o.decision === "APPROVED" ? [review(i + 1, "APPROVED", ago(0.8 + i))]
      : o.decision === "CHANGES_REQUESTED" ? [review(i + 2, "CHANGES_REQUESTED", ago(1))] : []),
    reviewRequests: { nodes: [{ requestedReviewer: { login: login(i + 1) } }], pageInfo: { hasNextPage: false } },
    timeline: [{ __typename: "ReviewRequestedEvent", createdAt, requestedReviewer: { login: login(i + 1) } }],
  };
});

type Filter = {
  team?: unknown;
  state?: { type: { in?: string[]; eq?: string } };
  completedAt?: { gte: string; lt: string };
  number?: { in: number[] };
  id?: { in: string[] };
  project?: { id: { in: string[] } };
  attachments?: { some: { url: { in: string[] } } };
};
const filterKeys = ["team", "state", "completedAt", "number", "id", "project", "attachments"];
function matches(issue: Issue, filter: Filter) {
  const unknown = Object.keys(filter).find((key) => !filterKeys.includes(key));
  if (unknown) throw new Error(`fixture: unsupported Linear filter ${unknown}`);
  const { state, completedAt, number, id, project, attachments } = filter;
  const completed = issue.completedAt as string | null;
  return (!state?.type.in || state.type.in.includes(issue.state.type)) &&
    (!state?.type.eq || state.type.eq === issue.state.type) &&
    (!completedAt || (!!completed && completed >= completedAt.gte && completed < completedAt.lt)) &&
    (!number || number.in.includes(issue.number)) &&
    (!id || id.in.includes(issue.id as string)) &&
    (!project || project.id.in.includes(issue.project?.id || "")) &&
    (!attachments || issue.attachments.nodes.some(({ metadata }) => attachments.some.url.in.includes(metadata.url)));
}
function linear(operation: string, variables: Json) {
  if (operation === "Issues")
    return { issues: page(issues.filter((i) => matches(i, variables.filter as Filter)).map((i) => ({ ...i, history: undefined }))) };
  if (operation === "Histories")
    return { issues: page(issues.filter((i) => (variables.ids as string[]).includes(i.id as string)).map((i) => ({
      id: i.id, updatedAt: i.updatedAt, history: { ...(i.history as Json), pageInfo: { hasNextPage: false, endCursor: null } },
    }))) };
  if (operation === "Projects") return { teams: { nodes: [{ projects: page(projects) }] } };
}
function github(operation: string, variables: Json) {
  if (operation === "Search") {
    const query = String(variables.query), range = /merged:(\S+)\.\.(\S+)/.exec(query);
    const nodes: unknown[] = range
      ? merged.filter((pr) => pr.mergedAt.slice(0, 10) >= range[1] && pr.mergedAt.slice(0, 10) <= range[2])
      : openPRs.filter((pr) => query.includes(`repo:${pr.repository.nameWithOwner} `) &&
        (query.includes("-review:approved") ? pr.reviewDecision !== "APPROVED"
          : !query.includes("review:approved") || pr.reviewDecision === "APPROVED"))
        .map((pr) => ({ ...pr, timeline: undefined }));
    return { search: { issueCount: nodes.length, ...page(nodes) } };
  }
  if (operation === "Timeline") {
    const pr = openPRs.find((p) => p.id === variables.id);
    return pr && { node: { timelineItems: { nodes: pr.timeline, pageInfo: { hasPreviousPage: false, startCursor: null } } } };
  }
}
async function graphql(body: string, resolve: (operation: string, variables: Json) => Json | undefined) {
  const { query, variables } = JSON.parse(body);
  const operation = /query (\w+)/.exec(query)?.[1] || "";
  const data = resolve(operation, variables || {});
  if (!data) console.warn(`fixture: unsupported query ${operation}`);
  return Response.json(data ? { data } : { errors: [{ message: `fixture: unsupported ${operation}` }] });
}

const dags = Array.from({ length: 30 }, (_, i) => `fixture_church_${String(i + 1).padStart(2, "0")}_content_sync`);
function airflow(url: URL) {
  if (url.pathname.endsWith("/dags"))
    return { dags: [...dags.map((dag_id) => ({ dag_id, is_paused: false })), { dag_id: "fixture_paused_dag", is_paused: true }], total_entries: dags.length + 1 };
  const i = dags.indexOf(decodeURIComponent(url.pathname.split("/").at(-2) || ""));
  const runs = i === 10 ? [] : [
    ...(i === 6 ? [{ state: "running", dag_run_id: "scheduled__now" }] : []),
    { state: i % 13 === 3 ? "failed" : "success", dag_run_id: `scheduled__${day(0)}` },
  ];
  return { dag_runs: url.searchParams.get("offset") === "0" ? runs : [], total_entries: runs.length };
}
const calendarDate = (days: number) => day(-days).replaceAll("-", "");
const pto = ["BEGIN:VCALENDAR", "VERSION:2.0",
  ...[[3, 7, "Avery Example - Vacation"], [10, 11, "Blake Sample - Personal day"], [-2, 2, "Casey Demo - Conference"]].flatMap(([a, b, summary], i) => [
    "BEGIN:VEVENT", `UID:fixture-${i}`, `DTSTART;VALUE=DATE:${calendarDate(a as number)}`,
    `DTEND;VALUE=DATE:${calendarDate(b as number)}`, `SUMMARY:${summary}`, "END:VEVENT",
  ]), "END:VCALENDAR"].join("\r\n");

const store = new Map<string, { value: string | number; expires: number }>();
const live = (key: string) => {
  const entry = store.get(key);
  if (entry?.expires && entry.expires < Date.now()) store.delete(key);
  return store.get(key);
};
function redis([name, ...args]: (string | number)[]) {
  const key = String(args[0]), options = args.slice(2).map((o) => String(o).toLowerCase());
  switch (String(name).toLowerCase()) {
    case "get": return live(key)?.value ?? null;
    case "set": {
      if (options.includes("nx") && live(key)) return null;
      const ex = options.indexOf("ex");
      store.set(key, { value: String(args[1]), expires: ex < 0 ? 0 : Date.now() + Number(options[ex + 1]) * 1000 });
      return "OK";
    }
    case "incr": {
      const value = Number(live(key)?.value ?? 0) + 1;
      store.set(key, { value, expires: live(key)?.expires ?? 0 });
      return value;
    }
    // Only the refresh lock's compare-and-delete script: eval script 1 key token.
    case "eval": return live(String(args[2]))?.value === String(args[3]) && store.delete(String(args[2])) ? 1 : 0;
    default: throw new Error(`fixture: unsupported Redis command ${name}`);
  }
}
function upstash(url: URL, body: string, headers: Headers) {
  const encode = (result: unknown) => ({
    result: typeof result === "string" && headers.get("upstash-encoding") === "base64"
      ? Buffer.from(result).toString("base64") : result,
  });
  const commands = JSON.parse(body);
  return url.pathname.startsWith("/pipeline") ? commands.map((c: string[]) => encode(redis(c))) : encode(redis(commands));
}

const realFetch = globalThis.fetch;
globalThis.fetch = async (input, init = {}) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  if (["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)) return realFetch(input, init);
  const body = typeof init.body === "string" ? init.body : "", headers = new Headers(init.headers);
  switch (url.hostname) {
    case "api.linear.app": return graphql(body, linear);
    case "api.github.com":
      if (url.pathname === "/graphql") return graphql(body, github);
      break;
    case "airflow.fixture.invalid": return Response.json(airflow(url));
    case "redis.fixture.invalid": return Response.json(upstash(url, body, headers));
    case "app.rippling.com": return new Response(pto, { headers: { "Content-Type": "text/calendar" } });
  }
  throw new TypeError(`fixture: blocked request to ${url.hostname}${url.pathname}`);
};

// Stand in for Vercel Cron: fleet and the 30-day metrics snapshot expire after 180 seconds.
if (process.argv.includes("start")) {
  const arg = (names: string[], fallback: string) => {
    const i = process.argv.findIndex((a) => names.includes(a));
    return i >= 0 ? process.argv[i + 1] : fallback;
  };
  const origin = `http://${arg(["--hostname", "-H"], "127.0.0.1")}:${arg(["--port", "-p"], process.env.PORT || "3000")}`;
  const refresh = async () => {
    const results = await Promise.all(["fleet", "metrics"].map((job) =>
      realFetch(`${origin}/api/cron/${job}`, { headers: { Authorization: "Bearer fixture" } })
        .then((r) => r.ok, () => false)));
    // Retry fast until the server listens: only /dags waits on these snapshots, and the explorer opens / first.
    setTimeout(refresh, results.every(Boolean) ? 60_000 : 250).unref();
  };
  refresh();
}
