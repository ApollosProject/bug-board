import { test } from "node:test";
import assert from "node:assert/strict";
import { renderToStaticMarkup } from "react-dom/server";
import { Timeline } from "../app/projects/page";
import type { Project } from "../lib/types";

const project = (id: string, overrides: Partial<Project> = {}): Project => ({
  id,
  name: id,
  url: `https://linear.app/project/${id}`,
  status: { name: "Ready", type: "planned" },
  lead: { displayName: "Zach" },
  members: { nodes: [] },
  initiatives: { nodes: [] },
  ...overrides,
});

test("timeline includes undated active ownership, preserves dated bounds, and keeps Linear links separate", async (t) => {
  const env = {
    UPSTASH_REDIS_REST_URL: "https://redis.example.test",
    UPSTASH_REDIS_REST_TOKEN: "fixture-redis",
  };
  const previous = Object.fromEntries(Object.keys(env).map((key) => [key, process.env[key]]));
  Object.assign(process.env, env);
  t.after(() => {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });
  t.mock.timers.enable({ apis: ["Date"], now: Date.parse("2026-10-12T12:00:00Z") });
  const projects = [
    project("undated-lead", { priorityLabel: "High", description: "Short summary", progress: 0.42, members: { nodes: [{ displayName: "Zach" }, { displayName: "Outside Collaborator" }] } }),
    project("undated-member", { lead: null, members: { nodes: [{ displayName: "Zach" }] }, priorityLabel: "Low" }),
    project("dated", { startDate: "2026-10-19", targetDate: "2026-10-25", priorityLabel: "Medium" }),
    project("start-only", { startDate: "2026-10-19" }),
    project("target-only", { targetDate: "2026-10-25" }),
    project("past", { startDate: "2026-09-01", targetDate: "2026-09-07", completedAt: "2026-09-07", status: { name: "Completed", type: "completed" } }),
    project("very-low", { startDate: "2026-10-19", priorityLabel: "Very Low" }),
    project("future", { startDate: "2026-12-01", targetDate: "2026-12-07" }),
    project("canceled", { status: { name: "Canceled", type: "canceled" } }),
    project("undated-completed", { status: { name: "Completed", type: "completed" } }),
    project("unassigned", { lead: null, priorityLabel: "Urgent" }),
    project("started", { status: { name: "In Progress", type: "started" }, health: "onTrack" }),
    project("paused", { status: { name: "Paused", type: "paused" }, health: "atRisk" }),
  ];
  t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
    const commands = JSON.parse(String(init?.body));
    const result = (command: string[]) => ({
      result: JSON.stringify({
        at: Date.now(),
        data: command[1].endsWith(":projects") ? projects : { configured: false, available: false, events: [] },
      }),
    });
    return Response.json(Array.isArray(commands[0]) ? commands.map(result) : result(commands));
  });
  const html = renderToStaticMarkup(await Timeline());
  const anchor = (id: string) => {
    const matches = html.match(new RegExp(`<a [^>]*href="https://linear.app/project/${id}"[^>]*>[^]*?</a>`, "g"));
    assert.ok(matches, `Missing ${id}`);
    return matches.find((a) => a.includes('class="timeline-bar')) || matches[0];
  };
  const card = (id: string) => {
    const ref = anchor(id).match(/aria-describedby="([^"]+)"[^>]*anchor-name:--\1"/)?.[1];
    assert.ok(ref, `Missing card for ${id}`);
    return html.match(new RegExp(`<div class="project-card" id="${ref}" role="tooltip" popover="manual" style="position-anchor:--${ref}">[^]*?</div></div>`))?.[0] || "";
  };
  for (const id of ["undated-lead", "undated-member"]) {
    const bar = anchor(id);
    assert.match(bar, /grid-column:1 \/ span 42/);
    assert.match(card(id), /<dt>Dates<\/dt><dd>Not set<\/dd>/);
    assert.match(bar, /target="_blank" rel="noopener noreferrer"/);
  }
  assert.notEqual(
    anchor("undated-lead").match(/grid-row:(\d+)/)?.[1],
    anchor("undated-member").match(/grid-row:(\d+)/)?.[1],
  );
  assert.match(anchor("dated"), /grid-column:8 \/ span 7/);
  assert.match(anchor("start-only"), /grid-column:8 \/ span 35/);
  assert.match(anchor("target-only"), /grid-column:1 \/ span 14/);
  for (const id of ["past", "future", "canceled", "undated-completed"]) {
    assert.ok(!html.includes(`https://linear.app/project/${id}`));
  }
  assert.match(anchor("unassigned"), /target="_blank" rel="noopener noreferrer"/);
  for (const [id, label, filled] of [["undated-lead", "High", 3], ["undated-member", "Low", 1], ["dated", "Medium", 2], ["very-low", "Very Low", 0], ["start-only", "No priority", 0]] as const) {
    assert.match(anchor(id), new RegExp(`aria-label="Priority: ${label}"`));
    assert.equal(anchor(id).match(/opacity="1"/g)?.length || 0, filled);
    assert.ok(!anchor(id).includes(`<small>${label}</small>`));
  }
  assert.match(anchor("very-low"), /opacity="0.35"/);
  assert.doesNotMatch(anchor("start-only"), /opacity="0.35"/);
  assert.match(html, /aria-label="Priority key"[^]*Very Low[^]*No priority/);
  assert.match(anchor("unassigned"), /aria-label="Priority: Urgent"[^]*fill-rule="evenodd"/);
  assert.match(anchor("undated-lead"), /class="timeline-bar neutral idle lead"/);
  assert.doesNotMatch(html, /class="timeline-bar[^>]*title=/);
  assert.match(card("undated-lead"), /<strong>undated-lead<\/strong><p>Short summary<\/p><p><meter value="0.42"><\/meter>42%<\/p>/);
  assert.match(card("undated-lead"), /<dt>Status<\/dt><dd>Ready<\/dd>[^]*<dt>Priority<\/dt><dd>High<\/dd>[^]*<dt>Lead<\/dt><dd>Zach<\/dd>[^]*<dt>Members<\/dt><dd>Outside Collaborator<\/dd>/);
  assert.match(card("undated-lead"), /<small>Opens in Linear in a new tab<\/small>/);
  assert.match(anchor("started"), /class="timeline-bar onTrack lead"/);
  assert.match(card("started"), /<dt>Status<\/dt><dd>In Progress · On track<\/dd>/);
  assert.match(anchor("paused"), /class="timeline-bar atRisk idle lead"/);
  assert.doesNotMatch(anchor("undated-member"), / lead"|· Lead/);
  assert.ok(html.includes("No projects in this window"));
});
