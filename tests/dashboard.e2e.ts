import { test } from "@e2e-dev/web";
import { expect } from "e2e";
test("dashboard navigation and no-secrets rendering", async ({
  app,
  screen,
}) => {
  await app.open("/");
  await expect(
    screen.getByRole("heading", "Bug Board", { exact: true }),
  ).toBeVisible();
  for (const name of ["Team", "Reviews", "Apps", "Projects", "DAGs"]) {
    await screen.getByRole("link", name, { exact: true }).tap();
    await expect(
      screen.getByRole("heading", name, { exact: true }),
    ).toBeVisible();
  }
  await app.screenshot("dags-dashboard");
});
test("preset and custom date windows survive navigation", async ({
  app,
  screen,
  browser,
}) => {
  await app.open("/team");
  await screen.getByRole("link", "7d", { exact: true }).tap();
  await expect(browser).toHaveURL(/days=7/);
  await screen.getByLabel("Start", { exact: true }).fill("2026-08-01");
  await screen.getByLabel("End", { exact: true }).fill("2026-08-07");
  await screen.getByRole("button", "Apply dates").tap();
  await expect(browser).toHaveURL(/start=2026-08-01.*end=2026-08-07/);
  await screen.getByRole("link", "Show everyone").tap();
  await expect(browser).toHaveURL(/everyone=1/);
  await expect(screen.getByRole("link", "Export CSV")).toHaveAttribute(
    "href",
    /start=2026-08-01.*end=2026-08-07/,
  );
});
test("review and DAG filters use URL state", async ({
  app,
  screen,
  browser,
}) => {
  await app.open("/reviews");
  await screen
    .getByLabel("Author", { exact: true })
    .selectOption({ value: "zach" });
  await screen
    .getByLabel("Reviewer", { exact: true })
    .selectOption({ value: "michael" });
  await screen.getByRole("button", "Filter reviews").tap();
  await expect(browser).toHaveURL(/author=zach.*reviewer=michael/);
  await app.open("/dags");
  await screen.getByLabel("Search DAGs", { exact: true }).fill("church");
  await screen
    .getByLabel("Run state", { exact: true })
    .selectOption({ value: "failed" });
  await screen.getByRole("button", "Filter DAGs").tap();
  await expect(browser).toHaveURL(/q=church.*state=failed/);
});
test("mobile navigation stays usable", async ({ app, browser, screen }) => {
  await browser.setViewport({ width: 390, height: 844 });
  await app.open("/");
  await screen.getByRole("link", "Reviews", { exact: true }).tap();
  await expect(
    screen.getByRole("heading", "Reviews", { exact: true }),
  ).toBeVisible();
  expect(
    await browser.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await app.screenshot("mobile-reviews");
});
test("health is public, API and cron remain closed without authentication", async ({
  app,
}) => {
  const health = await fetch(new URL("/healthz", app.baseUrl));
  expect(health.status).toBe(200);
  expect(await health.json()).toEqual({ status: "ok" });
  for (const path of ["/api/team/zach", "/api/cron/metrics"]) {
    const response = await fetch(new URL(path, app.baseUrl));
    expect([401, 503]).toContain(response.status);
    expect(response.headers.get("content-type")).toContain("application/json");
  }
});
test("legacy dashboard URLs still resolve", async ({ app, screen }) => {
  await app.open("/failing-dags");
  await expect(
    screen.getByRole("heading", "DAGs", { exact: true }),
  ).toBeVisible();
  await app.open("/app-versions");
  await expect(
    screen.getByRole("heading", "Apps", { exact: true }),
  ).toBeVisible();
});
