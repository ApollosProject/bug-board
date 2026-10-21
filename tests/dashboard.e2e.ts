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
  await expect(screen.getByRole("heading", "Shipping pipeline", { exact: true })).toBeVisible();
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
  const reviewer = screen.getByLabel("Reviewer", { exact: true });
  const approved = screen.getByLabel("Include approved", { exact: true });
  await approved.tap();
  await screen.getByRole("button", "Filter reviews").tap();
  await expect(browser).toHaveURL(/approved=1/);
  await screen.getByRole("link", "Reset", { exact: true }).tap();
  await expect(browser).toHaveURL(/\/reviews$/);
  await expect(reviewer).toHaveValue("");
  await expect(approved).toBeChecked({ checked: false });
  await browser.back();
  await expect(browser).toHaveURL(/approved=1/);
  await expect(reviewer).toHaveValue("michael");
  await expect(approved).toBeChecked();
  await app.open("/dags");
  await screen.getByLabel("Search DAGs", { exact: true }).fill("church");
  await screen
    .getByLabel("Run state", { exact: true })
    .selectOption({ value: "failed" });
  await screen.getByRole("button", "Filter DAGs").tap();
  await expect(browser).toHaveURL(/q=church.*state=failed/);
  const runState = screen.getByLabel("Run state", { exact: true });
  await runState.selectOption({ value: "no_runs" });
  await screen.getByRole("button", "Filter DAGs").tap();
  await expect(browser).toHaveURL(/state=no_runs/);
  await browser.back();
  await expect(browser).toHaveURL(/state=failed/);
  await expect(runState).toHaveValue("failed");
});
test("mobile navigation stays usable", async ({ app, browser, screen }) => {
  await browser.setViewport({ width: 390, height: 844 });
  await app.open("/");
  await screen.getByRole("link", "Reviews", { exact: true }).tap();
  await expect(
    screen.getByRole("heading", "Reviews", { exact: true }),
  ).toBeVisible();
  await app.screenshot("mobile-reviews");
  for (const width of [320, 390]) {
    await browser.setViewport({ width, height: 844 });
    await app.open("/");
    // CI is signed out; exercise the account's shared CSS without bypassing auth.
    expect(await browser.evaluate(() => {
      const form = document.createElement("form");
      const button = document.createElement("button");
      button.type = "submit";
      button.className = "account";
      button.textContent = "@example-engineering-long-github-username · log\u00a0out";
      form.append(button);
      document.querySelector(".site-header nav")!.append(form);
      return document.documentElement.scrollWidth <= innerWidth;
    })).toBe(true);
    for (const path of ["/", "/team", "/team/zach", "/reviews", "/apps", "/projects", "/dags", "/churches", "/churches/example", "/missing-page"]) {
      await app.open(path);
      await expect(screen.getByRole("heading", { level: 1 })).toBeVisible();
      expect(
        await browser.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
    }
  }
});
test("sign-in card has room around its action on phones and desktop", async ({
  app,
  browser,
  screen,
}) => {
  for (const width of [320, 390, 1280]) {
    await browser.setViewport({ width, height: 844 });
    await app.open("/signin?next=%2Freviews");
    await expect(screen.getByRole("link", "Sign in with GitHub")).toHaveAttribute(
      "href", "/login?next=%2Freviews",
    );
    const layout = await browser.evaluate(() => {
      const card = document.querySelector<HTMLElement>(".auth-card")!;
      const action = card.querySelector("a")!.getBoundingClientRect();
      const note = card.querySelector("p:last-child")!.getBoundingClientRect();
      return {
        padding: parseFloat(getComputedStyle(card).paddingLeft),
        gap: note.top - action.bottom,
        contained: document.documentElement.scrollWidth <= window.innerWidth,
      };
    });
    expect(layout.padding).toBeGreaterThanOrEqual(20);
    expect(layout.gap).toBeGreaterThanOrEqual(12);
    expect(layout.contained).toBe(true);
    await app.screenshot(`signin-${width}`);
  }
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
