import { test } from "@e2e-dev/web";
import { expect } from "e2e";
test("populated team metrics, person work, and CSV export", async ({
  app,
  screen,
  browser,
}) => {
  await app.open("/team");
  await expect(screen.getByRole("region", "Team metrics").getByRole("table")).toBeVisible();
  await expect(screen.getByRole("region", "Linear stage timing").getByRole("table")).toBeVisible();
  await expect(screen.getByRole("heading", "First-review wait", { exact: true })).toBeVisible();
  await expect(screen.getByRole("heading", /Approved but unmerged/)).toBeVisible();
  await app.screenshot("populated-pipeline");
  await screen.getByRole("link", "Zach", { exact: true }).tap();
  await expect(
    screen.getByRole("heading", "Zach", { exact: true }),
  ).toBeVisible();
  await expect(screen.getByRole("heading", /Delivery/)).toBeVisible();
  await app.screenshot("populated-person");
  await app.open("/team?everyone=1");
  await expect(
    screen.getByRole("link", "Andy Smith", { exact: true }),
  ).toBeVisible();
  const csv = await fetch(new URL("/team.csv", app.baseUrl));
  expect(csv.status).toBe(200);
  expect(await csv.text()).toContain("project_contributor_weeks");
  if (process.env.E2E_API_KEY) {
    const response = await fetch(new URL("/api/team/zach", app.baseUrl), {
      headers: { Authorization: `Bearer ${process.env.E2E_API_KEY}` },
    });
    expect(response.status).toBe(200);
    const json = await response.json();
    expect(json.person.slug).toBe("zach");
    expect(json.metrics.prs_merged.value).toEqual(expect.any(Number));
  }
  await expect(browser).toHaveURL(/everyone=1/);
});
test("populated mobile cards and issue lists stay readable", async ({ app, browser, screen }) => {
  for (const width of [320, 390]) {
    await browser.setViewport({ width, height: 844 });
    await app.open("/team/zach");
    await expect(screen.getByRole("heading", /Delivery/)).toBeVisible();
    await expect(browser.locator(".metric-value").first()).toBeVisible();
    for (const summary of await browser.locator("main details > summary").all()) {
      await summary.tap();
    }
    expect(await browser.evaluate(async () => {
      await Promise.all(document.getAnimations().map((animation) => animation.finished));
      return [...document.querySelectorAll<HTMLElement>(".cards *, details summary")]
        .filter((el) => el.clientWidth && el.scrollWidth > el.clientWidth + 1)
        .map((el) => el.textContent);
    })).toEqual([]);
    expect(await browser.evaluate(() =>
      [...document.querySelectorAll<HTMLElement>(".cards article")]
        .every((card) => card.getBoundingClientRect().width >= 190)
        && document.documentElement.scrollWidth <= innerWidth,
    )).toBe(true);
    await app.screenshot(`populated-mobile-person-${width}`);
    await app.open("/team");
    await expect(screen.getByRole("region", "Linear stage timing").getByRole("table")).toBeVisible();
    for (const summary of await browser.locator("main details > summary").all()) {
      await summary.tap();
    }
    expect(await browser.evaluate(() =>
      [...document.querySelectorAll<HTMLElement>(".issue-list li:not(:has(> .badge)) > div")]
        .every((el) => getComputedStyle(el.parentElement!).gridTemplateColumns.split(" ").length === 1),
    )).toBe(true);
  }
});
test("populated fleet and app releases filter on the real server", async ({
  app,
  screen,
}) => {
  await app.open("/dags");
  await expect(screen.getByRole("table")).toBeVisible();
  await screen
    .getByLabel("Run state", { exact: true })
    .selectOption({ value: "failed" });
  await screen.getByRole("button", "Filter DAGs").tap();
  await expect(screen.getByRole("table")).toBeVisible();
  await app.screenshot("populated-failed-dags");
  await app.open("/apps");
  await expect(screen.getByRole("table")).toBeVisible();
  await app.screenshot("populated-apps");
});
