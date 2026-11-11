import { test } from "@e2e-dev/web";
import { expect } from "e2e";
test("church directory lists churches and opens one with its issue", async ({
  app,
  screen,
  browser,
}) => {
  await app.open("/churches");
  await expect(
    screen.getByRole("heading", "Churches", { exact: true }),
  ).toBeVisible();
  const directory = screen.getByRole("region", "Churches");
  await expect(directory).toContainText("2 of 2 churches");
  await expect(
    directory.getByRole("link", "Cedar Creek", { exact: true }),
  ).toBeVisible();
  await expect(
    directory.getByRole("link", "Harbor Chapel", { exact: true }),
  ).toBeVisible();
  await screen.getByLabel("Search churches", { exact: true }).fill("cedar");
  await screen.getByRole("button", "Search", { exact: true }).tap();
  await expect(browser).toHaveURL(/q=cedar/);
  await expect(directory).toContainText("1 of 2 churches");
  await expect(
    directory.getByRole("link", "Harbor Chapel", { exact: true }),
  ).toHaveCount(0);
  await directory.getByRole("link", "Cedar Creek", { exact: true }).tap();
  await expect(browser).toHaveURL(/\/churches\/fixture_church_01$/);
  await expect(
    screen.getByRole("heading", "Cedar Creek", { exact: true }),
  ).toBeVisible();
  await expect(
    screen.getByRole("heading", "Configuration", { exact: true }),
  ).toBeVisible();
  await expect(
    screen.getByText("fixture_church_01, cedarcreek", { exact: true }),
  ).toBeVisible();
  await expect(
    screen.getByText("links.cedarcreek.example", { exact: true }),
  ).toBeVisible();
  await expect(
    screen.getByText("America/Chicago", { exact: true }),
  ).toBeVisible();
  await expect(
    screen.getByText("Enabled · stripe / stripe", { exact: true }),
  ).toBeVisible();
  await expect(
    screen.getByRole("heading", /Open issues mentioning Cedar Creek/),
  ).toBeVisible();
  await expect(
    screen.getByRole(
      "link",
      "Giving page shows a blank state after refresh at Cedar Creek",
      { exact: true },
    ),
  ).toBeVisible();
  await expect(
    screen.getByText("No completed issues mention this church.", {
      exact: true,
    }),
  ).toBeVisible();
});
