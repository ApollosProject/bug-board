import { test } from "@e2e-dev/web";
import { expect } from "e2e";
test("review queue keeps blocked pull requests out of ready", async ({
  app,
  screen,
  browser,
}) => {
  await app.open("/reviews");
  const ready = screen.getByRole("region", "Ready for Review pull requests");
  const running = screen.getByRole("region", "CI Running pull requests");
  const blocked = screen.getByRole("region", "Not Ready pull requests");
  for (const title of [
    "Fix settings crash",
    "Bump the lint toolchain",
    "Remove obsolete settings",
  ])
    await expect(ready.getByRole("link", title, { exact: true })).toBeVisible();
  await expect(
    running.getByRole("link", "Dark mode toggle", { exact: true }),
  ).toBeVisible();
  for (const title of [
    "Notification provider migration",
    "Event filter campuses",
    "Video SDK upgrade (stacked)",
    "Sermon notes formatting",
  ]) {
    await expect(
      blocked.getByRole("link", title, { exact: true }),
    ).toBeVisible();
    await expect(ready.getByRole("link", title, { exact: true })).toHaveCount(
      0,
    );
  }
  await expect(blocked).toContainText("CI failing");
  await expect(blocked).toContainText("Conflicts");
  await expect(blocked).toContainText("Stacked");
  await expect(blocked).toContainText("Changes requested");
  await expect(
    screen.getByRole("link", "Feed query speedup", { exact: true }),
  ).toHaveCount(0);
  await app.screenshot("review-queue");

  await screen.getByLabel("Include approved", { exact: true }).tap();
  await screen.getByRole("button", "Filter reviews").tap();
  await expect(browser).toHaveURL(/approved=1/);
  const approved = screen.getByRole("region", "Approved pull requests");
  await expect(
    approved.getByRole("link", "Feed query speedup", { exact: true }),
  ).toBeVisible();
  await expect(
    approved.getByRole("link", "Login link expiry", { exact: true }),
  ).toBeVisible();
  await expect(
    ready.getByRole("link", "Feed query speedup", { exact: true }),
  ).toHaveCount(0);

  await screen
    .getByLabel("Author", { exact: true })
    .selectOption({ value: "avery" });
  await screen.getByRole("button", "Filter reviews").tap();
  await expect(browser).toHaveURL(/author=avery/);
  await expect(
    ready.getByRole("link", "Fix settings crash", { exact: true }),
  ).toBeVisible();
  await expect(
    ready.getByRole("link", "Remove obsolete settings", { exact: true }),
  ).toHaveCount(0);
  await expect(
    blocked.getByRole("link", "Sermon notes formatting", { exact: true }),
  ).toBeVisible();
  await expect(
    approved.getByRole("link", "Feed query speedup", { exact: true }),
  ).toHaveCount(0);
});
