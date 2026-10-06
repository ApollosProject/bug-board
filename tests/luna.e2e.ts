import { test } from "@e2e-dev/web";
import { expect } from "e2e";
test(
  "Luna explores the review filter flow through AI Gateway",
  { tags: ["ai"] },
  async ({ app, agent, screen, browser }) => {
    await app.open("/");
    await agent.act(
      "Navigate to Reviews, filter Author to Zach and Reviewer to Michael, then apply the filters. Do not sign in or trigger any deployment.",
    );
    await expect(
      screen.getByRole("heading", "Reviews", { exact: true }),
    ).toBeVisible();
    await expect(browser).toHaveURL(/author=zach.*reviewer=michael/);
    await agent.assert(
      "This is an engineering review dashboard with author and reviewer filters, and no production deployment was initiated.",
    );
    await app.screenshot("luna-review-filters");
  },
);
test(
  "Luna checks the mobile dashboard",
  { tags: ["ai"] },
  async ({ app, agent, screen, browser }) => {
    await browser.setViewport({ width: 390, height: 844 });
    await app.open("/");
    await agent.act(
      "Use the site's navigation to open the DAGs dashboard. Do not sign in or click any deployment control.",
    );
    await expect(
      screen.getByRole("heading", "DAGs", { exact: true }),
    ).toBeVisible();
    await agent.assert(
      "The dashboard navigation and DAG filtering controls fit on this mobile screen and remain readable.",
      { vision: "only" },
    );
  },
);
