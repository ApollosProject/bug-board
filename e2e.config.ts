import type { E2EConfig } from "e2e";
import { web } from "@e2e-dev/web";
import { github } from "@e2e-dev/github";
import { gateway } from "ai";
import nextEnv from "@next/env";
nextEnv.loadEnvConfig(process.cwd());
const url = process.env.E2E_BASE_URL || "http://127.0.0.1:3000";
const agent = {
  model: gateway(process.env.E2E_MODEL || "openai/gpt-6-luna"),
  maxSteps: 15,
  maxModelCalls: 20,
  context:
    "Bug Board is a read-only engineering dashboard. Navigate and filter only. Never click deploy, confirm deployment, sign in, or log out. Missing integration credentials must show unavailable, not fabricated zero metrics." +
    (process.env.E2E_FIXTURES === "1"
      ? " This build serves fake fixture people, issues, PRs, projects, PTO, and DAGs. Apps, churches, and regression cards have no fixture and correctly show unavailable or refreshing. Linear and GitHub links point at fake URLs: do not follow or report external linear.app or github.com links."
      : ""),
};
export default {
  tests: [
    "tests/dashboard.e2e.ts",
    "tests/luna.e2e.ts",
    ...(process.env.E2E_DATA === "1" ? ["tests/data.e2e.ts"] : []),
  ],
  targets: [
    {
      name: "web",
      engine: web(),
      app: {
        url,
        environment: "test",
        ...(process.env.E2E_BASE_URL
          ? {}
          : {
              command: {
                executable: "npm",
                args: [
                  "run",
                  process.env.E2E_FIXTURES === "1" ? "start:fixtures" : "start",
                  "--",
                  "--hostname",
                  "127.0.0.1",
                  "--port",
                  "{port}",
                ],
                reuseExisting: true,
                log: ".e2e/logs/next.log",
              },
            }),
      },
    },
  ],
  agents: {
    default: agent,
    // Explore steps need a larger per-step budget than test steps.
    explore: { ...agent, maxSteps: 40, maxModelCalls: 40 },
  },
  workers: 2,
  retries: 0,
  trace: "on",
  video: "on",
  reporters: ["list", "junit", "markdown", github()],
} satisfies E2EConfig;
