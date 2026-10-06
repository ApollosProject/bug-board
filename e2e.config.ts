import type { E2EConfig } from "e2e";
import { web } from "@e2e-dev/web";
import { gateway } from "ai";
import nextEnv from "@next/env";
nextEnv.loadEnvConfig(process.cwd());
const url = process.env.E2E_BASE_URL || "http://127.0.0.1:3000";
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
                  "start",
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
    default: {
      model: gateway(process.env.E2E_MODEL || "openai/gpt-6-luna"),
      maxSteps: 15,
      maxModelCalls: 20,
      context:
        "Bug Board is a read-only engineering dashboard. Navigate and filter only. Never click deploy, confirm deployment, sign in, or log out. Missing integration credentials must show unavailable, not fabricated zero metrics.",
    },
  },
  workers: 2,
  retries: 0,
  trace: "on",
  video: "on",
  reporters: ["list", "junit", "markdown"],
} satisfies E2EConfig;
