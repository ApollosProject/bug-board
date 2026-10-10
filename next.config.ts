import { withWorkflow } from "workflow/next";
export default withWorkflow({
  poweredByHeader: false,
  serverExternalPackages: [
    "@google-cloud/bigquery",
    "google-auth-library",
    "gaxios",
  ],
  async redirects() {
    return [
      { source: "/failing-dags", destination: "/dags", permanent: true },
      { source: "/app-versions", destination: "/apps", permanent: true },
    ];
  },
  outputFileTracingIncludes: {
    "/*": [
      "./config.yml",
      "./regression_overrides.yml",
      "./lib/app-versions.sql",
    ],
  },
});
