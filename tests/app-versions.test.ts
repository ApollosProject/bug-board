import { test } from "node:test";
import assert from "node:assert/strict";
import { BigQuery } from "@google-cloud/bigquery";
import { appObservations } from "../lib/apps";

test("app analytics preserve main's ingestion-partition pruning and clock-skew buffer", async () => {
  const names = [
    "BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64",
    "BIGQUERY_ANALYTICS_PROJECT_ID",
    "BIGQUERY_ANALYTICS_DATASETS",
    "BIGQUERY_ANALYTICS_TABLES",
    "APP_VERSIONS_LOOKBACK_DAYS",
    "APP_VERSIONS_PARTITION_BUFFER_DAYS",
  ];
  const env = Object.fromEntries(
    names.map((name) => [name, process.env[name]]),
  );
  const descriptor = Object.getOwnPropertyDescriptor(
    BigQuery.prototype,
    "query",
  )!;
  const schema = (partitioned: boolean, table = "identifies") =>
    [
      "timestamp",
      "apollos_version",
      ...(partitioned ? ["_PARTITIONTIME"] : []),
    ].map((column_name) => ({
      dataset_name: "apollos",
      table_name: table,
      column_name,
    }));
  try {
    Object.assign(process.env, {
      BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64: Buffer.from(
        JSON.stringify({
          client_email: "fixture@example.test",
          private_key: "fixture",
        }),
      ).toString("base64"),
      BIGQUERY_ANALYTICS_PROJECT_ID: "analytics-project",
      BIGQUERY_ANALYTICS_DATASETS: "apollos",
      BIGQUERY_ANALYTICS_TABLES: "identifies,screens",
    });
    for (const { columns, days, buffer, expected, predicates } of [
      {
        columns: schema(true),
        days: "30",
        buffer: undefined,
        expected: 33,
        predicates: 1,
      },
      {
        columns: schema(true),
        days: "14",
        buffer: "7",
        expected: 21,
        predicates: 1,
      },
      {
        columns: schema(false),
        days: "30",
        buffer: "3",
        expected: undefined,
        predicates: 0,
      },
      {
        columns: [...schema(true), ...schema(false, "screens")],
        days: "30",
        buffer: "3",
        expected: 33,
        predicates: 1,
      },
      ...["invalid", "0", "-1", "1.5"].map((buffer) => ({
        columns: schema(true),
        days: "30",
        buffer,
        expected: 33,
        predicates: 1,
      })),
    ]) {
      process.env.APP_VERSIONS_LOOKBACK_DAYS = days;
      if (buffer === undefined)
        delete process.env.APP_VERSIONS_PARTITION_BUFFER_DAYS;
      else process.env.APP_VERSIONS_PARTITION_BUFFER_DAYS = buffer;
      const calls: { query: string; params: Record<string, unknown> }[] = [];
      Object.defineProperty(BigQuery.prototype, "query", {
        configurable: true,
        value: async (options: (typeof calls)[number]) => {
          calls.push(options);
          return [options.query.includes("INFORMATION_SCHEMA") ? columns : []];
        },
      });
      assert.deepEqual(await appObservations(), []);
      assert.equal(calls.length, 2, "reuse the existing schema round-trip");
      const request = calls[1];
      assert.match(
        request.query,
        /`timestamp` >= TIMESTAMP_SUB\(CURRENT_TIMESTAMP\(\), INTERVAL @lookback_days DAY\)/,
      );
      assert.equal(
        (request.query.match(/@partition_lookback_days/g) || []).length,
        predicates,
      );
      assert.deepEqual(request.params, {
        lookback_days: Number(days),
        ...(expected === undefined
          ? {}
          : { partition_lookback_days: expected }),
      });
      if (predicates)
        assert.match(
          request.query,
          /`_PARTITIONTIME` >= TIMESTAMP_TRUNC\(TIMESTAMP_SUB\(CURRENT_TIMESTAMP\(\), INTERVAL @partition_lookback_days DAY\), DAY\)/,
        );
      else assert.doesNotMatch(request.query, /_PARTITIONTIME/);
      if (columns.some((c) => c.table_name === "screens"))
        assert.match(
          request.query,
          /FROM `analytics-project\.apollos\.screens` WHERE `timestamp` >= TIMESTAMP_SUB\(CURRENT_TIMESTAMP\(\), INTERVAL @lookback_days DAY\)\s*\)/,
        );
    }
  } finally {
    Object.defineProperty(BigQuery.prototype, "query", descriptor);
    for (const [name, value] of Object.entries(env))
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
  }
});
