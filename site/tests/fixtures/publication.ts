import { createHash } from "node:crypto"
import { mkdtemp, readFile, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

import { DuckDBInstance } from "@duckdb/node-api"
import type { Page } from "@playwright/test"

const SHA = "a".repeat(64)
export const comparisonFixture = {
  version: 1,
  policy_id: "fixed-workloads-v1",
  results_db_sha256: SHA,
  retrospective: true,
  families: [
    {
      id: "clickbench",
      suite: "clickbench",
      title: "ClickBench",
      weight: 1,
      scale_factors: [1],
      query_names: ["Q1", "Q2"],
      coverage_query_names: ["Q1", "Q2", "Q28"],
      track: "raw",
    },
  ],
  registrations: [
    "fast",
    "slow",
    "partial",
    "wrong",
    "invalidated",
    "latestfailed",
    "preparation",
    "missing",
  ]
    .map((db) => ({ db, suite: "clickbench", scale_factors: [1], operations: ["select"] }))
    .concat([{ db: "specialist", suite: "rtabench", scale_factors: [1], operations: ["select"] }]),
  answer_comparison_exclusions: [
    { suite: "clickbench", query_name: "Q28", db: null, reason: "Regex semantics differ" },
  ],
  annotations: [
    {
      db: "preparation",
      suite: "clickbench",
      scale_factor: 1,
      run_id: 7,
      preparation: "incomplete",
      tuning: null,
      summary: "Index preparation was not completed.",
      evidence_url: "https://example.com/preparation",
    },
  ],
}

function literal(value: unknown): string {
  if (value === null) return "null"
  if (typeof value === "number") return String(value)
  return `'${String(value).replaceAll("'", "''")}'`
}

export async function makePublicationFixture(certifiedPartial = false): Promise<{
  buffer: Buffer
  cleanup: () => Promise<void>
}> {
  const directory = await mkdtemp(join(tmpdir(), "olap-publication-"))
  const path = join(directory, "fixture.db")
  const instance = await DuckDBInstance.create()
  try {
    const connection = await instance.connect()
    try {
      await connection.run(`attach ${literal(path)} as publication (storage_version 'v1.4.0')`)
      await connection.run(
        `create table publication.run (id integer, db varchar, db_version varchar, db_driver varchar, system varchar, suite varchar, suite_scale_factor integer, operation varchar, status varchar, started_at timestamp, finished_at timestamp, system_snapshot_id integer, error_type varchar, error_message varchar, benchmark_profile varchar, metadata json)`,
      )
      await connection.run(
        `create table publication.system_snapshot (id integer, os varchar, machine varchar, cpu_count_logical integer, memory_total_mb integer)`,
      )
      await connection.run(
        `insert into publication.system_snapshot values (1, 'test', 'test', 4, 8192)`,
      )
      await connection.run(
        `create table publication.run_step (id integer, run_id integer, step_type varchar, query_name varchar, iteration integer, iteration_role varchar, status varchar, result_status varchar, row_count integer, error_type varchar, error_message varchar, metadata json)`,
      )
      const engines = [
        "fast",
        "slow",
        "partial",
        "wrong",
        "invalidated",
        "latestfailed",
        "preparation",
        "latestfailed",
      ]
      let stepId = 0
      for (const [index, db] of engines.entries()) {
        const runId = index + 1
        const failed = runId === 8
        const metadata = {
          benchmark_profile: {
            id: "resident-hot-v1",
            warmup_iterations: 1,
            measured_iterations: 3,
          },
          input: { manifest_sha256: "same-input" },
          ...(db === "invalidated" ? { invalidation: { reason: "bad preparation" } } : {}),
        }
        const row = [
          runId,
          db,
          "1",
          null,
          "fixture",
          "clickbench",
          1,
          "select",
          failed ? "failed" : "completed",
          `2026-10-01 00:00:${String(runId).padStart(2, "0")}`,
          `2026-10-01 00:01:${String(runId).padStart(2, "0")}`,
          1,
          failed ? "TestError" : null,
          failed ? "latest run failed" : null,
          "resident-hot-v1",
          JSON.stringify(metadata),
        ]
        await connection.run(`insert into publication.run values (${row.map(literal).join(",")})`)
        if (failed && !certifiedPartial) continue
        for (const query of ["Q1", "Q2", "Q28"]) {
          if (failed && query !== "Q1") continue
          for (let iteration = 1; iteration <= 4; iteration++) {
            if (db === "partial" && query === "Q2" && iteration === 4) continue
            const status =
              query === "Q28"
                ? "unsupported"
                : db === "wrong" && query === "Q1" && iteration === 2
                  ? "wrong_result"
                  : "ok"
            const metadataStep = {
              duration_ms: (db === "slow" ? 4 : 1) * (query === "Q2" ? 2 : 1),
              answer_hash: `hash-${query}`,
              answer_hash_version: "v1",
              ...(status === "unsupported"
                ? { skip_reason: "Adapter has no implementation of this query." }
                : {}),
            }
            const step = [
              ++stepId,
              runId,
              "query",
              query,
              iteration,
              iteration === 1 ? "warmup" : "measured",
              "completed",
              status,
              1,
              null,
              null,
              JSON.stringify(metadataStep),
            ]
            await connection.run(
              `insert into publication.run_step values (${step.map(literal).join(",")})`,
            )
          }
        }
      }
      await connection.run("detach publication")
    } finally {
      connection.closeSync()
    }
  } finally {
    instance.closeSync()
  }
  return {
    buffer: await readFile(path),
    cleanup: () => rm(directory, { recursive: true, force: true }),
  }
}

export async function usePublicationFixture(
  page: Page,
  buffer: Buffer,
  comparison: unknown = comparisonFixture,
): Promise<void> {
  await page.route("**/data/manifest.json", (route) =>
    route.fulfill({
      json: {
        results_db_sha256:
          (comparison as { version: number }).version === 2
            ? createHash("sha256").update(buffer).digest("hex")
            : SHA,
        results_db_bytes: buffer.byteLength,
        results_db_format: {
          storage_compatibility_version: "v1.4.0",
          minimum_reader_engine_version: "v1.4.0",
          writer_engine_version: "2.0.0",
        },
        publication: { kind: "matrix" },
      },
    }),
  )
  await page.route("**/data/comparison.json", (route) => route.fulfill({ json: comparison }))
  await page.route("**/data/queries.json", (route) =>
    route.fulfill({
      json: {
        clickbench: Object.fromEntries(
          ["Q1", "Q2", "Q28"].map((query) => [query, { sql: "SELECT 1", db_overrides: {} }]),
        ),
      },
    }),
  )
  await page.route("**/data/results.db", (route) =>
    route.fulfill({ body: buffer, contentType: "application/octet-stream" }),
  )
}
