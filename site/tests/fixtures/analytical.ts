import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

import { DuckDBInstance } from "@duckdb/node-api"

import { makePublicationFixture } from "./publication"

export function analyticalSummary(completed = 120, elapsed = 65) {
  return {
    offered: completed,
    completed,
    errors: 0,
    timeouts: 0,
    throughput_per_second: completed / elapsed,
    latency_ms: {
      n: completed,
      percentile_method: "nearest_rank",
      ...Object.fromEntries(
        ([50, 95, 99] as const).map((percentile, index) => {
          const minimum = [5, 100, 1000][index]!
          return [
            `p${percentile}`,
            {
              value_ms: completed >= minimum ? percentile : null,
              status: completed >= minimum ? "available" : "insufficient_samples",
              minimum_n: minimum,
            },
          ]
        }),
      ),
    },
  }
}

export function analyticalMetadata(clients = 4, completed = 120) {
  return {
    version: 1,
    track: "analytical-streams-v1",
    clients,
    duration_seconds: 60,
    actual_elapsed_seconds: 65,
    measurement_started_at: "2026-10-22T10:00:00+00:00",
    measurement_finished_at: "2026-10-22T10:01:05+00:00",
    drain_seconds: 5,
    cancelled: false,
    query_manifest: {
      algorithm: "sha256",
      digest: "a".repeat(64),
      files: { Q1: "b".repeat(64), Q2: "c".repeat(64) },
    },
    excluded_queries: {
      Q28: { status: "unsupported", reason: "Documented analytical fixture skip" },
    },
    overall: analyticalSummary(completed),
    per_query: { Q1: analyticalSummary(completed), Q2: analyticalSummary(0) },
    budget: {
      configured_threads: 10,
      configured_engine_memory_bytes: 24 * 1024 ** 3,
      client_rss_guard_bytes: 28 * 1024 ** 3,
      client_rss_scope: "Sampled operation-process RSS, all client threads; not whole-host memory",
      multiplied_by_clients: false,
      scope: "Shared engine pool; fixture readback only",
      effective: { thread_pool_size: 10 },
    },
    worker_runtime: Object.fromEntries(
      Array.from({ length: clients }, (_, index) => [
        String(index),
        {
          runtime: { thread_pool_size: 10 },
          cache_attestation: "Cache hits are not instrumented",
        },
      ]),
    ),
  }
}

export async function makeAnalyticalFixture(): Promise<{
  buffer: Buffer
  cleanup: () => Promise<void>
}> {
  const publication = await makePublicationFixture()
  const directory = await mkdtemp(join(tmpdir(), "olap-analytical-"))
  const path = join(directory, "fixture.db")
  await writeFile(path, publication.buffer)
  const instance = await DuckDBInstance.create()
  try {
    const connection = await instance.connect()
    try {
      await connection.run(`attach '${path.replaceAll("'", "''")}' as evidence`)
      const runs = [
        {
          id: 100,
          db: "duckdb",
          version: "1.5.6",
          status: "completed",
          analytical: analyticalMetadata(1, 4),
        },
        {
          id: 101,
          db: "polars",
          version: "1.35.0",
          status: "completed",
          analytical: analyticalMetadata(),
        },
        {
          id: 102,
          db: "postgres",
          version: "17.6",
          status: "failed",
          analytical: {
            ...analyticalMetadata(16, 8),
            cancelled: true,
            overall: { ...analyticalSummary(8), offered: 10, errors: 1, timeouts: 1 },
          },
        },
        { id: 103, db: "duckdb", version: "1.4.9", status: "failed", analytical: null },
      ]
      for (const run of runs) {
        const metadata = JSON.stringify({ analytical: run.analytical }).replaceAll("'", "''")
        await connection.run(`insert into evidence.run
          (id, db, db_version, system, suite, suite_scale_factor, operation, status, started_at, finished_at, benchmark_profile, metadata)
          values (${run.id}, '${run.db}', '${run.version}', 'fixture', 'clickbench', 1, 'analytical', '${run.status}',
          '2026-10-22 10:00:00', '2026-10-22 10:01:05', 'analytical-streams-v1', '${metadata}')`)
      }
      await connection.run(`insert into evidence.run (id, operation, benchmark_profile) values
        (104, 'analytical', 'other-profile'), (105, 'populate', 'analytical-streams-v1')`)
      await connection.run("detach evidence")
    } finally {
      connection.closeSync()
    }
  } finally {
    instance.closeSync()
    await publication.cleanup()
  }
  return {
    buffer: await readFile(path),
    cleanup: () => rm(directory, { recursive: true, force: true }),
  }
}
