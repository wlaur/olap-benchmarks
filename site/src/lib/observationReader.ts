import { sql } from "kysely"

import { fetchComparisonManifest, record, type ComparisonManifest } from "./comparison"
import { getKyselyDb } from "./duckdb"
import type { IterationObservation, RunObservation } from "./observations"
import { validateSelectedCertificates } from "./overallEligibility"
import { fetchProfileScope } from "./queries"

export interface ComparisonData {
  manifest: ComparisonManifest
  runs: RunObservation[]
}

function parseMetadata(value: string | null): Record<string, unknown> {
  return value === null ? {} : record(JSON.parse(value))
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null
}

let dataPromise: Promise<ComparisonData> | null = null

export function fetchComparisonData(): Promise<ComparisonData> {
  dataPromise ??= loadComparisonData()
  return dataPromise
}

async function loadComparisonData(): Promise<ComparisonData> {
  const manifest = await fetchComparisonManifest()
  const [db, scope] = await Promise.all([getKyselyDb(), fetchProfileScope()])
  if (!scope.active) {
    validateSelectedCertificates(manifest, [])
    return { manifest, runs: [] }
  }
  const runRows = await db
    .selectFrom("run")
    .leftJoin("system_snapshot", "system_snapshot.id", "run.system_snapshot_id")
    .select([
      "run.id",
      "run.db",
      "run.db_version",
      "run.db_driver",
      "run.system",
      "run.suite",
      "run.suite_scale_factor",
      "run.status",
      "run.system_snapshot_id",
      "run.error_type",
      "run.error_message",
      "system_snapshot.os",
      "system_snapshot.machine",
      "system_snapshot.cpu_count_logical",
      "system_snapshot.memory_total_mb",
      sql<string | null>`cast(run.metadata as varchar)`.as("run_metadata"),
      sql<string>`cast(coalesce(run.finished_at, run.started_at) as varchar)`.as("finished_at"),
      sql<boolean>`run.finished_at is not null`.as("terminal"),
    ])
    .where("run.operation", "=", "select")
    .where("run.benchmark_profile", "=", scope.active.id)
    .orderBy(sql`coalesce(run.finished_at, run.started_at)`, "desc")
    .orderBy("run.id", "desc")
    .execute()
  const latest = new Map<string, RunObservation>()
  for (const row of runRows) {
    const key = JSON.stringify([
      row.system,
      row.suite,
      row.suite_scale_factor,
      row.db,
      row.db_version,
      row.db_driver,
    ])
    if (latest.has(key)) continue
    latest.set(key, {
      runId: row.id,
      db: row.db,
      version: row.db_version,
      driver: row.db_driver,
      system: row.system,
      suite: row.suite,
      scale: row.suite_scale_factor,
      status: row.status,
      finishedAt: manifest.version === 2 && !row.terminal ? "" : row.finished_at,
      snapshotId: row.system_snapshot_id,
      metadata: parseMetadata(row.run_metadata),
      snapshot: {
        os: row.os,
        machine: row.machine,
        logical_cpus: row.cpu_count_logical,
        memory_mb: row.memory_total_mb,
      },
      error: row.error_message ?? row.error_type,
      queries: new Map(),
    })
  }
  const runs = [...latest.values()]
  validateSelectedCertificates(manifest, runs)
  if (runs.length === 0) return { manifest, runs }
  const byRun = new Map(runs.map((run) => [run.runId, run]))
  const steps = await db
    .selectFrom("run_step")
    .select([
      "id",
      "run_id",
      "query_name",
      "iteration",
      "iteration_role",
      "status",
      "result_status",
      "row_count",
      "error_type",
      "error_message",
      sql<string | null>`cast(metadata as varchar)`.as("step_metadata"),
    ])
    .where(
      "run_id",
      "in",
      runs.map((run) => run.runId),
    )
    .where("step_type", "=", "query")
    .where("query_name", "is not", null)
    .orderBy("id")
    .execute()
  for (const step of steps) {
    const run = byRun.get(step.run_id)!
    const metadata = parseMetadata(step.step_metadata)
    const observation: IterationObservation = {
      id: step.id,
      queryName: step.query_name!,
      iteration: step.iteration,
      role: step.iteration_role,
      status: step.status,
      resultStatus: step.result_status,
      rowCount: step.row_count,
      durationS: typeof metadata.duration_ms === "number" ? metadata.duration_ms / 1000 : null,
      hash: text(metadata.answer_hash),
      hashVersion: text(metadata.answer_hash_version),
      hashMissingReason: text(metadata.answer_hash_skipped_reason),
      skipReason: text(metadata.skip_reason),
      error: step.error_message ?? step.error_type,
    }
    const observations = run.queries.get(observation.queryName) ?? []
    observations.push(observation)
    run.queries.set(observation.queryName, observations)
  }
  return { manifest, runs }
}
