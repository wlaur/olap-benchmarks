import { query } from "./duckdb"
import type { Filters, Run, RunStep } from "./types"

const ISO_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
const COMPLETED_RUN_CLAUSE = "status = 'completed' AND finished_at IS NOT NULL"
const COMPLETED_STEP_CLAUSE = "status = 'completed' AND finished_at IS NOT NULL"

export async function fetchSystems(): Promise<string[]> {
  const rows = await query<{ system: string }>(
    `SELECT DISTINCT system FROM results.run WHERE ${COMPLETED_RUN_CLAUSE} ORDER BY system`,
  )
  return rows.map((r) => r.system)
}

export async function fetchSuites(system: string): Promise<string[]> {
  const rows = await query<{ suite: string }>(
    `SELECT DISTINCT suite
     FROM results.run
     WHERE ${COMPLETED_RUN_CLAUSE} AND system = '${escape(system)}'
     ORDER BY suite`,
  )
  return rows.map((r) => r.suite)
}

export async function fetchDatabases(system: string): Promise<string[]> {
  const rows = await query<{ db: string }>(
    `SELECT DISTINCT db
     FROM results.run
     WHERE ${COMPLETED_RUN_CLAUSE} AND system = '${escape(system)}'
     ORDER BY db`,
  )
  return rows.map((r) => r.db)
}

export async function fetchRuns(filters: Filters): Promise<Run[]> {
  const clauses = [COMPLETED_RUN_CLAUSE]
  if (filters.system) clauses.push(`system = '${escape(filters.system)}'`)
  if (filters.suite) clauses.push(`suite = '${escape(filters.suite)}'`)
  if (filters.db) clauses.push(`db = '${escape(filters.db)}'`)
  if (filters.operation)
    clauses.push(`operation = '${escape(filters.operation)}'`)

  const where = `WHERE ${clauses.join(" AND ")}`
  return query<Run>(
    `SELECT id,
            suite,
            db,
            db_version,
            operation,
            system,
            strftime(started_at, '${ISO_TIMESTAMP_FORMAT}') AS started_at,
            strftime(finished_at, '${ISO_TIMESTAMP_FORMAT}') AS finished_at,
            EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_s,
            error_type,
            error_message
     FROM results.run
     ${where}
     ORDER BY started_at DESC`,
  )
}

export async function fetchRunSteps(runId: number): Promise<RunStep[]> {
  return query<RunStep>(
    `SELECT id,
            run_id,
            step_type,
            step_name,
            query_name,
            iteration,
            table_name,
            strftime(started_at, '${ISO_TIMESTAMP_FORMAT}') AS started_at,
            strftime(finished_at, '${ISO_TIMESTAMP_FORMAT}') AS finished_at,
            row_count,
            error_type,
            error_message
     FROM results.run_step
     WHERE run_id = ${runId} AND ${COMPLETED_STEP_CLAUSE}
     ORDER BY started_at`,
  )
}

function escape(value: string): string {
  return value.replace(/'/g, "''")
}
