import { query } from "./duckdb"
import type { Filters, Run, RunStep } from "./types"

export async function fetchSystems(): Promise<string[]> {
  const rows = await query<{ system: string }>(
    "SELECT DISTINCT system FROM results.run ORDER BY system",
  )
  return rows.map((r) => r.system)
}

export async function fetchSuites(system: string): Promise<string[]> {
  const rows = await query<{ suite: string }>(
    `SELECT DISTINCT suite FROM results.run WHERE system = '${escape(system)}' ORDER BY suite`,
  )
  return rows.map((r) => r.suite)
}

export async function fetchDatabases(system: string): Promise<string[]> {
  const rows = await query<{ db: string }>(
    `SELECT DISTINCT db FROM results.run WHERE system = '${escape(system)}' ORDER BY db`,
  )
  return rows.map((r) => r.db)
}

export async function fetchRuns(filters: Filters): Promise<Run[]> {
  const clauses: string[] = []
  if (filters.system) clauses.push(`system = '${escape(filters.system)}'`)
  if (filters.suite) clauses.push(`suite = '${escape(filters.suite)}'`)
  if (filters.db) clauses.push(`db = '${escape(filters.db)}'`)
  if (filters.operation)
    clauses.push(`operation = '${escape(filters.operation)}'`)

  const where = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : ""
  return query<Run>(
    `SELECT * FROM results.run ${where} ORDER BY started_at DESC`,
  )
}

export async function fetchRunSteps(runId: number): Promise<RunStep[]> {
  return query<RunStep>(
    `SELECT * FROM results.run_step WHERE run_id = ${runId} ORDER BY started_at`,
  )
}

function escape(value: string): string {
  return value.replace(/'/g, "''")
}
