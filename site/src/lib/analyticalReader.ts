import { sql } from "kysely"

import { ANALYTICAL_PROFILE, parseAnalyticalRun, type AnalyticalRun } from "./analytical"
import { getKyselyDb } from "./duckdb"
import { fetchQueriesManifest } from "./queries"

let dataPromise: Promise<AnalyticalRun[]> | null = null

export function fetchAnalyticalRuns(): Promise<AnalyticalRun[]> {
  dataPromise ??= loadAnalyticalRuns()
  return dataPromise
}

async function loadAnalyticalRuns(): Promise<AnalyticalRun[]> {
  const db = await getKyselyDb()
  const rows = await db
    .selectFrom("run")
    .select([
      "id",
      "suite",
      "suite_scale_factor",
      "system",
      "db",
      "db_version",
      "db_driver",
      "status",
      "error_message",
      sql<string | null>`cast(finished_at as varchar)`.as("finished_at"),
      sql<string | null>`cast(metadata as varchar)`.as("run_metadata"),
    ])
    .where("operation", "=", "analytical")
    .where("benchmark_profile", "=", ANALYTICAL_PROFILE)
    .orderBy("suite")
    .orderBy("suite_scale_factor")
    .orderBy("system")
    .orderBy("db")
    .orderBy(sql`coalesce(finished_at, started_at)`, "desc")
    .orderBy("id", "desc")
    .execute()
  if (rows.length === 0) return []
  const canonical = await fetchQueriesManifest().catch(() => null)
  return rows.map((row) =>
    parseAnalyticalRun(row, canonical?.[row.suite] ? Object.keys(canonical[row.suite]!) : null),
  )
}
