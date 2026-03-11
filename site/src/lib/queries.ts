import { sql } from "kysely"

import { getKyselyDb } from "./duckdb"
import type { QueriesManifest, TimeSeriesQuerySummary, TimeSeriesRunSummary } from "./types"

const ISO_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

let queriesCache: QueriesManifest | null = null

export async function fetchQueriesManifest(): Promise<QueriesManifest> {
  if (queriesCache) return queriesCache

  const response = await fetch(`${import.meta.env.BASE_URL}data/queries.json`)
  if (!response.ok) {
    throw new Error(`Failed to load queries.json: ${response.status}`)
  }
  queriesCache = (await response.json()) as QueriesManifest
  return queriesCache
}

export async function fetchSystems(): Promise<string[]> {
  const db = await getKyselyDb()
  const rows = await db
    .selectFrom("run")
    .select("system")
    .distinct()
    .where("status", "=", "completed")
    .where("finished_at", "is not", null)
    .orderBy("system")
    .execute()

  return rows.map((row) => row.system)
}

export async function fetchTimeSeriesRunSummaries(system: string): Promise<TimeSeriesRunSummary[]> {
  const db = await getKyselyDb()

  return db
    .selectFrom("run")
    .leftJoin("run_step", "run_step.run_id", "run.id")
    .select((eb) => [
      eb.ref("run.id").as("run_id"),
      eb.ref("run.db").as("db"),
      eb.ref("run.db_version").as("db_version"),
      sql<string>`strftime(${eb.ref("run.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as("started_at"),
      sql<string>`strftime(${eb.ref("run.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run.finished_at")} - ${eb.ref("run.started_at")}))`.as(
        "run_duration_s",
      ),
      sql<number | null>`median(
          EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))
        ) FILTER (
          WHERE ${eb.ref("run_step.step_type")} = 'query'
            AND ${eb.ref("run_step.status")} = 'completed'
            AND ${eb.ref("run_step.finished_at")} IS NOT NULL
        )`.as("median_query_duration_s"),
      sql<number>`cast(
          count(${eb.ref("run_step.id")}) FILTER (
            WHERE ${eb.ref("run_step.step_type")} = 'query'
              AND ${eb.ref("run_step.status")} = 'completed'
              AND ${eb.ref("run_step.finished_at")} IS NOT NULL
          ) as integer
        )`.as("query_count"),
    ])
    .where("run.suite", "=", "time_series")
    .where("run.operation", "=", "run")
    .where("run.status", "=", "completed")
    .where("run.finished_at", "is not", null)
    .where("run.system", "=", system)
    .groupBy(["run.id", "run.db", "run.db_version", "run.started_at", "run.finished_at"])
    .orderBy(sql`run_duration_s`)
    .orderBy("run.db")
    .execute()
}

export async function fetchTimeSeriesQuerySummaries(
  system: string,
): Promise<TimeSeriesQuerySummary[]> {
  const db = await getKyselyDb()

  return db
    .selectFrom("run_step")
    .innerJoin("run", "run.id", "run_step.run_id")
    .select((eb) => [
      eb.ref("run_step.query_name").$notNull().as("query_name"),
      eb.ref("run.db").as("db"),
      sql<number>`median(
          EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))
        )`.as("median_duration_s"),
      sql<number>`avg(
          EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))
        )`.as("avg_duration_s"),
      sql<number>`min(
          EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))
        )`.as("min_duration_s"),
      sql<number>`max(
          EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))
        )`.as("max_duration_s"),
      sql<number>`cast(count(*) as integer)`.as("iterations"),
    ])
    .where("run.suite", "=", "time_series")
    .where("run.operation", "=", "run")
    .where("run.status", "=", "completed")
    .where("run.finished_at", "is not", null)
    .where("run.system", "=", system)
    .where("run_step.step_type", "=", "query")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .groupBy(["run_step.query_name", "run.db"])
    .orderBy("run_step.query_name")
    .orderBy("run.db")
    .execute()
}
