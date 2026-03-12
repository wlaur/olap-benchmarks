import { sql } from "kysely"

import { getKyselyDb } from "./duckdb"
import type {
  QueriesManifest,
  TimeSeriesMetricSample,
  TimeSeriesOperationSummary,
  TimeSeriesOperation,
  TimeSeriesQuerySummary,
  TimeSeriesRunSummary,
} from "./types"

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
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          eb.ref("run.started_at").as("started_at"),
          eb.ref("run.finished_at").$notNull().as("finished_at"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", "time_series")
        .where("run.operation", "=", "run")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("latest_runs")
    .leftJoin("run_step", "run_step.run_id", "latest_runs.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      sql<string>`strftime(${eb.ref("latest_runs.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "started_at",
      ),
      sql<string>`strftime(${eb.ref("latest_runs.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("latest_runs.finished_at")} - ${eb.ref("latest_runs.started_at")}))`.as(
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
    .where("latest_runs.run_rank", "=", 1)
    .groupBy([
      "latest_runs.run_id",
      "latest_runs.db",
      "latest_runs.db_version",
      "latest_runs.started_at",
      "latest_runs.finished_at",
    ])
    .orderBy(sql`run_duration_s`)
    .orderBy("latest_runs.db")
    .execute()
}

export async function fetchTimeSeriesOperationSummaries(
  system: string,
): Promise<TimeSeriesOperationSummary[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          eb.ref("run.operation").as("operation"),
          eb.ref("run.started_at").as("started_at"),
          eb.ref("run.finished_at").$notNull().as("finished_at"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}, ${eb.ref("run.operation")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", "time_series")
        .where("run.operation", "in", ["populate", "run"])
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("latest_runs")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      sql<TimeSeriesOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
      sql<string>`strftime(${eb.ref("latest_runs.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "started_at",
      ),
      sql<string>`strftime(${eb.ref("latest_runs.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("latest_runs.finished_at")} - ${eb.ref("latest_runs.started_at")}))`.as(
        "run_duration_s",
      ),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .orderBy("latest_runs.db")
    .orderBy("latest_runs.operation")
    .execute()
}

export async function fetchTimeSeriesQuerySummaries(
  system: string,
): Promise<TimeSeriesQuerySummary[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", "time_series")
        .where("run.operation", "=", "run")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("run_step.query_name").$notNull().as("query_name"),
      eb.ref("latest_runs.db").as("db"),
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
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "query")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .groupBy(["run_step.query_name", "latest_runs.db"])
    .orderBy("run_step.query_name")
    .orderBy("latest_runs.db")
    .execute()
}

export async function fetchTimeSeriesMetricSamples(
  system: string,
): Promise<TimeSeriesMetricSample[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          eb.ref("run.operation").as("operation"),
          eb.ref("run.started_at").as("started_at"),
          eb.ref("run.finished_at").$notNull().as("finished_at"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}, ${eb.ref("run.operation")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", "time_series")
        .where("run.operation", "in", ["populate", "run"])
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("latest_runs")
    .innerJoin("run_metric", "run_metric.run_id", "latest_runs.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      sql<TimeSeriesOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
      sql<string>`strftime(${eb.ref("latest_runs.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "started_at",
      ),
      sql<string>`strftime(${eb.ref("latest_runs.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<string>`strftime(${eb.ref("run_metric.time")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "sample_time",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_metric.time")} - ${eb.ref("latest_runs.started_at")}))`.as(
        "elapsed_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("latest_runs.finished_at")} - ${eb.ref("latest_runs.started_at")}))`.as(
        "run_duration_s",
      ),
      eb.ref("run_metric.cpu_percent").as("cpu_percent"),
      eb.ref("run_metric.mem_mb").as("mem_mb"),
      eb.ref("run_metric.disk_mb").as("disk_mb"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .orderBy("latest_runs.operation")
    .orderBy("latest_runs.db")
    .orderBy("run_metric.time")
    .execute()
}
