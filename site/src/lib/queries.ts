import { sql } from "kysely"

import type { BenchmarkSuiteId } from "./benchmarks"
import { getKyselyDb } from "./duckdb"
import type {
  BenchmarkOperation,
  FlameSpan,
  InsertStep,
  MetricSample,
  OperationSummary,
  QueriesManifest,
  QueryStep,
  QuerySummary,
  RunSummary,
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

export async function fetchRunSummaries(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<RunSummary[]> {
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
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "select")
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

export async function fetchOperationSummaries(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<OperationSummary[]> {
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
        .where("run.suite", "=", suite)
        .where("run.operation", "in", ["populate", "mutate", "select"])
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("latest_runs")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      sql<BenchmarkOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
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

export async function fetchQuerySummaries(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<QuerySummary[]> {
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
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "select")
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

export async function fetchMetricSamples(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<MetricSample[]> {
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
        .where("run.suite", "=", suite)
        .where("run.operation", "in", ["populate", "mutate", "select"])
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
      sql<BenchmarkOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
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

export async function fetchInsertSteps(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<InsertStep[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          eb.ref("run.started_at").as("run_started_at"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "populate")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      eb.ref("run_step.table_name").$notNull().as("table_name"),
      sql<string>`strftime(${eb.ref("run_step.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "started_at",
      ),
      sql<string>`strftime(${eb.ref("run_step.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`.as(
        "duration_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.started_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "elapsed_end_s",
      ),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "phase")
    .where("run_step.step_name", "=", "insert")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.table_name", "is not", null)
    .orderBy("latest_runs.db")
    .orderBy("run_step.started_at")
    .execute()
}

export async function fetchQuerySteps(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<QueryStep[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.started_at").as("run_started_at"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "select")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("run_step.query_name").$notNull().as("query_name"),
      eb.ref("run_step.iteration").$notNull().as("iteration"),
      sql<string>`strftime(${eb.ref("run_step.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "started_at",
      ),
      sql<string>`strftime(${eb.ref("run_step.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`.as(
        "duration_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.started_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "elapsed_end_s",
      ),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "query")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .where("run_step.iteration", "is not", null)
    .orderBy("latest_runs.db")
    .orderBy("run_step.started_at")
    .execute()
}

export async function fetchMutateSummaries(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<QuerySummary[]> {
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
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "mutate")
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
    .where("run_step.step_type", "=", "mutation")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .groupBy(["run_step.query_name", "latest_runs.db"])
    .orderBy("run_step.query_name")
    .orderBy("latest_runs.db")
    .execute()
}

export async function fetchMutateSteps(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<QueryStep[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.started_at").as("run_started_at"),
          sql<number>`row_number() over (
            partition by ${eb.ref("run.db")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "mutate")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null)
        .where("run.system", "=", system),
    )
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db").as("db"),
      eb.ref("run_step.query_name").$notNull().as("query_name"),
      eb.ref("run_step.iteration").$notNull().as("iteration"),
      sql<string>`strftime(${eb.ref("run_step.started_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "started_at",
      ),
      sql<string>`strftime(${eb.ref("run_step.finished_at")}, ${ISO_TIMESTAMP_FORMAT})`.as(
        "finished_at",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`.as(
        "duration_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.started_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "elapsed_end_s",
      ),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "mutation")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .where("run_step.iteration", "is not", null)
    .orderBy("latest_runs.db")
    .orderBy("run_step.started_at")
    .execute()
}

export async function fetchFlameSpans(
  system: string,
  suite: BenchmarkSuiteId,
  targetDb: string,
): Promise<FlameSpan[]> {
  const db = await getKyselyDb()

  const operationOrder = sql<number>`CASE
    WHEN ${sql.ref("latest_runs.operation")} = 'populate' THEN 0
    WHEN ${sql.ref("latest_runs.operation")} = 'mutate'   THEN 1
    WHEN ${sql.ref("latest_runs.operation")} = 'select'   THEN 2
    ELSE 3
  END`

  const latestRuns = db
    .selectFrom("run")
    .select((eb) => [
      eb.ref("run.id").as("run_id"),
      eb.ref("run.db").as("db"),
      eb.ref("run.operation").as("operation"),
      eb.ref("run.started_at").as("run_started_at"),
      eb.ref("run.finished_at").$notNull().as("run_finished_at"),
      sql<number>`row_number() over (
        partition by ${eb.ref("run.operation")}
        order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
      )`.as("run_rank"),
    ])
    .where("run.suite", "=", suite)
    .where("run.db", "=", targetDb)
    .where("run.operation", "in", ["populate", "mutate", "select"])
    .where("run.status", "=", "completed")
    .where("run.finished_at", "is not", null)
    .where("run.system", "=", system)

  // Fetch operation-level spans
  const operationSpans = await db
    .with("latest_runs", () => latestRuns)
    .with("global_start", (qb) =>
      qb
        .selectFrom("latest_runs")
        .select((eb) => [eb.fn.min("latest_runs.run_started_at").as("min_start")])
        .where("latest_runs.run_rank", "=", 1),
    )
    .selectFrom("latest_runs")
    .crossJoin("global_start")
    .select((eb) => [
      sql<string>`'op_' || ${eb.ref("latest_runs.operation")}`.as("id"),
      sql<string>`${eb.ref("latest_runs.db")}`.as("db"),
      sql<BenchmarkOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
      sql<string>`${eb.ref("latest_runs.operation")}`.as("step_name"),
      sql<string | null>`NULL`.as("query_name"),
      sql<string | null>`NULL`.as("query_sql"),
      sql<number | null>`NULL`.as("iteration"),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("latest_runs.run_started_at")} - ${eb.ref("global_start.min_start")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("latest_runs.run_finished_at")} - ${eb.ref("global_start.min_start")}))`.as(
        "elapsed_end_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("latest_runs.run_finished_at")} - ${eb.ref("latest_runs.run_started_at")}))`.as(
        "duration_s",
      ),
      sql<"operation">`'operation'`.as("depth"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .orderBy(operationOrder)
    .execute()

  // Fetch step-level spans
  const stepSpans = await db
    .with("latest_runs", () => latestRuns)
    .with("global_start", (qb) =>
      qb
        .selectFrom("latest_runs")
        .select((eb) => [eb.fn.min("latest_runs.run_started_at").as("min_start")])
        .where("latest_runs.run_rank", "=", 1),
    )
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .crossJoin("global_start")
    .select((eb) => [
      sql<string>`'step_' || ${eb.ref("run_step.id")}`.as("id"),
      sql<string>`${eb.ref("latest_runs.db")}`.as("db"),
      sql<BenchmarkOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
      eb.ref("run_step.step_name").as("step_name"),
      eb.ref("run_step.query_name").as("query_name"),
      sql<string | null>`NULL`.as("query_sql"),
      eb.ref("run_step.iteration").as("iteration"),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.started_at")} - ${eb.ref("global_start.min_start")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("global_start.min_start")}))`.as(
        "elapsed_end_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`.as(
        "duration_s",
      ),
      sql<"step">`'step'`.as("depth"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .orderBy(operationOrder)
    .orderBy("run_step.started_at")
    .execute()

  // Fetch query-execution-level spans
  const querySpans = await db
    .with("latest_runs", () => latestRuns)
    .with("global_start", (qb) =>
      qb
        .selectFrom("latest_runs")
        .select((eb) => [eb.fn.min("latest_runs.run_started_at").as("min_start")])
        .where("latest_runs.run_rank", "=", 1),
    )
    .selectFrom("query_execution")
    .innerJoin("run_step", "run_step.id", "query_execution.run_step_id")
    .innerJoin("latest_runs", "latest_runs.run_id", "query_execution.run_id")
    .crossJoin("global_start")
    .select((eb) => [
      sql<string>`'qe_' || ${eb.ref("query_execution.id")}`.as("id"),
      sql<string>`${eb.ref("latest_runs.db")}`.as("db"),
      sql<BenchmarkOperation>`${eb.ref("latest_runs.operation")}`.as("operation"),
      eb.ref("run_step.step_name").as("step_name"),
      eb.ref("run_step.query_name").as("query_name"),
      eb.ref("query_execution.query").as("query_sql"),
      eb.ref("run_step.iteration").as("iteration"),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("query_execution.start_time")} - ${eb.ref("global_start.min_start")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("query_execution.end_time")} - ${eb.ref("global_start.min_start")}))`.as(
        "elapsed_end_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("query_execution.end_time")} - ${eb.ref("query_execution.start_time")}))`.as(
        "duration_s",
      ),
      sql<"query">`'query'`.as("depth"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .orderBy(operationOrder)
    .orderBy("query_execution.start_time")
    .execute()

  return [...operationSpans, ...stepSpans, ...querySpans]
}
