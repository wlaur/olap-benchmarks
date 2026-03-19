import { sql } from "kysely"

import type { BenchmarkSuiteId } from "./benchmarks"
import { getKyselyDb } from "./duckdb"
import type {
  BenchmarkOperation,
  InsertStep,
  MetricSample,
  OperationSummary,
  QueriesManifest,
  QueryStep,
  QuerySummary,
  RunSummary,
  StepMetricAvailability,
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

export async function fetchStepMetricAvailability(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<StepMetricAvailability[]> {
  const db = await getKyselyDb()

  const rows = await sql<StepMetricAvailability>`
    WITH latest_runs AS (
      SELECT
        run.id AS run_id,
        run.db,
        run.operation,
        row_number() OVER (
          PARTITION BY run.db, run.operation
          ORDER BY run.finished_at DESC, run.id DESC
        ) AS run_rank
      FROM run
      WHERE run.suite = ${suite}
        AND run.system = ${system}
        AND run.status = 'completed'
        AND run.finished_at IS NOT NULL
        AND run.operation IN ('populate', 'mutate', 'select')
    ),
    step_windows AS (
      SELECT
        lr.operation,
        CASE WHEN rs.step_type = 'phase' THEN rs.table_name ELSE rs.query_name END AS step_name,
        lr.run_id,
        MIN(rs.started_at) AS window_start,
        MAX(rs.finished_at) AS window_end
      FROM run_step rs
      INNER JOIN latest_runs lr ON lr.run_id = rs.run_id
      WHERE lr.run_rank = 1
        AND rs.status = 'completed'
        AND rs.finished_at IS NOT NULL
        AND (
          (rs.step_type = 'phase' AND rs.step_name = 'insert')
          OR rs.step_type = 'query'
          OR rs.step_type = 'mutation'
        )
      GROUP BY lr.operation, CASE WHEN rs.step_type = 'phase' THEN rs.table_name ELSE rs.query_name END, lr.run_id
    ),
    per_db_counts AS (
      SELECT
        lr.db,
        sw.operation,
        sw.step_name,
        count(rm.run_id) AS sample_count
      FROM step_windows sw
      INNER JOIN latest_runs lr
        ON lr.run_id = sw.run_id
      LEFT JOIN run_metric rm
        ON rm.run_id = sw.run_id
        AND rm.time >= sw.window_start
        AND rm.time <= sw.window_end
      GROUP BY lr.db, sw.operation, sw.step_name, sw.run_id
    )
    SELECT db, operation, step_name
    FROM per_db_counts
    WHERE sample_count >= 2
  `.execute(db)

  return rows.rows
}
