import { type Expression, sql } from "kysely"

import type { BenchmarkSuiteId } from "./benchmarks"
import { getKyselyDb, type ResultsDb } from "./duckdb"
import type {
  CatalogRunDimension,
  ExplorerQueryMetric,
  QueriesManifest,
  QueryCoverage,
  QuerySummary,
  RunStatus,
  SuiteScaleFactor,
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

export async function fetchSystemSuiteScaleFactors(system: string): Promise<SuiteScaleFactor[]> {
  const db = await getKyselyDb()
  return db
    .selectFrom("run")
    .select(["suite", "suite_scale_factor"])
    .distinct()
    .where("system", "=", system)
    .where("status", "!=", "running")
    .orderBy("suite")
    .orderBy("suite_scale_factor")
    .execute()
}

export async function fetchCatalogRunDimensions(): Promise<CatalogRunDimension[]> {
  const db = await getKyselyDb()
  return db
    .selectFrom("run")
    .select(["system", "suite", "suite_scale_factor", "db", "db_version"])
    .distinct()
    .where("operation", "=", "select")
    .where("status", "=", "completed")
    .where("finished_at", "is not", null)
    .orderBy("suite")
    .orderBy("db")
    .orderBy("db_version")
    .orderBy("suite_scale_factor")
    .orderBy("system")
    .execute()
}

export async function fetchExplorerQueryMetrics(
  suite: BenchmarkSuiteId,
): Promise<ExplorerQueryMetric[]> {
  const db = await getKyselyDb()

  return db
    .with("latest_explorer_runs", (qb) =>
      qb
        .selectFrom("run")
        .select([
          "run.id as run_id",
          "run.system",
          "run.suite_scale_factor",
          "run.db",
          "run.db_version",
          "run.finished_at",
        ])
        .select((eb) => [
          sql<number>`row_number() over (
            partition by
              ${eb.ref("run.system")},
              ${eb.ref("run.suite_scale_factor")},
              ${eb.ref("run.db")},
              ${eb.ref("run.db_version")}
            order by ${eb.ref("run.finished_at")} desc, ${eb.ref("run.id")} desc
          )`.as("run_rank"),
        ])
        .where("run.suite", "=", suite)
        .where("run.operation", "=", "select")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null),
    )
    .selectFrom("run_step")
    .innerJoin("latest_explorer_runs", "latest_explorer_runs.run_id", "run_step.run_id")
    .select((eb) => {
      const duration = sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`
      const warm = sql`${eb.ref("run_step.iteration_role")} in ('warm', 'steady_state')`

      return [
        eb.ref("latest_explorer_runs.run_id").as("run_id"),
        eb.ref("latest_explorer_runs.system").as("system"),
        eb.ref("latest_explorer_runs.suite_scale_factor").as("suite_scale_factor"),
        eb.ref("latest_explorer_runs.db").as("db"),
        eb.ref("latest_explorer_runs.db_version").as("db_version"),
        isoTimestamp(eb.ref("latest_explorer_runs.finished_at").$notNull()).as("finished_at"),
        eb.ref("run_step.query_name").$notNull().as("query_name"),
        sql<number>`coalesce(median(${duration}) filter (where ${warm}), median(${duration}))`.as(
          "median_duration_s",
        ),
      ]
    })
    .where("latest_explorer_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "query")
    .where("run_step.status", "=", "completed")
    .where("run_step.result_status", "=", "ok")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .groupBy([
      "latest_explorer_runs.run_id",
      "latest_explorer_runs.system",
      "latest_explorer_runs.suite_scale_factor",
      "latest_explorer_runs.db",
      "latest_explorer_runs.db_version",
      "latest_explorer_runs.finished_at",
      "run_step.query_name",
    ])
    .orderBy("latest_explorer_runs.system")
    .orderBy("latest_explorer_runs.db")
    .orderBy("latest_explorer_runs.db_version")
    .orderBy("latest_explorer_runs.suite_scale_factor")
    .orderBy("run_step.query_name")
    .execute()
}

function isoTimestamp(column: Expression<Date>) {
  return sql<string>`strftime(${column}, ${ISO_TIMESTAMP_FORMAT})`
}

interface LatestRunsOptions {
  system: string
  suite: BenchmarkSuiteId
  suiteScaleFactor: number
}

function withRunLabels(db: ResultsDb, options: LatestRunsOptions) {
  return db.with("run_labels", (qb) =>
    qb
      .selectFrom("run")
      .select((eb) => [
        eb.ref("run.db").as("db"),
        eb.ref("run.db_version").as("db_version"),
        sql<string>`case
            when count(distinct ${eb.ref("run.db_version")}) over (partition by ${eb.ref("run.db")}) > 1
            then ${eb.ref("run.db")} || ' ' || ${eb.ref("run.db_version")}
            else ${eb.ref("run.db")}
          end`.as("db_label"),
      ])
      .distinct()
      .where("run.suite", "=", options.suite)
      .where("run.suite_scale_factor", "=", options.suiteScaleFactor)
      .where("run.system", "=", options.system)
      .where("run.operation", "=", "select")
      .where("run.status", "!=", "running"),
  )
}

function withLatestCompletedSelectRuns(db: ResultsDb, options: LatestRunsOptions) {
  return withRunLabels(db, options)
    .with("scoped_runs", (qb) =>
      qb
        .selectFrom("run")
        .innerJoin("run_labels", (join) =>
          join
            .onRef("run_labels.db", "=", "run.db")
            .onRef("run_labels.db_version", "=", "run.db_version"),
        )
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          eb.ref("run_labels.db_label").as("db_label"),
          eb.ref("run.finished_at").$notNull().as("run_finished_at"),
        ])
        .where("run.suite", "=", options.suite)
        .where("run.suite_scale_factor", "=", options.suiteScaleFactor)
        .where("run.system", "=", options.system)
        .where("run.operation", "=", "select")
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null),
    )
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("scoped_runs")
        .selectAll("scoped_runs")
        .select((eb) => [
          sql<number>`row_number() over (
            partition by ${eb.ref("scoped_runs.db")}, ${eb.ref("scoped_runs.db_version")}
            order by ${eb.ref("scoped_runs.run_finished_at")} desc, ${eb.ref("scoped_runs.run_id")} desc
          )`.as("run_rank"),
        ]),
    )
}

function withLatestAttemptedSelectRuns(db: ResultsDb, options: LatestRunsOptions) {
  return withRunLabels(db, options)
    .with("scoped_runs", (qb) =>
      qb
        .selectFrom("run")
        .innerJoin("run_labels", (join) =>
          join
            .onRef("run_labels.db", "=", "run.db")
            .onRef("run_labels.db_version", "=", "run.db_version"),
        )
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          eb.ref("run_labels.db_label").as("db_label"),
          sql<Exclude<RunStatus, "running">>`${eb.ref("run.status")}`.as("run_status"),
          sql<Date>`coalesce(${eb.ref("run.finished_at")}, ${eb.ref("run.started_at")})`.as(
            "run_finished_at",
          ),
        ])
        .where("run.suite", "=", options.suite)
        .where("run.suite_scale_factor", "=", options.suiteScaleFactor)
        .where("run.system", "=", options.system)
        .where("run.operation", "=", "select")
        .where("run.status", "!=", "running"),
    )
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("scoped_runs")
        .selectAll("scoped_runs")
        .select((eb) => [
          sql<number>`row_number() over (
            partition by ${eb.ref("scoped_runs.db")}, ${eb.ref("scoped_runs.db_version")}
            order by ${eb.ref("scoped_runs.run_finished_at")} desc, ${eb.ref("scoped_runs.run_id")} desc
          )`.as("run_rank"),
        ]),
    )
}

export async function fetchQueryCoverage(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<QueryCoverage[]> {
  const db = await getKyselyDb()

  return withLatestAttemptedSelectRuns(db, { system, suite, suiteScaleFactor })
    .selectFrom("latest_runs")
    .leftJoin("run_step", "run_step.run_id", "latest_runs.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db_label").as("db"),
      eb.ref("latest_runs.db").as("db_name"),
      eb.ref("latest_runs.db_version").as("db_version"),
      eb.ref("latest_runs.run_status").as("latest_status"),
      sql<number>`cast(
        count(distinct ${eb.ref("run_step.query_name")}) filter (
          where ${eb.ref("run_step.step_type")} = 'query'
            and ${eb.ref("run_step.query_name")} is not null
            and ${eb.ref("run_step.result_status")} in ('error', 'timeout', 'wrong_result')
        ) as integer
      )`.as("failed_query_count"),
      sql<number>`cast(
        count(distinct ${eb.ref("run_step.query_name")}) filter (
          where ${eb.ref("run_step.step_type")} = 'query'
            and ${eb.ref("run_step.query_name")} is not null
            and ${eb.ref("run_step.result_status")} in ('ok', 'error', 'timeout', 'unsupported', 'wrong_result')
        ) as integer
      )`.as("attempted_query_count"),
      sql<number>`cast(
        count(distinct ${eb.ref("run_step.query_name")}) filter (
          where ${eb.ref("run_step.step_type")} = 'query'
            and ${eb.ref("run_step.query_name")} is not null
            and ${eb.ref("run_step.result_status")} = 'ok'
        ) as integer
      )`.as("completed_query_count"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .groupBy([
      "latest_runs.run_id",
      "latest_runs.db_label",
      "latest_runs.db",
      "latest_runs.db_version",
      "latest_runs.run_status",
    ])
    .orderBy("latest_runs.db_label")
    .execute()
}

export async function fetchQuerySummaries(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<QuerySummary[]> {
  const db = await getKyselyDb()

  return withLatestCompletedSelectRuns(db, { system, suite, suiteScaleFactor })
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => {
      const duration = sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`
      const warm = sql`${eb.ref("run_step.iteration_role")} in ('warm', 'steady_state')`
      const firstRun = sql`${eb.ref("run_step.iteration_role")} = 'first_run'`

      return [
        eb.ref("run_step.query_name").$notNull().as("query_name"),
        eb.ref("latest_runs.db_label").as("db"),
        eb.ref("latest_runs.db").as("db_name"),
        eb.ref("latest_runs.db_version").as("db_version"),
        sql<number>`coalesce(median(${duration}) filter (where ${warm}), median(${duration}))`.as(
          "median_duration_s",
        ),
        sql<number | null>`min(${duration}) filter (where ${firstRun})`.as("first_run_duration_s"),
        sql<number | null>`median(${duration}) filter (where ${warm})`.as("warm_median_duration_s"),
        sql<number | null>`min(${duration}) filter (where ${warm})`.as("best_warm_duration_s"),
        sql<number>`median(${duration})`.as("all_iterations_median_duration_s"),
        sql<number>`coalesce(avg(${duration}) filter (where ${warm}), avg(${duration}))`.as(
          "avg_duration_s",
        ),
        sql<number>`coalesce(min(${duration}) filter (where ${warm}), min(${duration}))`.as(
          "min_duration_s",
        ),
        sql<number>`coalesce(max(${duration}) filter (where ${warm}), max(${duration}))`.as(
          "max_duration_s",
        ),
        sql<number>`cast(count(*) as integer)`.as("iterations"),
        sql<number>`cast(count(*) filter (where ${warm}) as integer)`.as("warm_iterations"),
      ]
    })
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "query")
    .where("run_step.status", "=", "completed")
    .where("run_step.result_status", "=", "ok")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .groupBy([
      "run_step.query_name",
      "latest_runs.db_label",
      "latest_runs.db",
      "latest_runs.db_version",
    ])
    .orderBy("run_step.query_name")
    .orderBy("latest_runs.db_label")
    .execute()
}
