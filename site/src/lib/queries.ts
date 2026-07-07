import { type Expression, sql } from "kysely"

import type { BenchmarkSuiteId } from "./benchmarks"
import { getKyselyDb, type ResultsDb } from "./duckdb"
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

const BENCHMARK_OPERATIONS: readonly BenchmarkOperation[] = ["populate", "mutate", "select"]

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

export async function fetchSuiteScaleFactors(
  system: string,
  suite: BenchmarkSuiteId,
): Promise<number[]> {
  const db = await getKyselyDb()
  const rows = await db
    .selectFrom("run")
    .select("suite_scale_factor")
    .distinct()
    .where("suite", "=", suite)
    .where("system", "=", system)
    .where("status", "=", "completed")
    .where("finished_at", "is not", null)
    .orderBy("suite_scale_factor")
    .execute()

  return rows.map((row) => row.suite_scale_factor)
}

function isoTimestamp(column: Expression<Date>) {
  return sql<string>`strftime(${column}, ${ISO_TIMESTAMP_FORMAT})`
}

function epochSeconds(end: Expression<Date>, start: Expression<Date>) {
  return sql<number>`EXTRACT(EPOCH FROM (${end} - ${start}))`
}

interface LatestRunsOptions {
  system: string
  suite: BenchmarkSuiteId
  suiteScaleFactor: number
  operations: readonly BenchmarkOperation[]
  /** rank runs per (db, db_version, operation) instead of per (db, db_version) */
  perOperation?: boolean
}

/**
 * Shared "latest run per database variant" CTE.
 *
 * A database variant is (db, db_version). When more than one version of the
 * same db exists within the (system, suite) scope, `db_label` disambiguates
 * as "db version"; with a single version it stays "db". The label window is
 * computed over the whole scope (not per operation) so every panel of a suite
 * page labels the same variant identically.
 */
function withLatestRuns(db: ResultsDb, options: LatestRunsOptions) {
  return db
    .with("scoped_runs", (qb) =>
      qb
        .selectFrom("run")
        .select((eb) => [
          eb.ref("run.id").as("run_id"),
          eb.ref("run.db").as("db"),
          eb.ref("run.db_version").as("db_version"),
          sql<string>`case
            when count(distinct ${eb.ref("run.db_version")}) over (partition by ${eb.ref("run.db")}) > 1
            then ${eb.ref("run.db")} || ' ' || ${eb.ref("run.db_version")}
            else ${eb.ref("run.db")}
          end`.as("db_label"),
          sql<BenchmarkOperation>`${eb.ref("run.operation")}`.as("operation"),
          eb.ref("run.started_at").as("run_started_at"),
          eb.ref("run.finished_at").$notNull().as("run_finished_at"),
        ])
        .where("run.suite", "=", options.suite)
        .where("run.suite_scale_factor", "=", options.suiteScaleFactor)
        .where("run.system", "=", options.system)
        .where("run.operation", "in", [...BENCHMARK_OPERATIONS])
        .where("run.status", "=", "completed")
        .where("run.finished_at", "is not", null),
    )
    .with("latest_runs", (qb) =>
      qb
        .selectFrom("scoped_runs")
        .selectAll("scoped_runs")
        .select((eb) => [
          sql<number>`row_number() over (
            partition by ${eb.ref("scoped_runs.db")}, ${eb.ref("scoped_runs.db_version")}${
              options.perOperation ? sql`, ${eb.ref("scoped_runs.operation")}` : sql``
            }
            order by ${eb.ref("scoped_runs.run_finished_at")} desc, ${eb.ref("scoped_runs.run_id")} desc
          )`.as("run_rank"),
        ])
        .where("scoped_runs.operation", "in", [...options.operations]),
    )
}

export async function fetchRunSummaries(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<RunSummary[]> {
  const db = await getKyselyDb()

  return withLatestRuns(db, { system, suite, suiteScaleFactor, operations: ["select"] })
    .selectFrom("latest_runs")
    .leftJoin("run_step", "run_step.run_id", "latest_runs.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db_label").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      isoTimestamp(eb.ref("latest_runs.run_started_at")).as("started_at"),
      isoTimestamp(eb.ref("latest_runs.run_finished_at")).as("finished_at"),
      epochSeconds(eb.ref("latest_runs.run_finished_at"), eb.ref("latest_runs.run_started_at")).as(
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
      "latest_runs.db_label",
      "latest_runs.db_version",
      "latest_runs.run_started_at",
      "latest_runs.run_finished_at",
    ])
    .orderBy(sql`run_duration_s`)
    .orderBy("latest_runs.db_label")
    .execute()
}

export async function fetchOperationSummaries(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<OperationSummary[]> {
  const db = await getKyselyDb()

  return withLatestRuns(db, {
    system,
    suite,
    suiteScaleFactor,
    operations: BENCHMARK_OPERATIONS,
    perOperation: true,
  })
    .selectFrom("latest_runs")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db_label").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      eb.ref("latest_runs.operation").as("operation"),
      isoTimestamp(eb.ref("latest_runs.run_started_at")).as("started_at"),
      isoTimestamp(eb.ref("latest_runs.run_finished_at")).as("finished_at"),
      epochSeconds(eb.ref("latest_runs.run_finished_at"), eb.ref("latest_runs.run_started_at")).as(
        "run_duration_s",
      ),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .orderBy("latest_runs.db_label")
    .orderBy("latest_runs.operation")
    .execute()
}

async function fetchStepSummaries(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
  operation: BenchmarkOperation,
  stepType: "query" | "mutation",
): Promise<QuerySummary[]> {
  const db = await getKyselyDb()

  return withLatestRuns(db, { system, suite, suiteScaleFactor, operations: [operation] })
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("run_step.query_name").$notNull().as("query_name"),
      eb.ref("latest_runs.db_label").as("db"),
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
    .where("run_step.step_type", "=", stepType)
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .groupBy(["run_step.query_name", "latest_runs.db_label"])
    .orderBy("run_step.query_name")
    .orderBy("latest_runs.db_label")
    .execute()
}

export function fetchQuerySummaries(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<QuerySummary[]> {
  return fetchStepSummaries(system, suite, suiteScaleFactor, "select", "query")
}

export function fetchMutateSummaries(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<QuerySummary[]> {
  return fetchStepSummaries(system, suite, suiteScaleFactor, "mutate", "mutation")
}

export async function fetchMetricSamples(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<MetricSample[]> {
  const db = await getKyselyDb()

  return withLatestRuns(db, {
    system,
    suite,
    suiteScaleFactor,
    operations: BENCHMARK_OPERATIONS,
    perOperation: true,
  })
    .selectFrom("latest_runs")
    .innerJoin("run_metric", "run_metric.run_id", "latest_runs.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db_label").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      eb.ref("latest_runs.operation").as("operation"),
      isoTimestamp(eb.ref("latest_runs.run_started_at")).as("started_at"),
      isoTimestamp(eb.ref("latest_runs.run_finished_at")).as("finished_at"),
      isoTimestamp(eb.ref("run_metric.time")).as("sample_time"),
      epochSeconds(eb.ref("run_metric.time"), eb.ref("latest_runs.run_started_at")).as("elapsed_s"),
      epochSeconds(eb.ref("latest_runs.run_finished_at"), eb.ref("latest_runs.run_started_at")).as(
        "run_duration_s",
      ),
      eb.ref("run_metric.cpu_percent").as("cpu_percent"),
      eb.ref("run_metric.mem_mb").as("mem_mb"),
      eb.ref("run_metric.disk_mb").as("disk_mb"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .orderBy("latest_runs.operation")
    .orderBy("latest_runs.db_label")
    .orderBy("run_metric.time")
    .execute()
}

export async function fetchInsertSteps(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<InsertStep[]> {
  const db = await getKyselyDb()

  return withLatestRuns(db, { system, suite, suiteScaleFactor, operations: ["populate"] })
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db_label").as("db"),
      eb.ref("latest_runs.db_version").as("db_version"),
      eb.ref("run_step.table_name").$notNull().as("table_name"),
      isoTimestamp(eb.ref("run_step.started_at")).as("started_at"),
      isoTimestamp(eb.ref("run_step.finished_at").$notNull()).as("finished_at"),
      epochSeconds(eb.ref("run_step.finished_at").$notNull(), eb.ref("run_step.started_at")).as(
        "duration_s",
      ),
      epochSeconds(eb.ref("run_step.started_at"), eb.ref("latest_runs.run_started_at")).as(
        "elapsed_start_s",
      ),
      epochSeconds(
        eb.ref("run_step.finished_at").$notNull(),
        eb.ref("latest_runs.run_started_at"),
      ).as("elapsed_end_s"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", "phase")
    .where("run_step.step_name", "=", "insert")
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.table_name", "is not", null)
    .orderBy("latest_runs.db_label")
    .orderBy("run_step.started_at")
    .execute()
}

async function fetchStepsForOperation(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
  operation: BenchmarkOperation,
  stepType: "query" | "mutation",
): Promise<QueryStep[]> {
  const db = await getKyselyDb()

  return withLatestRuns(db, { system, suite, suiteScaleFactor, operations: [operation] })
    .selectFrom("run_step")
    .innerJoin("latest_runs", "latest_runs.run_id", "run_step.run_id")
    .select((eb) => [
      eb.ref("latest_runs.run_id").as("run_id"),
      eb.ref("latest_runs.db_label").as("db"),
      eb.ref("run_step.query_name").$notNull().as("query_name"),
      eb.ref("run_step.iteration").$notNull().as("iteration"),
      isoTimestamp(eb.ref("run_step.started_at")).as("started_at"),
      isoTimestamp(eb.ref("run_step.finished_at").$notNull()).as("finished_at"),
      epochSeconds(eb.ref("run_step.finished_at").$notNull(), eb.ref("run_step.started_at")).as(
        "duration_s",
      ),
      epochSeconds(eb.ref("run_step.started_at"), eb.ref("latest_runs.run_started_at")).as(
        "elapsed_start_s",
      ),
      epochSeconds(
        eb.ref("run_step.finished_at").$notNull(),
        eb.ref("latest_runs.run_started_at"),
      ).as("elapsed_end_s"),
    ])
    .where("latest_runs.run_rank", "=", 1)
    .where("run_step.step_type", "=", stepType)
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .where("run_step.query_name", "is not", null)
    .where("run_step.iteration", "is not", null)
    .orderBy("latest_runs.db_label")
    .orderBy("run_step.started_at")
    .execute()
}

export function fetchQuerySteps(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<QueryStep[]> {
  return fetchStepsForOperation(system, suite, suiteScaleFactor, "select", "query")
}

export function fetchMutateSteps(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
): Promise<QueryStep[]> {
  return fetchStepsForOperation(system, suite, suiteScaleFactor, "mutate", "mutation")
}

export async function fetchFlameSpans(
  system: string,
  suite: BenchmarkSuiteId,
  suiteScaleFactor: number,
  targetDb: string,
): Promise<FlameSpan[]> {
  const db = await getKyselyDb()

  const operationOrder = sql<number>`CASE
    WHEN ${sql.ref("latest_runs.operation")} = 'populate' THEN 0
    WHEN ${sql.ref("latest_runs.operation")} = 'mutate'   THEN 1
    WHEN ${sql.ref("latest_runs.operation")} = 'select'   THEN 2
    ELSE 3
  END`

  // `targetDb` is the database variant label from the UI.
  const withOperationOffsets = withLatestRuns(db, {
    system,
    suite,
    suiteScaleFactor,
    operations: BENCHMARK_OPERATIONS,
    perOperation: true,
  })
    .with("operation_runs", (qb) =>
      qb
        .selectFrom("latest_runs")
        .select((eb) => [
          eb.ref("latest_runs.run_id").as("run_id"),
          eb.ref("latest_runs.db_label").as("db"),
          eb.ref("latest_runs.operation").as("operation"),
          eb.ref("latest_runs.run_started_at").as("run_started_at"),
          eb.ref("latest_runs.run_finished_at").as("run_finished_at"),
          epochSeconds(
            eb.ref("latest_runs.run_finished_at"),
            eb.ref("latest_runs.run_started_at"),
          ).as("duration_s"),
          operationOrder.as("operation_order"),
        ])
        .where("latest_runs.run_rank", "=", 1)
        .where("latest_runs.db_label", "=", targetDb),
    )
    .with("operation_offsets", (qb) =>
      qb.selectFrom("operation_runs").select((eb) => [
        eb.ref("operation_runs.run_id").as("run_id"),
        eb.ref("operation_runs.db").as("db"),
        eb.ref("operation_runs.operation").as("operation"),
        eb.ref("operation_runs.run_started_at").as("run_started_at"),
        eb.ref("operation_runs.run_finished_at").as("run_finished_at"),
        eb.ref("operation_runs.duration_s").as("duration_s"),
        eb.ref("operation_runs.operation_order").as("operation_order"),
        sql<number>`coalesce(
          sum(${eb.ref("operation_runs.duration_s")}) over (
            order by ${eb.ref("operation_runs.operation_order")}
            rows between unbounded preceding and 1 preceding
          ),
          0
        )`.as("operation_offset_s"),
      ]),
    )

  const operationSpans = await withOperationOffsets
    .selectFrom("operation_offsets")
    .select((eb) => [
      sql<string>`'op_' || ${eb.ref("operation_offsets.operation")}`.as("id"),
      eb.ref("operation_offsets.db").as("db"),
      eb.ref("operation_offsets.operation").as("operation"),
      sql<string>`${eb.ref("operation_offsets.operation")}`.as("step_name"),
      sql<string | null>`NULL`.as("query_name"),
      sql<string | null>`NULL`.as("query_sql"),
      sql<number | null>`NULL`.as("iteration"),
      eb.ref("operation_offsets.operation_offset_s").as("elapsed_start_s"),
      sql<number>`${eb.ref("operation_offsets.operation_offset_s")} + ${eb.ref("operation_offsets.duration_s")}`.as(
        "elapsed_end_s",
      ),
      eb.ref("operation_offsets.duration_s").as("duration_s"),
      sql<"operation">`'operation'`.as("depth"),
    ])
    .orderBy("operation_offsets.operation_order")
    .execute()

  const rawStepSpans = await withOperationOffsets
    .selectFrom("run_step")
    .innerJoin("operation_offsets", "operation_offsets.run_id", "run_step.run_id")
    .select((eb) => [
      sql<string>`'step_' || ${eb.ref("run_step.id")}`.as("id"),
      eb.ref("operation_offsets.db").as("db"),
      eb.ref("operation_offsets.operation").as("operation"),
      eb.ref("run_step.step_name").as("step_name"),
      eb.ref("run_step.query_name").as("query_name"),
      sql<string | null>`NULL`.as("query_sql"),
      eb.ref("run_step.iteration").as("iteration"),
      sql<number>`${eb.ref("operation_offsets.operation_offset_s")} + EXTRACT(EPOCH FROM (${eb.ref("run_step.started_at")} - ${eb.ref("operation_offsets.run_started_at")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`${eb.ref("operation_offsets.operation_offset_s")} + EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("operation_offsets.run_started_at")}))`.as(
        "elapsed_end_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("run_step.finished_at")} - ${eb.ref("run_step.started_at")}))`.as(
        "duration_s",
      ),
      sql<"step">`'step'`.as("depth"),
    ])
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .orderBy("operation_offsets.operation_order")
    .orderBy("run_step.started_at")
    .execute()

  const stepSpans = collapseQueryStepIterations(rawStepSpans)

  const querySpans = await withOperationOffsets
    .selectFrom("query_execution")
    .innerJoin("run_step", "run_step.id", "query_execution.run_step_id")
    .innerJoin("operation_offsets", "operation_offsets.run_id", "query_execution.run_id")
    .select((eb) => [
      sql<string>`'qe_' || ${eb.ref("query_execution.id")}`.as("id"),
      eb.ref("operation_offsets.db").as("db"),
      eb.ref("operation_offsets.operation").as("operation"),
      eb.ref("run_step.step_name").as("step_name"),
      eb.ref("run_step.query_name").as("query_name"),
      eb.ref("query_execution.query").as("query_sql"),
      eb.ref("run_step.iteration").as("iteration"),
      sql<number>`${eb.ref("operation_offsets.operation_offset_s")} + EXTRACT(EPOCH FROM (${eb.ref("query_execution.start_time")} - ${eb.ref("operation_offsets.run_started_at")}))`.as(
        "elapsed_start_s",
      ),
      sql<number>`${eb.ref("operation_offsets.operation_offset_s")} + EXTRACT(EPOCH FROM (${eb.ref("query_execution.end_time")} - ${eb.ref("operation_offsets.run_started_at")}))`.as(
        "elapsed_end_s",
      ),
      sql<number>`EXTRACT(EPOCH FROM (${eb.ref("query_execution.end_time")} - ${eb.ref("query_execution.start_time")}))`.as(
        "duration_s",
      ),
      sql<"query">`'query'`.as("depth"),
    ])
    .where("run_step.status", "=", "completed")
    .where("run_step.finished_at", "is not", null)
    .orderBy("operation_offsets.operation_order")
    .orderBy("query_execution.start_time")
    .execute()

  return [...operationSpans, ...stepSpans, ...querySpans]
}

function collapseQueryStepIterations(stepSpans: FlameSpan[]): FlameSpan[] {
  const collapsedSpans: FlameSpan[] = []
  const groupedQueries = new Map<string, FlameSpan>()

  for (const span of stepSpans) {
    if (span.depth !== "step" || span.query_name === null) {
      collapsedSpans.push(span)
      continue
    }

    const groupKey = `${span.operation}:${span.step_name}:${span.query_name}`
    const existing = groupedQueries.get(groupKey)
    if (!existing) {
      groupedQueries.set(groupKey, {
        ...span,
        id: `step_group_${span.operation}_${span.step_name}_${span.query_name}`,
        iteration: null,
      })
      continue
    }

    existing.elapsed_start_s = Math.min(existing.elapsed_start_s, span.elapsed_start_s)
    existing.elapsed_end_s = Math.max(existing.elapsed_end_s, span.elapsed_end_s)
    existing.duration_s = existing.elapsed_end_s - existing.elapsed_start_s
  }

  return [...collapsedSpans, ...groupedQueries.values()].sort((left, right) => {
    const operationDelta = compareOperationOrder(left.operation, right.operation)
    if (operationDelta !== 0) return operationDelta
    return left.elapsed_start_s - right.elapsed_start_s || left.id.localeCompare(right.id)
  })
}

function compareOperationOrder(left: BenchmarkOperation, right: BenchmarkOperation): number {
  const operationOrder: Record<BenchmarkOperation, number> = {
    populate: 0,
    mutate: 1,
    select: 2,
  }

  return operationOrder[left] - operationOrder[right]
}
