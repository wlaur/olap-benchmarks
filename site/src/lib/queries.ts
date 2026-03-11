import { query } from "./duckdb"
import type { TimeSeriesQuerySummary, TimeSeriesRunSummary } from "./types"

const ISO_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
const COMPLETED_RUN_CLAUSE = "status = 'completed' AND finished_at IS NOT NULL"
const COMPLETED_RUN_ALIAS_CLAUSE =
  "run.status = 'completed' AND run.finished_at IS NOT NULL"
const COMPLETED_STEP_ALIAS_CLAUSE =
  "run_step.status = 'completed' AND run_step.finished_at IS NOT NULL"

export async function fetchSystems(): Promise<string[]> {
  const rows = await query<{ system: string }>(
    `SELECT DISTINCT system
     FROM results.run
     WHERE ${COMPLETED_RUN_CLAUSE}
     ORDER BY system`,
  )

  return rows.map((row) => row.system)
}

export async function fetchTimeSeriesRunSummaries(
  system: string,
): Promise<TimeSeriesRunSummary[]> {
  return query<TimeSeriesRunSummary>(
    `SELECT run.id AS run_id,
            run.db,
            run.db_version,
            strftime(run.started_at, '${ISO_TIMESTAMP_FORMAT}') AS started_at,
            strftime(run.finished_at, '${ISO_TIMESTAMP_FORMAT}') AS finished_at,
            EXTRACT(EPOCH FROM (run.finished_at - run.started_at)) AS run_duration_s,
            median(
              EXTRACT(EPOCH FROM (run_step.finished_at - run_step.started_at))
            ) FILTER (
              WHERE run_step.step_type = 'query'
                AND ${COMPLETED_STEP_ALIAS_CLAUSE}
            ) AS median_query_duration_s,
            count(run_step.id) FILTER (
              WHERE run_step.step_type = 'query'
                AND ${COMPLETED_STEP_ALIAS_CLAUSE}
            ) AS query_count
     FROM results.run AS run
     LEFT JOIN results.run_step AS run_step
       ON run_step.run_id = run.id
     WHERE run.suite = 'time_series'
       AND run.operation = 'run'
       AND ${COMPLETED_RUN_ALIAS_CLAUSE}
       AND run.system = '${escape(system)}'
     GROUP BY 1, 2, 3, 4, 5, 6
     ORDER BY run_duration_s, run.db`,
  )
}

export async function fetchTimeSeriesQuerySummaries(
  system: string,
): Promise<TimeSeriesQuerySummary[]> {
  return query<TimeSeriesQuerySummary>(
    `SELECT steps.query_name,
            steps.db,
            median(steps.duration_s) AS median_duration_s,
            avg(steps.duration_s) AS avg_duration_s,
            min(steps.duration_s) AS min_duration_s,
            max(steps.duration_s) AS max_duration_s,
            count(*) AS iterations
     FROM (
       SELECT run.db,
              run_step.query_name,
              EXTRACT(EPOCH FROM (run_step.finished_at - run_step.started_at)) AS duration_s
       FROM results.run_step AS run_step
       INNER JOIN results.run AS run
         ON run.id = run_step.run_id
       WHERE run.suite = 'time_series'
         AND run.operation = 'run'
         AND ${COMPLETED_RUN_ALIAS_CLAUSE}
         AND run.system = '${escape(system)}'
         AND run_step.step_type = 'query'
         AND ${COMPLETED_STEP_ALIAS_CLAUSE}
     ) AS steps
     GROUP BY 1, 2
     ORDER BY steps.query_name, steps.db`,
  )
}

function escape(value: string): string {
  return value.replace(/'/g, "''")
}
