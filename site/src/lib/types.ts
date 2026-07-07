export interface RunSummary {
  run_id: number
  db: string
  db_version: string
  started_at: string
  finished_at: string
  run_duration_s: number
  median_query_duration_s: number | null
  query_count: number
}

export interface OperationSummary {
  run_id: number
  db: string
  db_version: string
  operation: BenchmarkOperation
  started_at: string
  finished_at: string
  run_duration_s: number
}

export interface QuerySummary {
  query_name: string
  db: string
  median_duration_s: number
  avg_duration_s: number
  min_duration_s: number
  max_duration_s: number
  iterations: number
}

export type RunStatus = "running" | "completed" | "failed"

export interface QueryCoverage {
  run_id: number
  db: string
  db_version: string
  latest_status: Exclude<RunStatus, "running">
  failed_query_count: number
  attempted_query_count: number
  completed_query_count: number
}

export type BenchmarkOperation = "populate" | "mutate" | "select"

export interface MetricSample {
  run_id: number
  db: string
  db_version: string
  operation: BenchmarkOperation
  started_at: string
  finished_at: string
  sample_time: string
  elapsed_s: number
  run_duration_s: number
  cpu_percent: number
  mem_mb: number
  disk_mb: number
}

export interface InsertStep {
  run_id: number
  db: string
  db_version: string
  table_name: string
  started_at: string
  finished_at: string
  duration_s: number
  elapsed_start_s: number
  elapsed_end_s: number
}

export interface QueryStep {
  run_id: number
  db: string
  query_name: string
  iteration: number
  started_at: string
  finished_at: string
  duration_s: number
  elapsed_start_s: number
  elapsed_end_s: number
}

export interface QuerySqlEntry {
  sql: string | null
  db_overrides: Record<string, string>
}

export type QueriesManifest = Record<string, Record<string, QuerySqlEntry>>

export interface FlameSpan {
  id: string
  db: string
  operation: BenchmarkOperation
  step_name: string
  query_name: string | null
  query_sql: string | null
  iteration: number | null
  elapsed_start_s: number
  elapsed_end_s: number
  duration_s: number
  depth: "operation" | "step" | "query"
}
