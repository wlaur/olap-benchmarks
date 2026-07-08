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
  metadata: RunMetadata | null
}

export interface QuerySummary {
  query_name: string
  db: string
  db_name: string
  db_version: string
  median_duration_s: number
  first_run_duration_s: number | null
  warm_median_duration_s: number | null
  best_warm_duration_s: number | null
  all_iterations_median_duration_s: number
  avg_duration_s: number
  min_duration_s: number
  max_duration_s: number
  iterations: number
  warm_iterations: number
}

export interface CrossSystemQuerySummary extends QuerySummary {
  system: string
}

export type RunStatus = "running" | "completed" | "failed"

export type StepResultStatus =
  | "ok"
  | "timeout"
  | "unsupported"
  | "wrong_result"
  | "error"
  | "skipped"

export type IterationRole = "first_run" | "warm" | "steady_state"

export interface QueryCoverage {
  run_id: number
  db: string
  db_name: string
  db_version: string
  latest_status: Exclude<RunStatus, "running">
  failed_query_count: number
  attempted_query_count: number
  completed_query_count: number
}

export interface CrossSystemQueryCoverage extends QueryCoverage {
  system: string
  started_at: string
  finished_at: string
}

export type BenchmarkOperation = "populate" | "mutate" | "select" | "concurrent"
export type QueryAnalysisOperation = Exclude<BenchmarkOperation, "populate">

export interface RunMetadata {
  host?: {
    os?: string | null
    os_release?: string | null
    machine?: string | null
    processor?: string | null
    cpu_count_logical?: number | null
    memory_total_mb?: number | null
  } | null
  python?: {
    version?: string | null
  } | null
  docker?: {
    version?: string | null
    context?: string | null
    server_platform?: string | null
  } | null
  execution?: {
    mode?: "container" | "in_process" | string | null
    container_image?: string | null
    container_image_digest?: string | null
    container_platform?: string | null
    start_command?: string | null
  } | null
  methodology?: {
    timed_unit?: string | null
    iteration_roles?: string | null
    cache_policy?: string | null
  } | null
}

export interface SuiteScaleFactor {
  suite: string
  suite_scale_factor: number
}

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
