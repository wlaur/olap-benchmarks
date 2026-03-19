export interface Run {
  id: number
  suite: string
  db: string
  db_version: string
  operation: string
  system: string
  started_at: string
  finished_at: string
  duration_s: number
  error_type: string | null
  error_message: string | null
}

export interface RunStep {
  id: number
  run_id: number
  step_type: string
  step_name: string
  query_name: string | null
  iteration: number | null
  table_name: string | null
  started_at: string
  finished_at: string
  row_count: number | null
  error_type: string | null
  error_message: string | null
}

export interface RunMetric {
  id: number
  run_id: number
  time: string
  cpu_percent: number
  mem_mb: number
  disk_mb: number
}

export interface ChartPoint {
  name: string
  duration_s: number
}

export interface Filters {
  system: string | null
  suite: string | null
  db: string | null
  operation: string | null
}

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

export interface StepMetricAvailability {
  db: string
  operation: BenchmarkOperation
  step_name: string
}

export interface QuerySqlEntry {
  sql: string | null
  db_overrides: Record<string, string>
}

export type QueriesManifest = Record<string, Record<string, QuerySqlEntry>>
