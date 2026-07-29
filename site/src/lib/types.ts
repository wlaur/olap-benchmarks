export interface QuerySummary {
  query_name: string
  db: string
  db_name: string
  db_version: string
  db_driver: string | null
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

export type RunStatus = "running" | "completed" | "failed"

export interface QueryCoverage {
  run_id: number
  db: string
  db_name: string
  db_version: string
  db_driver: string | null
  latest_status: Exclude<RunStatus, "running">
  failed_query_count: number
  attempted_query_count: number
  completed_query_count: number
}

export type BenchmarkOperation = "populate" | "mutate" | "select" | "concurrent"

export interface SuiteScaleFactor {
  suite: string
  suite_scale_factor: number
}

export interface CatalogRunDimension {
  system: string
  suite: string
  suite_scale_factor: number
  db: string
  db_version: string
  db_driver: string | null
}

export interface ExplorerQueryMetric {
  run_id: number
  system: string
  suite_scale_factor: number
  db: string
  db_version: string
  db_driver: string | null
  finished_at: string
  query_name: string
  median_duration_s: number
}

export interface QuerySqlEntry {
  sql: string | null
  db_overrides: Record<string, string>
}

export type QueriesManifest = Record<string, Record<string, QuerySqlEntry>>
