export interface ProfileScope {
  active: { id: string; warmupIterations: number | null; measuredIterations: number | null } | null
  excluded: { id: string; runs: number }[]
}

export type BenchmarkOperation = "populate" | "mutate" | "select" | "concurrent"

export interface ExplorerQueryMetric {
  run_id: number
  system: string
  suite_scale_factor: number
  db: string
  db_version: string
  db_driver: string | null
  system_snapshot_id: number | null
  finished_at: string
  query_name: string
  median_duration_s: number
}

export interface QuerySqlEntry {
  sql: string | null
  db_overrides: Record<string, string>
}

export type QueriesManifest = Record<string, Record<string, QuerySqlEntry>>
