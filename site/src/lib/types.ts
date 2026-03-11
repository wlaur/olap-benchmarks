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
