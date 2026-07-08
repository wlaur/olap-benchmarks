import type { QueryComparisonRow } from "../components/QueryComparisonTable"
import { scaleDurationForChart, type DurationScaleMode } from "./format"
import type { SuiteConfig } from "./suiteConfig"
import type { BenchmarkOperation, OperationSummary, QuerySummary } from "./types"

export interface OverviewChartRow {
  db: string
  db_version: string
  fill?: string
  populate_duration_s: number
  select_duration_s: number
  mutate_duration_s: number
  concurrent_duration_s: number
  total_duration_s: number
  populate_chart_duration_s: number
  select_chart_duration_s: number
  mutate_chart_duration_s: number
  concurrent_chart_duration_s: number
}

export type OverviewOperationVisibility = Record<BenchmarkOperation, boolean>

export function buildOverviewChartData(
  operationSummaries: OperationSummary[],
  databases: string[],
  scaleMode: DurationScaleMode,
  visibleOperations: OverviewOperationVisibility,
): OverviewChartRow[] {
  const summariesByDatabase = new Map<
    string,
    {
      db: string
      db_version: string
      populate_duration_s: number
      select_duration_s: number
      mutate_duration_s: number
      concurrent_duration_s: number
    }
  >()

  for (const summary of operationSummaries) {
    const existing = summariesByDatabase.get(summary.db) ?? {
      db: summary.db,
      db_version: summary.db_version,
      populate_duration_s: 0,
      select_duration_s: 0,
      mutate_duration_s: 0,
      concurrent_duration_s: 0,
    }

    existing.db_version = summary.db_version
    if (summary.operation === "populate") {
      existing.populate_duration_s = summary.run_duration_s
    } else if (summary.operation === "select") {
      existing.select_duration_s = summary.run_duration_s
    } else if (summary.operation === "mutate") {
      existing.mutate_duration_s = summary.run_duration_s
    } else if (summary.operation === "concurrent") {
      existing.concurrent_duration_s = summary.run_duration_s
    }

    summariesByDatabase.set(summary.db, existing)
  }

  return databases.map((database) => {
    const entry = summariesByDatabase.get(database)
    if (!entry) {
      return {
        db: database,
        db_version: "",
        populate_duration_s: 0,
        select_duration_s: 0,
        mutate_duration_s: 0,
        concurrent_duration_s: 0,
        total_duration_s: 0,
        populate_chart_duration_s: 0,
        mutate_chart_duration_s: 0,
        select_chart_duration_s: 0,
        concurrent_chart_duration_s: 0,
      }
    }

    const visiblePopulate = visibleOperations.populate ? entry.populate_duration_s : 0
    const visibleSelect = visibleOperations.select ? entry.select_duration_s : 0
    const visibleMutate = visibleOperations.mutate ? entry.mutate_duration_s : 0
    const visibleConcurrent = visibleOperations.concurrent ? entry.concurrent_duration_s : 0
    const totalDuration = visiblePopulate + visibleSelect + visibleMutate + visibleConcurrent

    return {
      db: entry.db,
      db_version: entry.db_version,
      populate_duration_s: entry.populate_duration_s,
      select_duration_s: entry.select_duration_s,
      mutate_duration_s: entry.mutate_duration_s,
      concurrent_duration_s: entry.concurrent_duration_s,
      total_duration_s: totalDuration,
      populate_chart_duration_s: scaleDurationForChart(visiblePopulate, scaleMode),
      mutate_chart_duration_s: scaleDurationForChart(visibleMutate, scaleMode),
      select_chart_duration_s: scaleDurationForChart(visibleSelect, scaleMode),
      concurrent_chart_duration_s: scaleDurationForChart(visibleConcurrent, scaleMode),
    }
  })
}

export function buildQueryComparisonRows(
  querySummaries: QuerySummary[],
  databases: string[],
  suiteConfig: SuiteConfig,
): QueryComparisonRow[] {
  const groupedQueries = new Map<string, QuerySummary[]>()

  for (const querySummary of querySummaries) {
    const existingRows = groupedQueries.get(querySummary.query_name) ?? []
    existingRows.push(querySummary)
    groupedQueries.set(querySummary.query_name, existingRows)
  }

  return Array.from(groupedQueries.entries())
    .sort(([leftName], [rightName]) => suiteConfig.compareQueryNames(leftName, rightName))
    .map(([queryName, rows]) => {
      const { queryId, queryLabel, tableFamily } = suiteConfig.parseQueryName(queryName)
      const byDatabase = Object.fromEntries(
        databases.map((database) => [database, null]),
      ) as Record<string, number | null>
      const statsByDatabase = Object.fromEntries(
        databases.map((database) => [database, null]),
      ) as QueryComparisonRow["stats_by_database"]

      for (const row of rows) {
        byDatabase[row.db] = row.median_duration_s
        statsByDatabase[row.db] = {
          median_duration_s: row.median_duration_s,
          first_run_duration_s: row.first_run_duration_s,
          warm_median_duration_s: row.warm_median_duration_s,
          best_warm_duration_s: row.best_warm_duration_s,
          all_iterations_median_duration_s: row.all_iterations_median_duration_s,
          avg_duration_s: row.avg_duration_s,
          min_duration_s: row.min_duration_s,
          max_duration_s: row.max_duration_s,
          iterations: row.iterations,
          warm_iterations: row.warm_iterations,
        }
      }

      const numericDurations = Object.values(byDatabase).filter((value) => value !== null)
      const fastestDuration = Math.min(...numericDurations)
      const slowestDuration = Math.max(...numericDurations)
      const fastestDb = rows.find((row) => row.median_duration_s === fastestDuration)?.db ?? "—"

      return {
        query_name: queryName,
        query_label: queryLabel,
        table_family: tableFamily,
        query_id: queryId,
        fastest_db: fastestDb,
        spread_ratio: slowestDuration / fastestDuration,
        by_database: byDatabase,
        stats_by_database: statsByDatabase,
      }
    })
}

export function withAlpha(hexColor: string, alpha: number): string {
  const normalized = hexColor.replace("#", "")
  if (normalized.length !== 6) return hexColor

  const red = Number.parseInt(normalized.slice(0, 2), 16)
  const green = Number.parseInt(normalized.slice(2, 4), 16)
  const blue = Number.parseInt(normalized.slice(4, 6), 16)

  return `rgba(${red}, ${green}, ${blue}, ${alpha})`
}

export const LOG_FLOOR = 1e-6

export function computeMaxDuration(rows: QueryComparisonRow[]): number {
  return rows.reduce((max, row) => {
    for (const val of Object.values(row.by_database)) {
      if (val !== null && val > max) {
        max = val
      }
    }
    return max
  }, LOG_FLOOR)
}
