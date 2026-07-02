import type { MetricTrendRow } from "../components/MetricTrendChart"
import { scaleDurationForChart, type DurationScaleMode } from "./format"
import type { InsertStep, MetricSample } from "./types"

export interface InsertBarRow {
  table_name: string
  [db: string]: string | number
}

export function buildInsertBarData(
  steps: InsertStep[],
  databases: string[],
  scaleMode: DurationScaleMode,
): InsertBarRow[] {
  const byTable = new Map<string, InsertBarRow>()

  for (const step of steps) {
    const existing = byTable.get(step.table_name) ?? { table_name: step.table_name }
    existing[step.db] = scaleDurationForChart(step.duration_s, scaleMode)
    existing[`${step.db}_raw`] = step.duration_s
    byTable.set(step.table_name, existing)
  }

  return Array.from(byTable.values()).sort((a, b) => {
    const aMax = Math.max(
      ...databases.map((db) => {
        const raw = a[`${db}_raw`]
        return typeof raw === "number" ? raw : 0
      }),
    )
    const bMax = Math.max(
      ...databases.map((db) => {
        const raw = b[`${db}_raw`]
        return typeof raw === "number" ? raw : 0
      }),
    )
    return bMax - aMax
  })
}

export function buildMetricRows(
  samples: MetricSample[],
  metric: "cpu_percent" | "mem_mb",
): MetricTrendRow[] {
  const rowsBySecond = new Map<number, MetricTrendRow>()

  for (const sample of samples) {
    const second = Math.max(0, Math.round(sample.elapsed_s))
    const existing = rowsBySecond.get(second) ?? { elapsed_s: second }
    existing[sample.db] = sample[metric]
    rowsBySecond.set(second, existing)
  }

  return Array.from(rowsBySecond.values()).sort((a, b) => a.elapsed_s - b.elapsed_s)
}
