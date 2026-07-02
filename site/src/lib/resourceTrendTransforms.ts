import type { MetricTrendRow } from "../components/MetricTrendChart"
import { toTitleCase } from "./format"
import {
  formatCpuPercent,
  formatMegabytes,
  toMemoryScale,
  toMetricScale,
  type MetricScaleBuilder,
} from "./metricFormat"
import type { BenchmarkOperation, InsertStep, MetricSample, QueryStep } from "./types"

export interface StepOption {
  label: string
  value: string
}

export interface StepTimeWindow {
  start_s: number
  end_s: number
  duration_s: number
}

interface StepInstance extends StepTimeWindow {
  db: string
  value: string
}

export type MetricKey = "cpu_percent" | "mem_mb" | "disk_mb"

export const METRIC_CONFIGS: ReadonlyArray<{
  key: MetricKey
  label: string
  formatter: (value: number) => string
  scaleBuilder: MetricScaleBuilder
}> = [
  { key: "cpu_percent", label: "CPU", formatter: formatCpuPercent, scaleBuilder: toMetricScale },
  { key: "mem_mb", label: "Memory", formatter: formatMegabytes, scaleBuilder: toMemoryScale },
  { key: "disk_mb", label: "Disk", formatter: formatMegabytes, scaleBuilder: toMemoryScale },
]

export function getStepOptions(
  operation: BenchmarkOperation,
  insertSteps: InsertStep[],
  querySteps: QueryStep[],
  mutateSteps: QueryStep[],
  databases: string[],
  compareQueryNames: (left: string, right: string) => number,
): StepOption[] {
  const includedDatabases = new Set(databases)

  if (operation === "populate") {
    const tableNames = new Set(
      insertSteps.filter((s) => includedDatabases.has(s.db)).map((s) => s.table_name),
    )
    return Array.from(tableNames)
      .sort()
      .map((name) => ({ label: name, value: name }))
  }
  if (operation === "mutate") {
    const queryNames = new Set(
      mutateSteps.filter((s) => includedDatabases.has(s.db)).map((s) => s.query_name),
    )
    return Array.from(queryNames)
      .sort(compareQueryNames)
      .map((name) => ({ label: toTitleCase(name.replace(/_/g, " ")), value: name }))
  }
  const queryNames = new Set(
    querySteps.filter((s) => includedDatabases.has(s.db)).map((s) => s.query_name),
  )
  return Array.from(queryNames)
    .sort(compareQueryNames)
    .map((name) => ({ label: toTitleCase(name.replace(/_/g, " ")), value: name }))
}

function getStepInstances(
  operation: BenchmarkOperation,
  insertSteps: InsertStep[],
  querySteps: QueryStep[],
  mutateSteps: QueryStep[],
  databases: string[],
): StepInstance[] {
  const includedDatabases = new Set(databases)

  if (operation === "populate") {
    return insertSteps
      .filter((step) => includedDatabases.has(step.db))
      .map((step) => ({
        db: step.db,
        value: step.table_name,
        start_s: step.elapsed_start_s,
        end_s: step.elapsed_end_s,
        duration_s: step.duration_s,
      }))
  }

  const steps = operation === "mutate" ? mutateSteps : querySteps
  return steps
    .filter((step) => includedDatabases.has(step.db))
    .map((step) => ({
      db: step.db,
      value: step.query_name,
      start_s: step.elapsed_start_s,
      end_s: step.elapsed_end_s,
      duration_s: step.duration_s,
    }))
}

export function getAvailableStepWindows(
  operation: BenchmarkOperation,
  metricSamples: MetricSample[],
  insertSteps: InsertStep[],
  querySteps: QueryStep[],
  mutateSteps: QueryStep[],
  databases: string[],
): Map<string, Map<string, StepTimeWindow>> {
  const stepInstances = getStepInstances(operation, insertSteps, querySteps, mutateSteps, databases)
  const samplesByDb = new Map<string, number[]>()

  for (const sample of metricSamples) {
    if (sample.operation !== operation || !databases.includes(sample.db)) continue
    const existing = samplesByDb.get(sample.db)
    if (existing) {
      existing.push(sample.elapsed_s)
    } else {
      samplesByDb.set(sample.db, [sample.elapsed_s])
    }
  }

  const available = new Map<string, Map<string, StepTimeWindow & { sampleCount: number }>>()

  for (const instance of stepInstances) {
    const sampleCount = countSamplesInWindow(
      samplesByDb.get(instance.db) ?? [],
      instance.start_s,
      instance.end_s,
    )
    if (sampleCount < 2) continue

    const windowsByDb =
      available.get(instance.value) ?? new Map<string, StepTimeWindow & { sampleCount: number }>()
    const current = windowsByDb.get(instance.db)
    if (
      current === undefined ||
      sampleCount > current.sampleCount ||
      (sampleCount === current.sampleCount && instance.duration_s > current.duration_s)
    ) {
      windowsByDb.set(instance.db, {
        start_s: instance.start_s,
        end_s: instance.end_s,
        duration_s: instance.duration_s,
        sampleCount,
      })
    }
    available.set(instance.value, windowsByDb)
  }

  return new Map(
    Array.from(available.entries())
      .filter(([, windowsByDb]) => windowsByDb.size >= (databases.length > 1 ? 2 : 1))
      .map(([stepValue, windowsByDb]) => [
        stepValue,
        new Map(
          Array.from(windowsByDb.entries()).map(([db, window]) => [
            db,
            {
              start_s: window.start_s,
              end_s: window.end_s,
              duration_s: window.duration_s,
            },
          ]),
        ),
      ]),
  )
}

function countSamplesInWindow(sampleTimes: number[], startS: number, endS: number): number {
  let count = 0
  for (const sampleTime of sampleTimes) {
    if (sampleTime < startS) continue
    if (sampleTime > endS) break
    count += 1
  }
  return count
}

export function buildTrendChartData(
  metricSamples: MetricSample[],
  operation: BenchmarkOperation,
  stepTimeWindows: Map<string, StepTimeWindow>,
): Record<MetricKey, MetricTrendRow[]> {
  const result: Record<MetricKey, Map<number, MetricTrendRow>> = {
    cpu_percent: new Map(),
    mem_mb: new Map(),
    disk_mb: new Map(),
  }

  // Find the first metric sample elapsed_s per database within the window
  // so we can normalize each database's timeline to start at 0.
  const firstSampleElapsed = new Map<string, number>()
  for (const sample of metricSamples) {
    if (sample.operation !== operation) continue
    const window = stepTimeWindows.get(sample.db)
    if (!window) continue
    if (sample.elapsed_s < window.start_s || sample.elapsed_s > window.end_s) continue
    const existing = firstSampleElapsed.get(sample.db)
    if (existing === undefined || sample.elapsed_s < existing) {
      firstSampleElapsed.set(sample.db, sample.elapsed_s)
    }
  }

  for (const sample of metricSamples) {
    if (sample.operation !== operation) continue
    const window = stepTimeWindows.get(sample.db)
    if (!window) continue
    if (sample.elapsed_s < window.start_s || sample.elapsed_s > window.end_s) continue

    const dbOffset = firstSampleElapsed.get(sample.db) ?? window.start_s
    const normalizedElapsed = Math.max(0, Math.round(sample.elapsed_s - dbOffset))

    for (const metricConfig of METRIC_CONFIGS) {
      const map = result[metricConfig.key]
      const existing = map.get(normalizedElapsed) ?? { elapsed_s: normalizedElapsed }
      existing[sample.db] = sample[metricConfig.key]
      map.set(normalizedElapsed, existing)
    }
  }

  return {
    cpu_percent: Array.from(result.cpu_percent.values()).sort((a, b) => a.elapsed_s - b.elapsed_s),
    mem_mb: Array.from(result.mem_mb.values()).sort((a, b) => a.elapsed_s - b.elapsed_s),
    disk_mb: Array.from(result.disk_mb.values()).sort((a, b) => a.elapsed_s - b.elapsed_s),
  }
}
