import { ChevronDown, ChevronUp } from "lucide-react"
import { useMemo, useState } from "react"
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { getDatabaseColors } from "../../lib/databaseColors"
import {
  formatCpuPercent,
  formatMegabytes,
  toMemoryScale,
  toMetricScale,
} from "../../lib/metricFormat"
import { METRIC_SAMPLE_RATE_S, type SuiteConfig } from "../../lib/suiteConfig"
import type {
  BenchmarkOperation,
  InsertStep,
  MetricSample,
  QueryStep,
  StepMetricAvailability,
} from "../../lib/types"
import { DatabaseLegend } from "../DatabaseLegend"
import { PanelCard } from "../layout/Panel"
import { BodyText, SectionTitle } from "../Typography"

interface ResourceTrendPanelProps {
  suiteConfig: SuiteConfig
  metricSamples: MetricSample[]
  insertSteps: InsertStep[]
  querySteps: QueryStep[]
  mutateSteps: QueryStep[]
  databases: string[]
  stepMetricAvailability: StepMetricAvailability[]
}

interface StepOption {
  label: string
  value: string
}

type MetricKey = "cpu_percent" | "mem_mb" | "disk_mb"

interface ChartRow {
  elapsed_s: number
  [db: string]: number | undefined
}

type ScaleBuilder = (maxValue: number) => {
  domain: [number, number]
  ticks: number[]
  formatter?: (v: number) => string
}

const METRIC_CONFIGS: ReadonlyArray<{
  key: MetricKey
  label: string
  formatter: (value: number) => string
  scaleBuilder: ScaleBuilder
}> = [
  { key: "cpu_percent", label: "CPU", formatter: formatCpuPercent, scaleBuilder: toMetricScale },
  { key: "mem_mb", label: "Memory", formatter: formatMegabytes, scaleBuilder: toMemoryScale },
  { key: "disk_mb", label: "Disk", formatter: formatMegabytes, scaleBuilder: toMemoryScale },
]

export function ResourceTrendPanel({
  suiteConfig,
  metricSamples,
  insertSteps,
  querySteps,
  mutateSteps,
  databases,
  stepMetricAvailability,
}: ResourceTrendPanelProps) {
  const [isExpanded, setIsExpanded] = useState(false)
  const [selectedOperation, setSelectedOperation] = useState<BenchmarkOperation>("select")
  const [selectedStep, setSelectedStep] = useState<string | null>(null)

  const databaseColors = getDatabaseColors(databases)

  const availableStepKeys = useMemo(
    () => new Set(stepMetricAvailability.map((s) => `${s.operation}:${s.step_name}`)),
    [stepMetricAvailability],
  )

  const stepOptions = useMemo(
    () =>
      getStepOptions(selectedOperation, insertSteps, querySteps, mutateSteps).filter((step) =>
        availableStepKeys.has(`${selectedOperation}:${step.value}`),
      ),
    [selectedOperation, insertSteps, querySteps, mutateSteps, availableStepKeys],
  )

  const resolvedStep =
    selectedStep && stepOptions.some((s) => s.value === selectedStep)
      ? selectedStep
      : (stepOptions[0]?.value ?? null)

  const stepTimeWindows = useMemo(
    () =>
      resolvedStep
        ? getStepTimeWindows(selectedOperation, resolvedStep, insertSteps, querySteps, mutateSteps)
        : new Map<string, { start_s: number; end_s: number }>(),
    [selectedOperation, resolvedStep, insertSteps, querySteps, mutateSteps],
  )

  const stepDurations = useMemo(() => {
    const durations = new Map<string, number>()
    for (const [db, window] of stepTimeWindows) {
      durations.set(db, window.end_s - window.start_s)
    }
    return durations
  }, [stepTimeWindows])

  const maxDuration = Math.max(0, ...Array.from(stepDurations.values()))
  const isTooFast = maxDuration < METRIC_SAMPLE_RATE_S

  const chartData = useMemo(() => {
    if (isTooFast || !resolvedStep) return { cpu_percent: [], mem_mb: [], disk_mb: [] }
    return buildTrendChartData(metricSamples, selectedOperation, stepTimeWindows)
  }, [metricSamples, selectedOperation, stepTimeWindows, isTooFast, resolvedStep])

  const hasSufficientData =
    !isTooFast &&
    Math.max(chartData.cpu_percent.length, chartData.mem_mb.length, chartData.disk_mb.length) > 1

  const availableOperations = suiteConfig.operations

  const hasData = metricSamples.length > 0

  return (
    <PanelCard>
      <div className="flex items-start justify-between gap-4">
        <div>
          <SectionTitle as="h3">Resource trends</SectionTitle>
          <BodyText className="mt-1 max-w-3xl">
            CPU, memory, and disk usage during a specific step, normalized so each database starts
            at elapsed 0. Select an operation and step to compare resource profiles.
          </BodyText>
        </div>
        <button
          type="button"
          onClick={() => setIsExpanded((current) => !current)}
          disabled={!hasData}
          className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border-default bg-surface-inset px-4 py-2 text-sm font-medium text-slate-300 transition-colors hover:border-slate-700 hover:text-slate-100 disabled:cursor-not-allowed disabled:text-slate-500"
        >
          {isExpanded ? <ChevronUp className="size-4" /> : <ChevronDown className="size-4" />}
          {isExpanded ? "Hide" : "Show"}
        </button>
      </div>

      {!isExpanded ? null : !hasData ? (
        <div className="mt-4 rounded-2xl border border-dashed border-border-default bg-surface-inset px-6 py-8 text-sm text-slate-500">
          No resource metrics were recorded for the selected databases.
        </div>
      ) : (
        <div className="mt-5 rounded-2xl border border-border-default bg-surface-inset p-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="space-y-3">
              <div className="inline-flex rounded-full border border-border-default bg-surface-inset p-1">
                {availableOperations.map((operation) => (
                  <button
                    key={operation}
                    type="button"
                    onClick={() => {
                      setSelectedOperation(operation)
                      setSelectedStep(null)
                    }}
                    className={
                      selectedOperation === operation
                        ? "rounded-full bg-accent-400/10 px-3 py-1 text-xs font-medium text-accent-200 shadow-[inset_0_0_0_1px_rgba(108,142,239,0.4)]"
                        : "rounded-full px-3 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
                    }
                  >
                    {toTitleCase(operation)}
                  </button>
                ))}
              </div>

              {stepOptions.length > 0 ? (
                <div className="flex flex-wrap gap-1.5">
                  {stepOptions.map((step) => (
                    <button
                      key={step.value}
                      type="button"
                      onClick={() => setSelectedStep(step.value)}
                      className={
                        resolvedStep === step.value
                          ? "rounded-full bg-accent-400/10 px-2.5 py-1 text-xs font-medium text-accent-200 shadow-[inset_0_0_0_1px_rgba(108,142,239,0.4)]"
                          : "rounded-full border border-border-default px-2.5 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
                      }
                    >
                      {step.label}
                    </button>
                  ))}
                </div>
              ) : (
                <p className="text-xs text-slate-500">No steps found for this operation.</p>
              )}
            </div>
            <DatabaseLegend databases={databases} databaseColors={databaseColors} />
          </div>

          {resolvedStep === null ? (
            <div className="mt-4 rounded-2xl border border-dashed border-border-default bg-surface-inset px-6 py-8 text-sm text-slate-500">
              No steps available for the selected operation.
            </div>
          ) : isTooFast || !hasSufficientData ? (
            <div className="mt-4 rounded-2xl border border-dashed border-border-default bg-surface-inset px-6 py-8 text-sm text-slate-500">
              Step too fast for resource sampling (under {METRIC_SAMPLE_RATE_S}s). Resource metrics
              are sampled every {METRIC_SAMPLE_RATE_S} seconds, so steps shorter than this threshold
              produce no data points.
            </div>
          ) : (
            <div className="mt-4 grid gap-3">
              {METRIC_CONFIGS.map((metric) => (
                <TrendChart
                  key={metric.key}
                  label={metric.label}
                  data={chartData[metric.key]}
                  databases={databases}
                  databaseColors={databaseColors}
                  formatter={metric.formatter}
                  scaleBuilder={metric.scaleBuilder}
                  maxElapsed={maxDuration}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </PanelCard>
  )
}

interface TrendChartProps {
  label: string
  data: ChartRow[]
  databases: string[]
  databaseColors: Record<string, string>
  formatter: (value: number) => string
  scaleBuilder: ScaleBuilder
  maxElapsed: number
}

function TrendChart({
  label,
  data,
  databases,
  databaseColors,
  formatter,
  scaleBuilder,
  maxElapsed,
}: TrendChartProps) {
  const yValues = data.flatMap((row) =>
    databases.map((db) => row[db]).filter((v): v is number => v !== undefined),
  )
  const maxY = Math.max(1, ...yValues)
  const yScale = scaleBuilder(maxY)
  const { domain: yDomain, ticks: yTicks } = yScale
  const tickFormatter = yScale.formatter ?? formatter

  const xTicks = buildEvenElapsedTicks(maxElapsed)

  return (
    <div className="rounded-2xl border border-border-default bg-surface-primary/60 p-3">
      <p className="mb-3 text-sm font-medium text-slate-200">{label}</p>
      <div className="h-40">
        <ResponsiveContainer
          width="100%"
          height="100%"
          initialDimension={{ width: 640, height: 160 }}
        >
          <LineChart
            data={data}
            syncId="resource-trends"
            margin={{ top: 8, right: 12, bottom: 0, left: 0 }}
          >
            <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />
            <XAxis
              type="number"
              dataKey="elapsed_s"
              domain={[0, maxElapsed]}
              ticks={xTicks}
              tick={{ fill: "#64748b", fontSize: 11 }}
              axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickFormatter={formatElapsedLabel}
            />
            <YAxis
              domain={yDomain}
              ticks={yTicks}
              width={70}
              tick={{ fill: "#64748b", fontSize: 11 }}
              axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickFormatter={tickFormatter}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#161a23",
                border: "1px solid rgba(148, 163, 184, 0.12)",
                borderRadius: 12,
                color: "#e2e8f0",
              }}
              labelStyle={{ color: "#e2e8f0" }}
              itemStyle={{ color: "#e2e8f0" }}
              cursor={{ stroke: "rgba(148, 163, 184, 0.15)", strokeDasharray: "4 4" }}
              labelFormatter={(value) => {
                const numericValue = typeof value === "number" ? value : Number(value ?? 0)
                return `Elapsed ${formatElapsedLabel(numericValue)}`
              }}
              formatter={(value, _name, item) => {
                const numericValue = typeof value === "number" ? value : Number(value ?? 0)
                return [formatter(numericValue), item.name ?? ""] as const
              }}
            />
            {databases.map((db) => (
              <Line
                key={db}
                type="stepAfter"
                name={db}
                dataKey={(row: ChartRow) => row[db]}
                connectNulls
                dot={false}
                activeDot={{ r: 3, strokeWidth: 0 }}
                stroke={databaseColors[db] ?? "#94a3b8"}
                strokeWidth={2}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function getStepOptions(
  operation: BenchmarkOperation,
  insertSteps: InsertStep[],
  querySteps: QueryStep[],
  mutateSteps: QueryStep[],
): StepOption[] {
  if (operation === "populate") {
    const tableNames = new Set(insertSteps.map((s) => s.table_name))
    return Array.from(tableNames)
      .sort()
      .map((name) => ({ label: name, value: name }))
  }
  if (operation === "mutate") {
    const queryNames = new Set(mutateSteps.map((s) => s.query_name))
    return Array.from(queryNames)
      .sort()
      .map((name) => ({ label: toTitleCase(name.replace(/_/g, " ")), value: name }))
  }
  const queryNames = new Set(querySteps.map((s) => s.query_name))
  return Array.from(queryNames)
    .sort()
    .map((name) => ({ label: toTitleCase(name.replace(/_/g, " ")), value: name }))
}

function getStepTimeWindows(
  operation: BenchmarkOperation,
  stepValue: string,
  insertSteps: InsertStep[],
  querySteps: QueryStep[],
  mutateSteps: QueryStep[],
): Map<string, { start_s: number; end_s: number }> {
  const windows = new Map<string, { start_s: number; end_s: number }>()

  if (operation === "populate") {
    for (const step of insertSteps) {
      if (step.table_name !== stepValue) continue
      const existing = windows.get(step.db)
      if (!existing) {
        windows.set(step.db, { start_s: step.elapsed_start_s, end_s: step.elapsed_end_s })
      } else {
        existing.start_s = Math.min(existing.start_s, step.elapsed_start_s)
        existing.end_s = Math.max(existing.end_s, step.elapsed_end_s)
      }
    }
    return windows
  }

  const steps = operation === "mutate" ? mutateSteps : querySteps
  for (const step of steps) {
    if (step.query_name !== stepValue) continue
    const existing = windows.get(step.db)
    if (!existing) {
      windows.set(step.db, { start_s: step.elapsed_start_s, end_s: step.elapsed_end_s })
    } else {
      existing.start_s = Math.min(existing.start_s, step.elapsed_start_s)
      existing.end_s = Math.max(existing.end_s, step.elapsed_end_s)
    }
  }
  return windows
}

function buildTrendChartData(
  metricSamples: MetricSample[],
  operation: BenchmarkOperation,
  stepTimeWindows: Map<string, { start_s: number; end_s: number }>,
): Record<MetricKey, ChartRow[]> {
  const result: Record<MetricKey, Map<number, ChartRow>> = {
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

function formatElapsedLabel(value: number): string {
  const total = Math.max(0, Math.round(value))
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  const s = total % 60
  if (h > 0) return s > 0 ? `${h}h ${m}m ${s}s` : `${h}h ${m}m`
  if (m > 0) return s > 0 ? `${m}m ${s}s` : `${m}m`
  return `${s}s`
}

const ELAPSED_STEPS = [
  1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 43200, 86400,
]

function buildEvenElapsedTicks(maxSeconds: number): number[] {
  const safeMax = Math.max(1, Math.ceil(maxSeconds))
  const target = safeMax / 5
  const step = ELAPSED_STEPS.find((s) => s >= target) ?? ELAPSED_STEPS[ELAPSED_STEPS.length - 1]!
  const ticks: number[] = []
  for (let t = 0; t <= safeMax; t += step) ticks.push(t)
  return ticks
}
function toTitleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toUpperCase())
}
