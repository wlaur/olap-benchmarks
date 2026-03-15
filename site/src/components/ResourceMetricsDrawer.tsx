import { X } from "lucide-react"
import { useMemo } from "react"
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import type { TimeSeriesMetricSample, TimeSeriesQueryStep } from "../lib/types"
import { DatabaseLegend } from "./DatabaseLegend"
import { Eyebrow, MetaLabel } from "./Typography"

interface ResourceMetricsDrawerProps {
  isOpen: boolean
  onClose: () => void
  selectedQuery: string | null
  querySteps: TimeSeriesQueryStep[]
  metricSamples: TimeSeriesMetricSample[]
  databases: string[]
  databaseColors: Record<string, string>
}

type ChartRow = {
  elapsed_s: number
} & Partial<Record<string, number>>

const ITERATION_COLORS = ["rgba(108, 142, 239, 0.12)", "rgba(108, 142, 239, 0.06)"] as const

const SAMPLING_THRESHOLD_S = 3

export function ResourceMetricsDrawer({
  isOpen,
  onClose,
  selectedQuery,
  querySteps,
  metricSamples,
  databases,
  databaseColors,
}: ResourceMetricsDrawerProps) {
  const runSamples = useMemo(
    () =>
      metricSamples.filter((sample) => sample.operation === "run" && databases.includes(sample.db)),
    [metricSamples, databases],
  )

  const queryIterations = useMemo(() => {
    if (!selectedQuery) return []
    return querySteps
      .filter((s) => s.query_name === selectedQuery && databases.includes(s.db))
      .sort((a, b) => a.iteration - b.iteration || a.elapsed_start_s - b.elapsed_start_s)
  }, [selectedQuery, querySteps, databases])

  const iterationRanges = useMemo(() => {
    if (queryIterations.length === 0) return []

    const byIteration = new Map<number, { minStart: number; maxEnd: number }>()
    for (const step of queryIterations) {
      const existing = byIteration.get(step.iteration)
      if (!existing) {
        byIteration.set(step.iteration, {
          minStart: step.elapsed_start_s,
          maxEnd: step.elapsed_end_s,
        })
      } else {
        existing.minStart = Math.min(existing.minStart, step.elapsed_start_s)
        existing.maxEnd = Math.max(existing.maxEnd, step.elapsed_end_s)
      }
    }

    return Array.from(byIteration.entries())
      .sort(([a], [b]) => a - b)
      .map(([iteration, range]) => ({
        iteration,
        start: range.minStart,
        end: range.maxEnd,
      }))
  }, [queryIterations])

  const maxQueryDuration = useMemo(() => {
    if (queryIterations.length === 0) return 0
    return Math.max(...queryIterations.map((s) => s.duration_s))
  }, [queryIterations])

  const tooFast = maxQueryDuration < SAMPLING_THRESHOLD_S

  const cpuData = useMemo(() => buildMetricRows(runSamples, "cpu_percent"), [runSamples])
  const memData = useMemo(() => buildMetricRows(runSamples, "mem_mb"), [runSamples])

  const maxElapsed = useMemo(
    () => Math.max(1, ...runSamples.map((s) => Math.ceil(s.run_duration_s))),
    [runSamples],
  )
  const elapsedTicks = useMemo(() => buildEvenTicks(maxElapsed), [maxElapsed])

  const queryLabel = selectedQuery
    ? selectedQuery.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
    : ""

  return (
    <div
      className={`fixed inset-x-0 bottom-0 z-40 border-t border-border-default bg-surface-raised shadow-2xl transition-transform duration-300 ease-out ${
        isOpen ? "translate-y-0" : "translate-y-full"
      }`}
      style={{ height: 340 }}
    >
      <div className="flex h-full flex-col px-5 py-4">
        <div className="flex shrink-0 items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <Eyebrow>Resource metrics</Eyebrow>
            {selectedQuery ? (
              <span className="rounded-full bg-accent-400/10 px-3 py-1 text-xs font-medium text-accent-200 shadow-[inset_0_0_0_1px_rgba(108,142,239,0.4)]">
                {queryLabel}
              </span>
            ) : null}
            {tooFast && selectedQuery ? (
              <MetaLabel className="text-amber-400/80">
                Query completes in {"<"}
                {SAMPLING_THRESHOLD_S}s — limited resource data
              </MetaLabel>
            ) : null}
          </div>
          <div className="flex items-center gap-3">
            <DatabaseLegend databases={databases} databaseColors={databaseColors} />
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg p-1.5 text-slate-400 transition-colors hover:bg-surface-inset hover:text-slate-200"
            >
              <X className="size-4" />
            </button>
          </div>
        </div>

        {runSamples.length === 0 ? (
          <div className="flex flex-1 items-center justify-center text-sm text-slate-500">
            No resource metrics available for the run phase.
          </div>
        ) : (
          <div className="mt-3 grid min-h-0 flex-1 gap-3 xl:grid-cols-2">
            <MetricChart
              label="CPU"
              data={cpuData}
              databases={databases}
              databaseColors={databaseColors}
              maxElapsed={maxElapsed}
              elapsedTicks={elapsedTicks}
              formatter={(v: number) => `${v.toFixed(0)}%`}
              iterationRanges={iterationRanges}
            />
            <MetricChart
              label="Memory"
              data={memData}
              databases={databases}
              databaseColors={databaseColors}
              maxElapsed={maxElapsed}
              elapsedTicks={elapsedTicks}
              formatter={formatMegabytes}
              iterationRanges={iterationRanges}
            />
          </div>
        )}
      </div>
    </div>
  )
}

function MetricChart({
  label,
  data,
  databases,
  databaseColors,
  maxElapsed,
  elapsedTicks,
  formatter,
  iterationRanges,
}: {
  label: string
  data: ChartRow[]
  databases: string[]
  databaseColors: Record<string, string>
  maxElapsed: number
  elapsedTicks: number[]
  formatter: (v: number) => string
  iterationRanges: { iteration: number; start: number; end: number }[]
}) {
  const values = data.flatMap((row) => databases.map((db) => (row[db] as number | undefined) ?? 0))
  const maxVal = Math.max(1, ...values)
  const yScale = toMetricScale(maxVal)

  return (
    <div className="rounded-xl border border-border-default bg-surface-inset p-3">
      <p className="mb-2 text-xs font-medium text-slate-300">{label}</p>
      <div className="h-full min-h-0" style={{ height: "calc(100% - 28px)" }}>
        <ResponsiveContainer
          width="100%"
          height="100%"
          initialDimension={{ width: 500, height: 180 }}
        >
          <LineChart
            data={data}
            syncId="resource-drawer"
            margin={{ top: 4, right: 12, bottom: 0, left: 0 }}
          >
            <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />

            {iterationRanges.map((range, i) => (
              <ReferenceArea
                key={range.iteration}
                x1={range.start}
                x2={range.end}
                fill={ITERATION_COLORS[i % ITERATION_COLORS.length]}
                fillOpacity={1}
                label={{
                  value: `iter ${range.iteration}`,
                  position: "insideTopLeft",
                  fill: "#64748b",
                  fontSize: 9,
                  offset: 4,
                }}
              />
            ))}

            <XAxis
              type="number"
              dataKey="elapsed_s"
              domain={[0, maxElapsed]}
              ticks={elapsedTicks}
              tick={{ fill: "#64748b", fontSize: 10 }}
              axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickFormatter={formatElapsedLabel}
            />
            <YAxis
              domain={yScale.domain}
              ticks={yScale.ticks}
              width={60}
              tick={{ fill: "#64748b", fontSize: 10 }}
              axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickFormatter={formatter}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#161a23",
                border: "1px solid rgba(148, 163, 184, 0.12)",
                borderRadius: 12,
                color: "#e2e8f0",
              }}
              labelStyle={{ color: "#e2e8f0" }}
              cursor={{ stroke: "rgba(148, 163, 184, 0.15)", strokeDasharray: "4 4" }}
              labelFormatter={(v) => {
                const numV = typeof v === "number" ? v : Number(v ?? 0)
                return `Elapsed ${formatElapsedLabel(numV)}`
              }}
              formatter={(value, _name, item) =>
                [formatter(Number(value ?? 0)), item.name ?? ""] as const
              }
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
                strokeWidth={1.5}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function buildMetricRows(
  samples: TimeSeriesMetricSample[],
  metric: "cpu_percent" | "mem_mb",
): ChartRow[] {
  const rowsBySecond = new Map<number, ChartRow>()

  for (const sample of samples) {
    const second = Math.max(0, Math.round(sample.elapsed_s))
    const existing = rowsBySecond.get(second) ?? ({ elapsed_s: second } as ChartRow)
    existing[sample.db] = sample[metric]
    rowsBySecond.set(second, existing)
  }

  return Array.from(rowsBySecond.values()).sort((a, b) => a.elapsed_s - b.elapsed_s)
}

function buildEvenTicks(maxSeconds: number): number[] {
  const safeMax = Math.max(1, Math.ceil(maxSeconds))
  const roughStep = safeMax / 4
  const step = getNiceStep(roughStep)
  const ticks: number[] = []
  for (let t = 0; t <= safeMax; t += step) ticks.push(t)
  if (ticks.length < 2 || ticks[ticks.length - 1] !== safeMax) ticks.push(safeMax)
  return ticks
}

function toMetricScale(maxValue: number): { domain: [number, number]; ticks: number[] } {
  const roughStep = maxValue <= 0 ? 1 : maxValue / 4
  const step = getNiceStep(roughStep)
  const roundedMax = maxValue <= 0 ? step : Math.ceil(maxValue / step) * step
  const ticks: number[] = []
  for (let t = 0; t <= roundedMax; t += step) ticks.push(t)
  return { domain: [0, roundedMax], ticks }
}

function getNiceStep(value: number): number {
  const exponent = Math.floor(Math.log10(Math.max(value, 1)))
  const magnitude = 10 ** exponent
  const normalized = value / magnitude
  if (normalized <= 1) return magnitude
  if (normalized <= 2) return 2 * magnitude
  if (normalized <= 5) return 5 * magnitude
  return 10 * magnitude
}

function formatMegabytes(value: number): string {
  if (value >= 1024) {
    const gb = value / 1024
    return `${gb >= 10 ? gb.toFixed(0) : gb.toFixed(1)} GB`
  }
  return `${value.toFixed(0)} MB`
}

function formatElapsedLabel(value: number): string {
  const total = Math.max(0, Math.round(value))
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  const s = total % 60
  if (h > 0) return `${h}h ${m}m`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

export function shouldShowResourceDrawer(
  querySteps: TimeSeriesQueryStep[],
  selectedQuery: string | null,
  databases: string[],
): boolean {
  if (!selectedQuery) return false
  const steps = querySteps.filter((s) => s.query_name === selectedQuery && databases.includes(s.db))
  if (steps.length === 0) return false
  const maxDuration = Math.max(...steps.map((s) => s.duration_s))
  return maxDuration >= SAMPLING_THRESHOLD_S
}
