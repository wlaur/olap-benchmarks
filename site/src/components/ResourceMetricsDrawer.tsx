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

import type { MetricSample, QueryStep } from "../lib/types"
import { DatabaseLegend } from "./DatabaseLegend"
import { Eyebrow, MetaLabel } from "./Typography"

interface ResourceMetricsDrawerProps {
  isOpen: boolean
  onClose: () => void
  selectedQuery: string | null
  querySteps: QueryStep[]
  metricSamples: MetricSample[]
  databases: string[]
  databaseColors: Record<string, string>
}

type ChartRow = {
  elapsed_s: number
}

type IterationRange = {
  iteration: number
  start: number
  end: number
}

type AlignedMetricSample = MetricSample & {
  iteration: number
  offset_s: number
}

type DatabaseTimeline = {
  db: string
  totalElapsed_s: number
  iterationRanges: IterationRange[]
  samples: AlignedMetricSample[]
}

type MetricSeries = {
  key: string
  points: { elapsed_s: number; value: number }[]
}

const ITERATION_COLORS = ["rgba(108, 142, 239, 0.12)", "rgba(108, 142, 239, 0.06)"] as const

const SAMPLING_THRESHOLD_S = 3
const ITERATION_GAP_S = 0.25

export const RESOURCE_DRAWER_HEIGHT_PX = 460

export function getTraceEligibleDatabases(
  querySteps: QueryStep[],
  selectedQuery: string | null,
  databases: string[],
): string[] {
  if (!selectedQuery) return []

  const maxDurationByDb = new Map<string, number>()
  for (const step of querySteps) {
    if (step.query_name !== selectedQuery || !databases.includes(step.db)) continue
    const current = maxDurationByDb.get(step.db) ?? 0
    maxDurationByDb.set(step.db, Math.max(current, step.duration_s))
  }

  return databases.filter((db) => (maxDurationByDb.get(db) ?? 0) >= SAMPLING_THRESHOLD_S)
}

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

    return queryIterations
      .map((step) => ({
        iteration: step.iteration,
        start: step.elapsed_start_s,
        end: step.elapsed_end_s,
      }))
      .sort((a, b) => a.start - b.start)
  }, [queryIterations])

  const maxQueryDuration = useMemo(() => {
    if (queryIterations.length === 0) return 0
    return Math.max(...queryIterations.map((s) => s.duration_s))
  }, [queryIterations])

  const tooFast = maxQueryDuration < SAMPLING_THRESHOLD_S

  const databaseTimelines = useMemo<DatabaseTimeline[]>(() => {
    if (queryIterations.length === 0 || iterationRanges.length === 0) return []

    const stepsByDb = new Map<string, QueryStep[]>()
    for (const step of queryIterations) {
      const existing = stepsByDb.get(step.db) ?? []
      existing.push(step)
      stepsByDb.set(step.db, existing)
    }

    const samplesByDb = new Map<string, MetricSample[]>()
    for (const sample of runSamples) {
      const existing = samplesByDb.get(sample.db) ?? []
      existing.push(sample)
      samplesByDb.set(sample.db, existing)
    }

    return databases.flatMap((db) => {
      const steps =
        stepsByDb
          .get(db)
          ?.slice()
          .sort((a, b) => a.iteration - b.iteration) ?? []
      if (steps.length === 0) return []

      const dbSamples = samplesByDb.get(db) ?? []
      let cursor = 0
      const alignedRanges: IterationRange[] = []
      const alignedSamples: AlignedMetricSample[] = []

      for (const [index, step] of steps.entries()) {
        const iterationSamples = dbSamples
          .filter(
            (sample) =>
              sample.elapsed_s >= step.elapsed_start_s && sample.elapsed_s <= step.elapsed_end_s,
          )
          .map((sample) => ({
            ...sample,
            iteration: step.iteration,
            offset_s: sample.elapsed_s - step.elapsed_start_s,
          }))
          .sort((a, b) => a.offset_s - b.offset_s)

        const firstOffset = iterationSamples[0]?.offset_s ?? 0
        const lastOffset = iterationSamples.at(-1)?.offset_s ?? Math.max(step.duration_s, 0.001)
        const duration = Math.max(lastOffset - firstOffset, 0.001)
        const start = cursor
        const end = start + duration

        alignedRanges.push({ iteration: step.iteration, start, end })

        for (const sample of iterationSamples) {
          alignedSamples.push({
            ...sample,
            elapsed_s: start + (sample.offset_s - firstOffset),
          })
        }

        cursor = end + (index < steps.length - 1 ? ITERATION_GAP_S : 0)
      }

      return [
        {
          db,
          totalElapsed_s: Math.max(cursor, 1),
          iterationRanges: alignedRanges,
          samples: alignedSamples,
        },
      ]
    })
  }, [runSamples, queryIterations, iterationRanges, databases])

  const queryLabel = selectedQuery
    ? selectedQuery.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
    : ""

  return (
    <div
      className={`fixed inset-x-0 bottom-0 z-40 border-t border-border-default bg-surface-raised shadow-2xl transition-transform duration-300 ease-out ${
        isOpen ? "translate-y-0" : "translate-y-full"
      }`}
      style={{ height: RESOURCE_DRAWER_HEIGHT_PX }}
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
          <div className="mt-3 grid min-h-0 flex-1 gap-3 xl:grid-cols-3">
            <MetricChart
              label="CPU"
              timelines={databaseTimelines}
              metric="cpu_percent"
              databaseColors={databaseColors}
              formatter={(v: number) => `${v.toFixed(0)}%`}
            />
            <MetricChart
              label="Memory"
              timelines={databaseTimelines}
              metric="mem_mb"
              databaseColors={databaseColors}
              formatter={formatMegabytes}
            />
            <MetricChart
              label="Disk"
              timelines={databaseTimelines}
              metric="disk_mb"
              databaseColors={databaseColors}
              formatter={formatMegabytes}
            />
          </div>
        )}
      </div>
    </div>
  )
}

function MetricChart({
  label,
  timelines,
  metric,
  databaseColors,
  formatter,
}: {
  label: string
  timelines: DatabaseTimeline[]
  metric: "cpu_percent" | "mem_mb" | "disk_mb"
  databaseColors: Record<string, string>
  formatter: (v: number) => string
}) {
  const values = timelines.flatMap((timeline) => timeline.samples.map((sample) => sample[metric]))
  const maxVal = Math.max(1, ...values)
  const yScale = toMetricScale(maxVal)

  return (
    <div className="rounded-xl border border-border-default bg-surface-inset p-3">
      <p className="mb-2 text-xs font-medium text-slate-300">{label}</p>
      <div
        className="grid h-full min-h-0 gap-2"
        style={{ gridTemplateRows: `repeat(${Math.max(timelines.length, 1)}, minmax(0, 1fr))` }}
      >
        {timelines.map((timeline) => {
          const data = buildElapsedRows(timeline.samples)
          const series = buildMetricSeries(timeline.samples, metric)
          const maxElapsed = Math.max(1, Math.ceil(timeline.totalElapsed_s))
          const elapsedTicks = buildEvenTicks(0, maxElapsed)

          return (
            <div key={timeline.db} className="grid min-h-0 grid-cols-[84px_minmax(0,1fr)] gap-2">
              <div className="flex min-h-0 flex-col justify-between py-1">
                <p className="truncate text-[11px] font-medium text-slate-300">{timeline.db}</p>
                <p className="text-[10px] text-slate-500">{formatElapsedLabel(maxElapsed)}</p>
              </div>
              <div className="min-h-0">
                <ResponsiveContainer
                  width="100%"
                  height="100%"
                  minHeight={64}
                  initialDimension={{ width: 500, height: 72 }}
                >
                  <LineChart
                    data={data}
                    syncId={`resource-drawer-${timeline.db}`}
                    margin={{ top: 4, right: 12, bottom: 0, left: 0 }}
                  >
                    <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />

                    {timeline.iterationRanges.map((range) => {
                      const widthFraction = (range.end - range.start) / maxElapsed
                      return (
                        <ReferenceArea
                          key={`${timeline.db}-${range.iteration}-${range.start}`}
                          x1={range.start}
                          x2={range.end}
                          fill={ITERATION_COLORS[range.iteration % ITERATION_COLORS.length]}
                          fillOpacity={1}
                          label={
                            widthFraction > 0.08
                              ? {
                                  value: `iter ${range.iteration}`,
                                  position: "insideTopLeft",
                                  fill: "#64748b",
                                  fontSize: 9,
                                  offset: 4,
                                }
                              : undefined
                          }
                        />
                      )
                    })}

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
                      width={56}
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
                      formatter={(value) => [formatter(Number(value ?? 0)), timeline.db] as const}
                    />
                    {series.map((entry) => (
                      <Line
                        key={entry.key}
                        data={entry.points}
                        type="stepAfter"
                        name={timeline.db}
                        dataKey="value"
                        connectNulls={false}
                        dot={false}
                        activeDot={{ r: 3, strokeWidth: 0 }}
                        stroke={databaseColors[timeline.db] ?? "#94a3b8"}
                        strokeWidth={1.5}
                        isAnimationActive={false}
                      />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function buildElapsedRows(samples: AlignedMetricSample[]): ChartRow[] {
  return Array.from(new Set(samples.map((sample) => sample.elapsed_s)))
    .sort((a, b) => a - b)
    .map((elapsed_s) => ({ elapsed_s }))
}

function buildMetricSeries(
  samples: AlignedMetricSample[],
  metric: "cpu_percent" | "mem_mb" | "disk_mb",
): MetricSeries[] {
  const grouped = new Map<string, { iteration: number; points: MetricSeries["points"] }>()

  for (const sample of samples) {
    const key = String(sample.iteration)
    const existing = grouped.get(key) ?? { iteration: sample.iteration, points: [] }
    existing.points.push({ elapsed_s: sample.elapsed_s, value: sample[metric] })
    grouped.set(key, existing)
  }

  return Array.from(grouped.entries())
    .sort(([, left], [, right]) => left.iteration - right.iteration)
    .map(([key, entry]) => ({
      key,
      points: entry.points.sort((a, b) => a.elapsed_s - b.elapsed_s),
    }))
}

const ELAPSED_STEPS = [
  1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 43200, 86400,
]

function buildEvenTicks(minSeconds: number, maxSeconds: number): number[] {
  const safeSpan = Math.max(1, Math.ceil(maxSeconds - minSeconds))
  const target = safeSpan / 5
  const step = ELAPSED_STEPS.find((s) => s >= target) ?? ELAPSED_STEPS[ELAPSED_STEPS.length - 1]!
  const start = Math.ceil(minSeconds / step) * step
  const ticks: number[] = []
  for (let t = start; t <= maxSeconds; t += step) ticks.push(t)
  const firstTick = ticks[0]
  if (firstTick === undefined || firstTick > minSeconds) ticks.unshift(minSeconds)
  const lastTick = ticks.at(-1)
  if (lastTick !== maxSeconds) ticks.push(maxSeconds)
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
  if (h > 0) return s > 0 ? `${h}h ${m}m ${s}s` : `${h}h ${m}m`
  if (m > 0) return s > 0 ? `${m}m ${s}s` : `${m}m`
  return `${s}s`
}

export function shouldShowResourceDrawer(
  querySteps: QueryStep[],
  selectedQuery: string | null,
  databases: string[],
): boolean {
  return getTraceEligibleDatabases(querySteps, selectedQuery, databases).length > 0
}
