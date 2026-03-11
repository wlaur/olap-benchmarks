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

import type { TimeSeriesMetricSample, TimeSeriesOperation } from "../lib/types"
import { DatabaseLegend } from "./DatabaseLegend"

const OPERATIONS: TimeSeriesOperation[] = ["populate", "run"]

const METRIC_CONFIGS = [
  {
    key: "cpu_percent",
    label: "CPU",
    formatter: (value: number) => `${value.toFixed(0)}%`,
  },
  {
    key: "mem_mb",
    label: "Memory",
    formatter: formatMegabytes,
  },
  {
    key: "disk_mb",
    label: "Disk",
    formatter: formatMegabytes,
  },
] as const

type MetricKey = (typeof METRIC_CONFIGS)[number]["key"]

type ChartRow = {
  elapsed_s: number
} & Partial<Record<string, number>>

interface MetricsTimeSeriesPanelProps {
  samples: TimeSeriesMetricSample[]
  databases: string[]
  databaseColors: Record<string, string>
}

export function MetricsTimeSeriesPanel({
  samples,
  databases,
  databaseColors,
}: MetricsTimeSeriesPanelProps) {
  const [isExpanded, setIsExpanded] = useState(true)
  const includedDatabaseSet = useMemo(() => new Set(databases), [databases])

  const filteredSamples = useMemo(
    () => samples.filter((sample) => includedDatabaseSet.has(sample.db)),
    [includedDatabaseSet, samples],
  )

  const chartData = useMemo(() => buildMetricChartData(filteredSamples), [filteredSamples])
  const xDomains = useMemo(() => buildXDomains(filteredSamples), [filteredSamples])
  const xTicks = useMemo(() => buildXTicks(xDomains), [xDomains])
  const yScales = useMemo(() => buildYScales(filteredSamples), [filteredSamples])

  return (
    <section className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-lg font-semibold text-slate-50">Resource metrics</h3>
          <p className="mt-1 max-w-3xl text-sm text-slate-400">
            Latest completed traces for each database, aligned on elapsed time from operation start.
            Populate and run stay separate, while CPU, memory, and disk share the same database
            overlays.
          </p>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-3">
          {isExpanded ? (
            <DatabaseLegend databases={databases} databaseColors={databaseColors} />
          ) : null}
          <button
            onClick={() => setIsExpanded((current) => !current)}
            className="inline-flex items-center gap-1.5 rounded-full border border-slate-800 bg-slate-950/80 px-3 py-1 text-xs font-medium text-slate-300 transition-colors hover:border-slate-700 hover:text-slate-100"
          >
            {isExpanded ? <ChevronUp className="size-3.5" /> : <ChevronDown className="size-3.5" />}
            {isExpanded ? "Hide" : "Show"}
          </button>
        </div>
      </div>

      {!isExpanded ? null : filteredSamples.length === 0 ? (
        <div className="mt-4 rounded-2xl border border-dashed border-slate-800 bg-slate-950/40 px-6 py-8 text-sm text-slate-500">
          No resource metrics were recorded for the selected databases.
        </div>
      ) : (
        <div className="mt-5 grid gap-4 xl:grid-cols-2">
          {OPERATIONS.map((operation) => {
            const sampleCount = filteredSamples.filter(
              (sample) => sample.operation === operation,
            ).length
            const databaseCount = new Set(
              filteredSamples
                .filter((sample) => sample.operation === operation)
                .map((sample) => sample.db),
            ).size
            const durationLabel =
              sampleCount === 0
                ? "No completed runs"
                : `${databaseCount} database${databaseCount === 1 ? "" : "s"}`

            return (
              <div
                key={operation}
                className="rounded-2xl border border-slate-800 bg-slate-950/45 p-4"
              >
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-xs font-medium tracking-[0.18em] text-cyan-300 uppercase">
                      {operation}
                    </p>
                    <h4 className="mt-2 text-xl font-semibold text-slate-50">
                      {toTitleCase(operation)} trace
                    </h4>
                    <p className="mt-1 text-sm text-slate-400">
                      {durationLabel} · X-axis is elapsed time from start
                    </p>
                  </div>
                  <div className="rounded-full border border-slate-800 bg-slate-950/80 px-3 py-1 text-xs text-slate-400">
                    Max {formatElapsedLabel(xDomains[operation])}
                  </div>
                </div>

                <div className="mt-4 grid gap-3">
                  {METRIC_CONFIGS.map((metric) => (
                    <div
                      key={`${operation}-${metric.key}`}
                      className="rounded-2xl border border-slate-800 bg-slate-950/70 p-3"
                    >
                      <div className="mb-3 flex items-center justify-between gap-3">
                        <p className="text-sm font-medium text-slate-200">{metric.label}</p>
                      </div>

                      <div className="h-40">
                        <ResponsiveContainer width="100%" height="100%">
                          <LineChart
                            data={chartData[operation][metric.key]}
                            syncId={`time-series-metrics-${operation}`}
                            margin={{ top: 8, right: 12, bottom: 0, left: 0 }}
                          >
                            <CartesianGrid stroke="#1e293b" vertical={false} />
                            <XAxis
                              type="number"
                              dataKey="elapsed_s"
                              domain={[0, xDomains[operation]]}
                              ticks={xTicks[operation]}
                              tick={{ fill: "#94a3b8", fontSize: 11 }}
                              axisLine={{ stroke: "#334155" }}
                              tickLine={{ stroke: "#334155" }}
                              tickFormatter={formatElapsedLabel}
                            />
                            <YAxis
                              domain={yScales[metric.key].domain}
                              ticks={yScales[metric.key].ticks}
                              width={70}
                              tick={{ fill: "#94a3b8", fontSize: 11 }}
                              axisLine={{ stroke: "#334155" }}
                              tickLine={{ stroke: "#334155" }}
                              tickFormatter={metric.formatter}
                            />
                            <Tooltip
                              contentStyle={{
                                backgroundColor: "#020617",
                                border: "1px solid #334155",
                                borderRadius: 14,
                              }}
                              cursor={{ stroke: "#475569", strokeDasharray: "4 4" }}
                              labelFormatter={(value) =>
                                `Elapsed ${formatElapsedLabel(Number(value))}`
                              }
                              formatter={(value: number, _name, item) => [
                                metric.formatter(value),
                                item.name,
                              ]}
                            />
                            {databases.map((db) => (
                              <Line
                                key={`${operation}-${metric.key}-${db}`}
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
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </section>
  )
}

function buildMetricChartData(
  samples: TimeSeriesMetricSample[],
): Record<TimeSeriesOperation, Record<MetricKey, ChartRow[]>> {
  return {
    populate: {
      cpu_percent: buildRowsForMetric(samples, "populate", "cpu_percent"),
      mem_mb: buildRowsForMetric(samples, "populate", "mem_mb"),
      disk_mb: buildRowsForMetric(samples, "populate", "disk_mb"),
    },
    run: {
      cpu_percent: buildRowsForMetric(samples, "run", "cpu_percent"),
      mem_mb: buildRowsForMetric(samples, "run", "mem_mb"),
      disk_mb: buildRowsForMetric(samples, "run", "disk_mb"),
    },
  }
}

function buildRowsForMetric(
  samples: TimeSeriesMetricSample[],
  operation: TimeSeriesOperation,
  metric: MetricKey,
): ChartRow[] {
  const rowsBySecond = new Map<number, ChartRow>()

  for (const sample of samples) {
    if (sample.operation !== operation) continue

    const second = Math.max(0, Math.round(sample.elapsed_s))
    const existing = rowsBySecond.get(second) ?? ({ elapsed_s: second } as ChartRow)
    existing[sample.db] = sample[metric]
    rowsBySecond.set(second, existing)
  }

  return Array.from(rowsBySecond.values()).sort((left, right) => left.elapsed_s - right.elapsed_s)
}

function buildXDomains(samples: TimeSeriesMetricSample[]): Record<TimeSeriesOperation, number> {
  return {
    populate: Math.max(
      1,
      ...samples
        .filter((sample) => sample.operation === "populate")
        .map((sample) => Math.ceil(sample.run_duration_s)),
    ),
    run: Math.max(
      1,
      ...samples
        .filter((sample) => sample.operation === "run")
        .map((sample) => Math.ceil(sample.run_duration_s)),
    ),
  }
}

function buildXTicks(
  domains: Record<TimeSeriesOperation, number>,
): Record<TimeSeriesOperation, number[]> {
  return {
    populate: buildEvenElapsedTicks(domains.populate),
    run: buildEvenElapsedTicks(domains.run),
  }
}

function buildYScales(
  samples: TimeSeriesMetricSample[],
): Record<MetricKey, { domain: [number, number]; ticks: number[] }> {
  return {
    cpu_percent: toMetricScale(samples.map((sample) => sample.cpu_percent)),
    mem_mb: toMetricScale(samples.map((sample) => sample.mem_mb)),
    disk_mb: toMetricScale(samples.map((sample) => sample.disk_mb)),
  }
}

function toMetricScale(values: number[]): { domain: [number, number]; ticks: number[] } {
  const maxValue = Math.max(0, ...values)
  const roughStep = maxValue <= 0 ? 1 : maxValue / 4
  const step = getNiceStep(roughStep)
  const roundedMax = maxValue <= 0 ? step : Math.ceil(maxValue / step) * step
  const ticks: number[] = []

  for (let tick = 0; tick <= roundedMax; tick += step) {
    ticks.push(tick)
  }

  return {
    domain: [0, roundedMax],
    ticks,
  }
}

function formatMegabytes(value: number): string {
  if (value >= 1024) {
    const gbValue = value / 1024
    return `${gbValue >= 10 ? gbValue.toFixed(0) : gbValue.toFixed(1)} GB`
  }

  return `${value.toFixed(0)} MB`
}

function formatElapsedLabel(value: number): string {
  const totalSeconds = Math.max(0, Math.round(value))
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

function buildEvenElapsedTicks(maxSeconds: number): number[] {
  const safeMax = Math.max(1, Math.ceil(maxSeconds))
  const roughStep = safeMax / 4
  const step = getNiceStep(roughStep)
  const ticks: number[] = []

  for (let tick = 0; tick <= safeMax; tick += step) {
    ticks.push(tick)
  }

  if (ticks.length < 2 || ticks[ticks.length - 1] !== safeMax) {
    ticks.push(safeMax)
  }

  return ticks
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

function toTitleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toUpperCase())
}
