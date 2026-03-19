import { useMemo, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  CartesianGrid as CpuGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import {
  formatDurationAxisTick,
  formatDurationSeconds,
  getDurationAxisDomain,
  getDurationAxisTicks,
  scaleDurationForChart,
  type DurationScaleMode,
} from "../lib/format"
import {
  formatCpuPercent,
  formatMegabytes,
  toMemoryScale,
  toMetricScale,
} from "../lib/metricFormat"
import type { InsertStep, MetricSample } from "../lib/types"
import { DatabaseLegend } from "./DatabaseLegend"
import { DurationScaleToggle } from "./DurationScaleToggle"
import { ChartFrame, PanelCard, PanelHeader } from "./layout/Panel"
import { BodyText, SectionTitle } from "./Typography"

interface InsertPerformancePanelProps {
  insertSteps: InsertStep[]
  metricSamples: MetricSample[]
  databases: string[]
  databaseColors: Record<string, string>
}

interface InsertBarRow {
  table_name: string
  [db: string]: string | number
}

type MetricChartRow = {
  elapsed_s: number
} & Partial<Record<string, number>>

export function InsertPerformancePanel({
  insertSteps,
  metricSamples,
  databases,
  databaseColors,
}: InsertPerformancePanelProps) {
  const [insertScaleMode, setInsertScaleMode] = useState<DurationScaleMode>("linear")

  const filteredSteps = useMemo(
    () => insertSteps.filter((step) => databases.includes(step.db)),
    [insertSteps, databases],
  )

  const barData = useMemo(
    () => buildInsertBarData(filteredSteps, databases, insertScaleMode),
    [filteredSteps, databases, insertScaleMode],
  )

  const populateSamples = useMemo(
    () =>
      metricSamples.filter(
        (sample) => sample.operation === "populate" && databases.includes(sample.db),
      ),
    [metricSamples, databases],
  )

  const cpuChartData = useMemo(
    () => buildMetricRows(populateSamples, "cpu_percent"),
    [populateSamples],
  )
  const memChartData = useMemo(() => buildMetricRows(populateSamples, "mem_mb"), [populateSamples])

  const maxElapsed = useMemo(
    () => Math.max(1, ...populateSamples.map((s) => Math.ceil(s.run_duration_s))),
    [populateSamples],
  )
  const elapsedTicks = useMemo(() => buildEvenTicks(maxElapsed), [maxElapsed])

  const maxDuration = useMemo(
    () => Math.max(0.001, ...filteredSteps.map((s) => s.duration_s)),
    [filteredSteps],
  )
  const insertAxisDomain = useMemo(
    () => getDurationAxisDomain(maxDuration, insertScaleMode),
    [maxDuration, insertScaleMode],
  )
  const insertAxisTicks = useMemo(
    () => getDurationAxisTicks(maxDuration, insertScaleMode),
    [maxDuration, insertScaleMode],
  )

  if (filteredSteps.length === 0) return null

  return (
    <PanelCard>
      <PanelHeader>
        <div>
          <SectionTitle as="h3">Insert performance</SectionTitle>
          <BodyText className="mt-1">
            Per-table insert durations from the latest populate run, with resource utilization
            during the populate phase.
          </BodyText>
        </div>
        <DatabaseLegend databases={databases} databaseColors={databaseColors} />
      </PanelHeader>

      <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <ChartFrame>
          <div className="mb-3 flex items-center justify-between">
            <p className="text-sm font-medium text-slate-200">Insert duration by table</p>
            <DurationScaleToggle mode={insertScaleMode} onChange={setInsertScaleMode} compact />
          </div>
          <div style={{ height: Math.max(160, barData.length * 48 + 40) }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={barData}
                layout="vertical"
                margin={{ top: 4, right: 16, bottom: 4, left: 0 }}
              >
                <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" horizontal={false} />
                <XAxis
                  type="number"
                  domain={insertAxisDomain}
                  ticks={insertAxisTicks}
                  tick={{ fill: "#64748b", fontSize: 11 }}
                  axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickFormatter={(v: number) => formatDurationAxisTick(v, insertScaleMode)}
                />
                <YAxis
                  type="category"
                  dataKey="table_name"
                  width={110}
                  tick={{ fill: "#94a3b8", fontSize: 11 }}
                  axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickLine={false}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#161a23",
                    border: "1px solid rgba(148, 163, 184, 0.12)",
                    borderRadius: 12,
                    color: "#e2e8f0",
                  }}
                  labelStyle={{ color: "#e2e8f0" }}
                  formatter={(value, name, item) => {
                    const row = item.payload as InsertBarRow
                    const rawKey = `${String(name)}_raw`
                    const rawValue = rawKey in row ? Number(row[rawKey]) : Number(value ?? 0)
                    return formatDurationSeconds(rawValue)
                  }}
                  cursor={{ fill: "rgba(15, 23, 42, 0.3)" }}
                />
                {databases.map((db) => (
                  <Bar
                    key={db}
                    dataKey={db}
                    name={db}
                    fill={databaseColors[db] ?? "#94a3b8"}
                    radius={[0, 4, 4, 0]}
                    barSize={8}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </ChartFrame>

        {populateSamples.length <= 1 ? (
          <div className="flex items-center justify-center rounded-xl border border-dashed border-border-default bg-surface-inset px-6 py-8 text-sm text-slate-500">
            No resource metrics were recorded during populate.
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            <MetricMiniChart
              label="CPU during populate"
              data={cpuChartData}
              databases={databases}
              databaseColors={databaseColors}
              maxElapsed={maxElapsed}
              elapsedTicks={elapsedTicks}
              formatter={formatCpuPercent}
              scaleBuilder={toMetricScale}
              syncId="insert-metrics"
            />
            <MetricMiniChart
              label="Memory during populate"
              data={memChartData}
              databases={databases}
              databaseColors={databaseColors}
              maxElapsed={maxElapsed}
              elapsedTicks={elapsedTicks}
              formatter={formatMegabytes}
              scaleBuilder={toMemoryScale}
              syncId="insert-metrics"
            />
          </div>
        )}
      </div>
    </PanelCard>
  )
}

function MetricMiniChart({
  label,
  data,
  databases,
  databaseColors,
  maxElapsed,
  elapsedTicks,
  formatter,
  scaleBuilder,
  syncId,
}: {
  label: string
  data: MetricChartRow[]
  databases: string[]
  databaseColors: Record<string, string>
  maxElapsed: number
  elapsedTicks: number[]
  formatter: (v: number) => string
  scaleBuilder: (maxValue: number) => {
    domain: [number, number]
    ticks: number[]
    formatter?: (v: number) => string
  }
  syncId: string
}) {
  const values = data.flatMap((row) => databases.map((db) => (row[db] as number | undefined) ?? 0))
  const maxVal = Math.max(1, ...values)
  const yScale = scaleBuilder(maxVal)

  return (
    <ChartFrame>
      <p className="mb-2 text-sm font-medium text-slate-200">{label}</p>
      <div className="h-32">
        <ResponsiveContainer
          width="100%"
          height="100%"
          initialDimension={{ width: 400, height: 128 }}
        >
          <LineChart data={data} syncId={syncId} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CpuGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />
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
              tickFormatter={yScale.formatter ?? formatter}
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
              labelFormatter={(v) =>
                `Elapsed ${formatElapsedLabel(typeof v === "number" ? v : Number(v ?? 0))}`
              }
              formatter={(value, _name, item) =>
                [formatter(Number(value ?? 0)), item.name ?? ""] as const
              }
            />
            {databases.map((db) => (
              <Line
                key={db}
                type="stepAfter"
                name={db}
                dataKey={(row: MetricChartRow) => row[db]}
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
    </ChartFrame>
  )
}

function buildInsertBarData(
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

function buildMetricRows(
  samples: MetricSample[],
  metric: "cpu_percent" | "mem_mb",
): MetricChartRow[] {
  const rowsBySecond = new Map<number, MetricChartRow>()

  for (const sample of samples) {
    const second = Math.max(0, Math.round(sample.elapsed_s))
    const existing = rowsBySecond.get(second) ?? ({ elapsed_s: second } as MetricChartRow)
    existing[sample.db] = sample[metric]
    rowsBySecond.set(second, existing)
  }

  return Array.from(rowsBySecond.values()).sort((a, b) => a.elapsed_s - b.elapsed_s)
}

const ELAPSED_STEPS = [
  1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 43200, 86400,
]

function buildEvenTicks(maxSeconds: number): number[] {
  const safeMax = Math.max(1, Math.ceil(maxSeconds))
  const target = safeMax / 5
  const step = ELAPSED_STEPS.find((s) => s >= target) ?? ELAPSED_STEPS[ELAPSED_STEPS.length - 1]!
  const ticks: number[] = []
  for (let t = 0; t <= safeMax; t += step) ticks.push(t)
  return ticks
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
