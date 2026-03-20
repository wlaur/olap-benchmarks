import { useEffect, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  ErrorBar,
  Rectangle,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import {
  formatDurationAxisTick,
  formatDurationSeconds,
  formatMultiplier,
  getDurationAxisDomain,
  getDurationAxisTicks,
  scaleDurationForChart,
  type DurationScaleMode,
} from "../lib/format"
import type { QuerySqlEntry } from "../lib/types"
import { DurationScaleToggle } from "./DurationScaleToggle"
import { ChartFrame, PanelCard } from "./layout/Panel"
import type { QueryComparisonRow } from "./QueryComparisonTable"
import { SqlCodeView } from "./SqlCodeView"
import { BodyText, SectionTitle } from "./Typography"

interface QueryDetailPanelProps {
  row: QueryComparisonRow
  databases: string[]
  databaseColors: Record<string, string>
  sql: QuerySqlEntry | null
  onClose: () => void
}

export function QueryDetailPanel({
  row,
  databases,
  databaseColors,
  sql,
  onClose,
}: QueryDetailPanelProps) {
  const overrideKeys = sql
    ? databases.filter((database) => Object.hasOwn(sql.db_overrides, database))
    : []
  const hasTabs = sql !== null && (sql.sql !== null || overrideKeys.length > 0)
  const defaultTab = sql?.sql !== null ? "common" : (overrideKeys[0] ?? "common")
  const [activeTab, setActiveTab] = useState(defaultTab)
  const [chartScaleMode, setChartScaleMode] = useState<DurationScaleMode>("linear")

  useEffect(() => {
    setActiveTab(defaultTab)
  }, [defaultTab, row.query_name])

  const chartData = databases
    .map((db) => {
      const raw = row.by_database[db]
      const stats = row.stats_by_database[db]
      const medianDuration = raw ?? 0
      const chartDuration = scaleDurationForChart(medianDuration, chartScaleMode)
      const minDuration = stats?.min_duration_s ?? medianDuration
      const maxDuration = stats?.max_duration_s ?? medianDuration
      const minChartDuration = scaleDurationForChart(minDuration, chartScaleMode)
      const maxChartDuration = scaleDurationForChart(maxDuration, chartScaleMode)

      return {
        db,
        duration: medianDuration,
        chart_duration: chartDuration,
        chart_error: [
          Math.max(0, chartDuration - minChartDuration),
          Math.max(0, maxChartDuration - chartDuration),
        ] as const,
        chart_error_span: Math.max(0, maxChartDuration - minChartDuration),
        stats,
        raw,
        fill: databaseColors[db] ?? "#94a3b8",
      }
    })
    .filter((d) => d.raw !== null)
  const maxDuration = Math.max(
    0,
    ...chartData.map((entry) => entry.stats?.max_duration_s ?? entry.duration),
  )
  const axisDomain = getDurationAxisDomain(maxDuration, chartScaleMode)
  const axisTicks = getDurationAxisTicks(maxDuration, chartScaleMode)
  const chartSpan = Math.max(0, axisDomain[1] - axisDomain[0])
  const minVisibleErrorSpan = chartSpan * 0.018
  const visibleChartData = chartData.map((entry) => ({
    ...entry,
    chart_error: entry.chart_error_span >= minVisibleErrorSpan ? entry.chart_error : undefined,
  }))

  const activeSql = activeTab === "common" ? sql?.sql : sql?.db_overrides[activeTab]
  const chartHeight = Math.max(180, chartData.length * 34)

  return (
    <PanelCard className="animate-panel-enter flex h-full min-h-0 min-w-0 flex-col gap-4 overflow-hidden rounded-2xl p-5">
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
        <div>
          <SectionTitle as="h4">{row.query_label}</SectionTitle>
          <BodyText className="mt-0.5">
            {row.table_family} · Q{row.query_id} · Fastest: {row.fastest_db} · Spread:{" "}
            {formatMultiplier(row.spread_ratio)}
          </BodyText>
        </div>
        <button
          onClick={onClose}
          className="rounded-lg px-3 py-1.5 text-xs font-medium text-slate-400 transition-colors hover:bg-surface-inset hover:text-slate-200"
        >
          Close
        </button>
      </div>

      <ChartFrame className="min-h-0 min-w-0 p-4">
        <div className="mb-4 flex items-center justify-end">
          <DurationScaleToggle mode={chartScaleMode} onChange={setChartScaleMode} />
        </div>
        <div className="panel-scrollbar min-h-0 min-w-0 overflow-auto">
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer
              width="100%"
              height="100%"
              initialDimension={{ width: 420, height: chartHeight }}
            >
              <BarChart
                data={visibleChartData}
                layout="vertical"
                margin={{ top: 8, right: 20, bottom: 8, left: 8 }}
              >
                <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" horizontal={false} />
                <XAxis
                  type="number"
                  domain={axisDomain}
                  ticks={axisTicks}
                  allowDataOverflow
                  tick={{ fill: "#64748b", fontSize: 11 }}
                  axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickFormatter={(value: number) => formatDurationAxisTick(value, chartScaleMode)}
                />
                <YAxis
                  type="category"
                  dataKey="db"
                  width={132}
                  tick={{ fill: "#94a3b8", fontSize: 12 }}
                  axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
                  tickFormatter={truncateAxisLabel}
                />
                <Tooltip
                  cursor={{ fill: "rgba(15, 23, 42, 0.3)" }}
                  contentStyle={{
                    backgroundColor: "#1e2330",
                    border: "1px solid rgba(148, 163, 184, 0.12)",
                    borderRadius: 12,
                    color: "#e2e8f0",
                  }}
                  labelStyle={{ color: "#e2e8f0" }}
                  itemStyle={{ color: "#e2e8f0" }}
                  content={({ active, label, payload }) => {
                    if (!active || !payload || payload.length === 0) return null

                    const entry = payload[0]?.payload as
                      | {
                          db: string
                          stats: QueryComparisonRow["stats_by_database"][string]
                        }
                      | undefined

                    if (!entry?.stats) return null

                    return (
                      <div className="rounded-xl border border-border-default bg-surface-primary/95 px-3 py-2 text-xs text-slate-200 shadow-2xl">
                        <p className="font-medium text-slate-50">{String(label)}</p>
                        <p className="mt-1 text-slate-300">
                          Min {formatDurationSeconds(entry.stats.min_duration_s)}
                        </p>
                        <p className="text-accent-300">
                          Median {formatDurationSeconds(entry.stats.median_duration_s)}
                        </p>
                        <p className="text-slate-300">
                          Max {formatDurationSeconds(entry.stats.max_duration_s)}
                        </p>
                        <p className="text-slate-400">
                          Median / max{" "}
                          {(entry.stats.max_duration_s / entry.stats.median_duration_s).toFixed(2)}x
                        </p>
                        <p className="mt-1 text-slate-500">{entry.stats.iterations} runs</p>
                      </div>
                    )
                  }}
                />
                <Bar
                  dataKey="chart_duration"
                  name="Duration"
                  radius={[0, 6, 6, 0]}
                  shape={(props) => (
                    <Rectangle
                      {...props}
                      fill={(props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8"}
                    />
                  )}
                >
                  <ErrorBar
                    dataKey="chart_error"
                    width={7}
                    stroke="rgba(30, 41, 59, 0.72)"
                    strokeWidth={4}
                    isAnimationActive
                  />
                  <ErrorBar
                    dataKey="chart_error"
                    width={5}
                    stroke="rgba(108, 142, 239, 0.8)"
                    strokeWidth={2.25}
                    isAnimationActive
                  />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </ChartFrame>

      {hasTabs ? (
        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden rounded-xl border border-border-default bg-surface-inset">
          <div className="flex shrink-0 gap-1 border-b border-border-default px-4 py-2">
            {sql?.sql !== null ? (
              <button
                className={
                  activeTab === "common"
                    ? "rounded-md border border-accent-500/30 bg-accent-500/10 px-3 py-1 text-xs font-medium text-accent-300"
                    : "rounded-md px-3 py-1 text-xs font-medium text-slate-400 hover:text-slate-200"
                }
                onClick={() => setActiveTab("common")}
              >
                SQL
              </button>
            ) : null}
            {overrideKeys.map((db) => (
              <button
                key={db}
                className={
                  activeTab === db
                    ? "rounded-md border border-accent-500/30 bg-accent-500/10 px-3 py-1 text-xs font-medium text-accent-300"
                    : "rounded-md px-3 py-1 text-xs font-medium text-slate-400 hover:text-slate-200"
                }
                onClick={() => setActiveTab(db)}
              >
                {db}
              </button>
            ))}
          </div>
          <div className="min-h-0 min-w-0 flex-1 overflow-hidden">
            <SqlCodeView code={activeSql ?? "No SQL available"} />
          </div>
        </div>
      ) : (
        <div className="flex min-h-0 flex-1 items-center justify-center rounded-xl border border-dashed border-border-default bg-surface-inset px-6 text-center text-sm text-slate-500">
          No SQL available for this query.
        </div>
      )}
    </PanelCard>
  )
}

function truncateAxisLabel(value: string): string {
  const maxLength = 18
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength - 3)}...`
}
