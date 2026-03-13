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
import type { QueryComparisonRow } from "./QueryComparisonTable"
import { SqlCodeView } from "./SqlCodeView"

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
  const [chartScaleMode, setChartScaleMode] = useState<DurationScaleMode>("log")

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

  return (
    <div className="animate-panel-enter flex h-full min-h-0 flex-col gap-4 rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
      <div className="flex items-start justify-between">
        <div>
          <h4 className="text-lg font-semibold text-slate-50">{row.query_label}</h4>
          <p className="mt-0.5 text-sm text-slate-400">
            {row.table_family} · Q{row.query_id} · Fastest: {row.fastest_db} · Spread:{" "}
            {formatMultiplier(row.spread_ratio)}
          </p>
        </div>
        <button
          onClick={onClose}
          className="rounded-lg px-3 py-1.5 text-xs font-medium text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
        >
          Close
        </button>
      </div>

      <div className="rounded-2xl border border-slate-800 bg-slate-950/50 p-4">
        <div className="mb-4 flex items-center justify-end">
          <DurationScaleToggle mode={chartScaleMode} onChange={setChartScaleMode} />
        </div>
        <ResponsiveContainer width="100%" height={Math.max(170, chartData.length * 30)}>
          <BarChart
            data={visibleChartData}
            layout="vertical"
            margin={{ top: 8, right: 20, bottom: 8, left: 8 }}
          >
            <CartesianGrid stroke="#1e293b" horizontal={false} />
            <XAxis
              type="number"
              domain={axisDomain}
              ticks={axisTicks}
              allowDataOverflow
              tick={{ fill: "#94a3b8", fontSize: 11 }}
              axisLine={{ stroke: "#334155" }}
              tickLine={{ stroke: "#334155" }}
              tickFormatter={(value: number) => formatDurationAxisTick(value, chartScaleMode)}
            />
            <YAxis
              type="category"
              dataKey="db"
              width={132}
              tick={{ fill: "#cbd5e1", fontSize: 12 }}
              axisLine={{ stroke: "#334155" }}
              tickLine={{ stroke: "#334155" }}
              tickFormatter={truncateAxisLabel}
            />
            <Tooltip
              cursor={{ fill: "rgba(15, 23, 42, 0.45)" }}
              contentStyle={{
                backgroundColor: "#020617",
                border: "1px solid #334155",
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
                  <div className="rounded-xl border border-slate-700 bg-slate-950/95 px-3 py-2 text-xs text-slate-200 shadow-2xl">
                    <p className="font-medium text-slate-50">{String(label)}</p>
                    <p className="mt-1 text-slate-300">
                      Min {formatDurationSeconds(entry.stats.min_duration_s)}
                    </p>
                    <p className="text-cyan-300">
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
                stroke="rgba(103, 232, 249, 0.98)"
                strokeWidth={2.25}
                isAnimationActive
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {hasTabs ? (
        <div className="flex min-h-0 flex-1 flex-col rounded-2xl border border-slate-700 bg-slate-950">
          <div className="flex shrink-0 gap-1 border-b border-slate-800 px-4 py-2">
            {sql?.sql !== null ? (
              <button
                className={
                  activeTab === "common"
                    ? "rounded-md border border-cyan-500/30 bg-cyan-500/10 px-3 py-1 text-xs font-medium text-cyan-300"
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
                    ? "rounded-md border border-cyan-500/30 bg-cyan-500/10 px-3 py-1 text-xs font-medium text-cyan-300"
                    : "rounded-md px-3 py-1 text-xs font-medium text-slate-400 hover:text-slate-200"
                }
                onClick={() => setActiveTab(db)}
              >
                {db}
              </button>
            ))}
          </div>
          <div className="panel-scrollbar min-h-0 flex-1 overflow-x-hidden overflow-y-auto">
            <SqlCodeView code={activeSql ?? "No SQL available"} />
          </div>
        </div>
      ) : (
        <div className="flex min-h-0 flex-1 items-center justify-center rounded-2xl border border-dashed border-slate-800 bg-slate-950/40 px-6 text-center text-sm text-slate-500">
          No SQL available for this query.
        </div>
      )}
    </div>
  )
}

function truncateAxisLabel(value: string): string {
  const maxLength = 18
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength - 3)}...`
}
