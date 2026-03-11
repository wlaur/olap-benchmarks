import { useEffect, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import { formatDurationSeconds, formatMultiplier } from "../lib/format"
import type { QuerySqlEntry } from "../lib/types"
import type { QueryComparisonRow } from "./QueryComparisonTable"

const LOG_FLOOR = 1e-6

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
  const overrideKeys = sql ? Object.keys(sql.db_overrides) : []
  const hasTabs = sql !== null && (sql.sql !== null || overrideKeys.length > 0)
  const defaultTab =
    sql?.sql !== null ? "common" : (overrideKeys[0] ?? "common")
  const [activeTab, setActiveTab] = useState(defaultTab)

  useEffect(() => {
    setActiveTab(defaultTab)
  }, [defaultTab, row.query_name])

  const chartData = databases
    .map((db) => ({
      db,
      duration: Math.max(row.by_database[db] ?? 0, LOG_FLOOR),
      raw: row.by_database[db],
      fill: databaseColors[db] ?? "#94a3b8",
    }))
    .filter((d) => d.raw !== null)

  const activeSql =
    activeTab === "common" ? sql?.sql : sql?.db_overrides[activeTab]

  return (
    <div className="animate-panel-enter space-y-5 rounded-2xl border border-slate-800 bg-slate-900/70 p-6">
      <div className="flex items-start justify-between">
        <div>
          <h4 className="text-lg font-semibold text-slate-50">
            {row.query_label}
          </h4>
          <p className="mt-0.5 text-sm text-slate-400">
            {row.category} · {row.scale} · Fastest: {row.fastest_db} · Spread:{" "}
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

      <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
        <ResponsiveContainer
          width="100%"
          height={Math.max(180, chartData.length * 36)}
        >
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ top: 8, right: 20, bottom: 8, left: 8 }}
          >
            <CartesianGrid stroke="#1e293b" horizontal={false} />
            <XAxis
              type="number"
              scale="log"
              domain={[LOG_FLOOR, "auto"]}
              allowDataOverflow
              tick={{ fill: "#94a3b8", fontSize: 11 }}
              axisLine={{ stroke: "#334155" }}
              tickLine={{ stroke: "#334155" }}
              tickFormatter={(value: number) => formatDurationSeconds(value)}
            />
            <YAxis
              type="category"
              dataKey="db"
              width={100}
              tick={{ fill: "#cbd5e1", fontSize: 12 }}
              axisLine={{ stroke: "#334155" }}
              tickLine={{ stroke: "#334155" }}
            />
            <Tooltip
              cursor={{ fill: "rgba(15, 23, 42, 0.45)" }}
              contentStyle={{
                backgroundColor: "#020617",
                border: "1px solid #334155",
                borderRadius: 12,
              }}
              formatter={(value: number) => formatDurationSeconds(value)}
            />
            <Bar dataKey="duration" radius={[0, 6, 6, 0]}>
              {chartData.map((entry) => (
                <Cell key={entry.db} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {hasTabs ? (
        <div className="rounded-xl border border-slate-700 bg-slate-950">
          <div className="flex gap-1 border-b border-slate-800 px-4 py-2">
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
          <pre className="overflow-x-auto p-4 font-mono text-sm leading-relaxed text-slate-300">
            {activeSql ?? "No SQL available"}
          </pre>
        </div>
      ) : null}
    </div>
  )
}
