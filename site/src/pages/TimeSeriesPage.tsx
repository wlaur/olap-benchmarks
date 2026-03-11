import { createColumnHelper } from "@tanstack/react-table"
import { startTransition, useEffect, useMemo, useState } from "react"
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

import { DatabaseLegend } from "../components/DatabaseLegend"
import { DatabaseMultiSelect } from "../components/filters/DatabaseMultiSelect"
import { QueryComparisonTable, type QueryComparisonRow } from "../components/QueryComparisonTable"
import { QueryDetailPanel } from "../components/QueryDetailPanel"
import { QueryTable } from "../components/QueryTable"
import { StatCard } from "../components/StatCard"
import { useSelectionState } from "../hooks/useSelectionState"
import { formatDurationSeconds } from "../lib/format"
import {
  fetchQueriesManifest,
  fetchTimeSeriesQuerySummaries,
  fetchTimeSeriesRunSummaries,
} from "../lib/queries"
import type { QueriesManifest, TimeSeriesQuerySummary, TimeSeriesRunSummary } from "../lib/types"

interface TimeSeriesPageProps {
  system: string
}

interface TimeSeriesPageState {
  loading: boolean
  error: string | null
  runSummaries: TimeSeriesRunSummary[]
  querySummaries: TimeSeriesQuerySummary[]
  queriesManifest: QueriesManifest | null
}

const LOG_FLOOR = 1e-6

const DATABASE_COLORS = ["#38bdf8", "#f97316", "#34d399", "#facc15", "#f472b6", "#a78bfa"]

const runColumnHelper = createColumnHelper<TimeSeriesRunSummary>()

const RUN_COLUMNS = [
  runColumnHelper.accessor("db", { header: "Database" }),
  runColumnHelper.accessor("db_version", { header: "Version" }),
  runColumnHelper.accessor("started_at", { header: "Started" }),
  runColumnHelper.accessor("finished_at", { header: "Finished" }),
  runColumnHelper.accessor("run_duration_s", {
    header: "Run Duration",
    cell: (info) => formatDurationSeconds(info.getValue()),
  }),
  runColumnHelper.accessor("median_query_duration_s", {
    header: "Median Query",
    cell: (info) => {
      const value = info.getValue()
      return value === null ? "—" : formatDurationSeconds(value)
    },
  }),
  runColumnHelper.accessor("query_count", { header: "Queries" }),
]

export function TimeSeriesPage({ system }: TimeSeriesPageProps) {
  const [state, setState] = useState<TimeSeriesPageState>({
    loading: true,
    error: null,
    runSummaries: [],
    querySummaries: [],
    queriesManifest: null,
  })
  const [selectedDatabases, setSelectedDatabases] = useState<string[]>([])
  const selection = useSelectionState()

  useEffect(() => {
    let cancelled = false

    setState({
      loading: true,
      error: null,
      runSummaries: [],
      querySummaries: [],
      queriesManifest: null,
    })

    Promise.all([
      fetchTimeSeriesRunSummaries(system),
      fetchTimeSeriesQuerySummaries(system),
      fetchQueriesManifest().catch(() => null),
    ])
      .then(([runSummaries, querySummaries, queriesManifest]) => {
        if (cancelled) return

        startTransition(() => {
          setState({
            loading: false,
            error: null,
            runSummaries,
            querySummaries,
            queriesManifest,
          })
        })
      })
      .catch((nextError) => {
        if (cancelled) return

        startTransition(() => {
          setState({
            loading: false,
            error: String(nextError),
            runSummaries: [],
            querySummaries: [],
            queriesManifest: null,
          })
        })
      })

    return () => {
      cancelled = true
    }
  }, [system])

  const databases = useMemo(
    () => Array.from(new Set(state.runSummaries.map((run) => run.db))).sort(),
    [state.runSummaries],
  )

  useEffect(() => {
    setSelectedDatabases((currentSelection) => {
      if (databases.length === 0) return []

      const nextSelection = databases.filter((database) => currentSelection.includes(database))
      const resolvedSelection = nextSelection.length > 0 ? nextSelection : databases

      if (
        resolvedSelection.length === currentSelection.length &&
        resolvedSelection.every((database, index) => database === currentSelection[index])
      ) {
        return currentSelection
      }

      return resolvedSelection
    })
  }, [databases])

  const includedDatabases = selectedDatabases.length > 0 ? selectedDatabases : databases
  const includedDatabaseSet = new Set(includedDatabases)
  const filteredRunSummaries = state.runSummaries.filter((run) => includedDatabaseSet.has(run.db))
  const filteredQuerySummaries = state.querySummaries.filter((row) =>
    includedDatabaseSet.has(row.db),
  )

  const databaseColors = Object.fromEntries(
    databases.map((db, idx) => [db, DATABASE_COLORS[idx % DATABASE_COLORS.length]!]),
  )

  const queryRows = buildQueryComparisonRows(filteredQuerySummaries, includedDatabases)

  const globalMaxDuration = queryRows.reduce((max, row) => {
    for (const val of Object.values(row.by_database)) {
      if (val !== null && val > max) {
        max = val
      }
    }
    return max
  }, LOG_FLOOR)

  const fastestRun = filteredRunSummaries[0] ?? null
  const slowestQuery = filteredQuerySummaries.reduce<TimeSeriesQuerySummary | null>(
    (currentSlowest, qs) => {
      if (!currentSlowest || qs.median_duration_s > currentSlowest.median_duration_s) {
        return qs
      }
      return currentSlowest
    },
    null,
  )
  const queryCount = new Set(filteredQuerySummaries.map((row) => row.query_name)).size

  const runChartData = filteredRunSummaries.map((run) => ({
    db: run.db,
    duration_s: Math.max(run.run_duration_s, LOG_FLOOR),
    fill: databaseColors[run.db] ?? "#94a3b8",
  }))

  const selectedRow = selection.selectedQuery
    ? (queryRows.find((r) => r.query_name === selection.selectedQuery) ?? null)
    : null

  const selectedSql = state.queriesManifest?.time_series?.[selection.selectedQuery ?? ""] ?? null

  function toggleDatabase(database: string) {
    setSelectedDatabases((currentSelection) => {
      const nextSelection = currentSelection.includes(database)
        ? currentSelection.filter((value) => value !== database)
        : [...currentSelection, database].sort()

      return nextSelection.length > 0 ? nextSelection : currentSelection
    })
  }

  return (
    <section className="space-y-10">
      <header className="max-w-4xl space-y-4">
        <p className="text-sm font-medium tracking-[0.18em] text-cyan-300 uppercase">Time Series</p>
        <h2 className="text-4xl font-semibold tracking-tight text-slate-50">
          Query-by-query latency comparison
        </h2>
        <p className="text-lg leading-8 text-slate-300">
          Click any query row to see a detailed comparison across databases with the actual SQL.
          Hover to highlight.
        </p>
      </header>

      {state.loading ? (
        <p className="text-sm text-slate-400">Loading completed time-series runs for {system}...</p>
      ) : null}

      {state.error ? (
        <p className="text-sm text-red-300">Failed to load time-series data: {state.error}</p>
      ) : null}

      {!state.loading && !state.error && state.runSummaries.length === 0 ? (
        <p className="text-sm text-slate-400">
          No completed time-series runs were found for {system}.
        </p>
      ) : null}

      {!state.loading && !state.error && state.runSummaries.length > 0 ? (
        <>
          <DatabaseMultiSelect
            databases={databases}
            selectedDatabases={includedDatabases}
            onSelectAll={() => setSelectedDatabases(databases)}
            onToggleDatabase={toggleDatabase}
          />

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <StatCard
              label="System"
              value={system}
              detail="Global app scope. Cross-system comparisons stay out of the UI."
            />
            <StatCard
              label="Completed Runs"
              value={String(filteredRunSummaries.length)}
              detail={`${includedDatabases.length} databases across ${queryCount} benchmark queries.`}
            />
            <StatCard
              label="Fastest Full Run"
              value={
                fastestRun
                  ? `${fastestRun.db} · ${formatDurationSeconds(fastestRun.run_duration_s)}`
                  : "—"
              }
              detail={
                fastestRun?.median_query_duration_s !== null &&
                fastestRun?.median_query_duration_s !== undefined
                  ? `Median query ${formatDurationSeconds(fastestRun.median_query_duration_s)}`
                  : fastestRun
                    ? "Median query unavailable"
                    : undefined
              }
            />
            <StatCard
              label="Slowest Query Median"
              value={slowestQuery ? formatDurationSeconds(slowestQuery.median_duration_s) : "—"}
              detail={
                slowestQuery
                  ? `${formatTimeSeriesQueryName(slowestQuery.query_name).queryLabel} on ${slowestQuery.db}`
                  : undefined
              }
            />
          </div>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">Full benchmark run duration</h3>
              <p className="text-sm text-slate-400">
                Total time per database (log scale). The per-query view below explains where the
                differences come from.
              </p>
            </div>

            <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={runChartData} margin={{ top: 16, right: 16, bottom: 16, left: 16 }}>
                  <CartesianGrid stroke="#1e293b" vertical={false} />
                  <XAxis
                    dataKey="db"
                    tick={{ fill: "#94a3b8" }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                  />
                  <YAxis
                    scale="log"
                    domain={[LOG_FLOOR, "auto"]}
                    allowDataOverflow
                    tick={{ fill: "#94a3b8" }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                    tickFormatter={(value: number) => formatDurationSeconds(value)}
                  />
                  <Tooltip
                    cursor={{ fill: "rgba(15, 23, 42, 0.55)" }}
                    contentStyle={{
                      backgroundColor: "#020617",
                      border: "1px solid #334155",
                      borderRadius: 16,
                    }}
                    formatter={(value: number) => formatDurationSeconds(value)}
                  />
                  <Bar dataKey="duration_s" radius={[10, 10, 0, 0]}>
                    {runChartData.map((entry) => (
                      <Cell key={entry.db} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">Query latency comparison</h3>
              <p className="text-sm text-slate-400">
                Each row shows median latency per database on a log scale. Click a row to see
                detailed comparison and SQL.
              </p>
            </div>

            <DatabaseLegend databases={includedDatabases} databaseColors={databaseColors} />

            <QueryComparisonTable
              rows={queryRows}
              databases={includedDatabases}
              databaseColors={databaseColors}
              selection={selection}
              maxDuration={globalMaxDuration}
            />

            {selectedRow ? (
              <QueryDetailPanel
                row={selectedRow}
                databases={includedDatabases}
                databaseColors={databaseColors}
                sql={selectedSql}
                onClose={() => selection.setSelectedQuery(null)}
              />
            ) : null}
          </section>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">Completed run details</h3>
              <p className="text-sm text-slate-400">
                Full run metadata for the currently included databases.
              </p>
            </div>
            <QueryTable data={filteredRunSummaries} columns={RUN_COLUMNS} />
          </section>
        </>
      ) : null}
    </section>
  )
}

function buildQueryComparisonRows(
  querySummaries: TimeSeriesQuerySummary[],
  databases: string[],
): QueryComparisonRow[] {
  const groupedQueries = new Map<string, TimeSeriesQuerySummary[]>()

  for (const querySummary of querySummaries) {
    const existingRows = groupedQueries.get(querySummary.query_name) ?? []
    existingRows.push(querySummary)
    groupedQueries.set(querySummary.query_name, existingRows)
  }

  return Array.from(groupedQueries.entries())
    .sort(([leftName], [rightName]) => leftName.localeCompare(rightName))
    .map(([queryName, rows]) => {
      const { category, queryLabel, scale } = formatTimeSeriesQueryName(queryName)
      const byDatabase = Object.fromEntries(
        databases.map((database) => [database, null]),
      ) as Record<string, number | null>

      for (const row of rows) {
        byDatabase[row.db] = row.median_duration_s
      }

      const numericDurations = Object.values(byDatabase).filter((value) => value !== null)
      const fastestDuration = Math.min(...numericDurations)
      const slowestDuration = Math.max(...numericDurations)
      const fastestDb = rows.find((row) => row.median_duration_s === fastestDuration)?.db ?? "—"

      return {
        query_name: queryName,
        query_label: queryLabel,
        category,
        scale,
        fastest_db: fastestDb,
        spread_ratio: slowestDuration / fastestDuration,
        by_database: byDatabase,
      }
    })
}

function formatTimeSeriesQueryName(queryName: string): {
  category: string
  queryLabel: string
  scale: string
} {
  const normalizedName = queryName.replace(/^\d+_/, "")
  const scale = normalizedName.includes("_small_")
    ? "Small wide"
    : normalizedName.includes("_large_")
      ? "Large wide"
      : "Wide"
  const cleanedName = normalizedName.replace(/_(small|large)_wide$/, "").replace(/_wide$/, "")
  const queryLabel = toTitleCase(cleanedName.replace(/_/g, " "))

  return {
    category: getTimeSeriesCategory(normalizedName),
    queryLabel,
    scale,
  }
}

function getTimeSeriesCategory(queryName: string): string {
  if (queryName.includes("export")) return "Export"
  if (queryName.includes("resample")) return "Resample"
  if (queryName.includes("aggregate")) return "Aggregate"
  if (queryName.includes("scalar")) return "Scalar lookup"
  if (queryName.includes("raw_filtered")) return "Filtered scan"
  if (queryName.includes("raw")) return "Raw scan"
  if (queryName.includes("conditional")) return "Conditional aggregate"
  if (queryName.includes("max_time") || queryName.includes("latest_time_range")) {
    return "Time boundary"
  }
  return "Other"
}

function toTitleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toUpperCase())
}
