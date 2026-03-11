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

interface DatabaseAggregateStats {
  db: string
  score: number | null
  wins: number
}

const LOG_FLOOR = 1e-6

const DATABASE_COLORS = ["#38bdf8", "#f97316", "#34d399", "#facc15", "#f472b6", "#a78bfa"]

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
  const databaseAggregateStats = buildDatabaseAggregateStats(queryRows, includedDatabases)
  const scoreLeader =
    databaseAggregateStats
      .filter((entry) => entry.score !== null)
      .sort((left, right) => (right.score ?? 0) - (left.score ?? 0))[0] ?? null
  const winsLeader =
    [...databaseAggregateStats].sort(
      (left, right) => right.wins - left.wins || left.db.localeCompare(right.db),
    )[0] ?? null
  const widestSpreadRow =
    [...queryRows].sort((left, right) => right.spread_ratio - left.spread_ratio)[0] ?? null

  const fastestRun = filteredRunSummaries[0] ?? null
  const queryCount = new Set(filteredQuerySummaries.map((row) => row.query_name)).size
  const versionByDatabase = Object.fromEntries(
    filteredRunSummaries.map((run) => [run.db, run.db_version]),
  )

  const runChartData = filteredRunSummaries.map((run) => ({
    db: run.db,
    duration_s: run.run_duration_s,
    fill: databaseColors[run.db] ?? "#94a3b8",
  }))
  const overviewChartHeight = Math.max(120, Math.min(170, runChartData.length * 28 + 28))

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

  if (state.loading) {
    return (
      <section className="flex h-full min-h-0 items-center justify-center">
        <div className="rounded-3xl border border-slate-800 bg-slate-900/70 px-6 py-5 text-sm text-slate-400">
          Loading completed time-series runs for {system}...
        </div>
      </section>
    )
  }

  if (state.error) {
    return (
      <section className="flex h-full min-h-0 items-center justify-center">
        <div className="rounded-3xl border border-red-500/30 bg-red-950/20 px-6 py-5 text-sm text-red-300">
          Failed to load time-series data: {state.error}
        </div>
      </section>
    )
  }

  if (state.runSummaries.length === 0) {
    return (
      <section className="flex h-full min-h-0 items-center justify-center">
        <div className="rounded-3xl border border-slate-800 bg-slate-900/70 px-6 py-5 text-sm text-slate-400">
          No completed time-series runs were found for {system}.
        </div>
      </section>
    )
  }

  return (
    <section className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
      <div className="grid shrink-0 gap-4 xl:grid-cols-[minmax(24rem,0.95fr)_minmax(0,1.15fr)]">
        <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
          <div className="space-y-2">
            <p className="text-sm font-medium tracking-[0.18em] text-cyan-300 uppercase">
              Time Series
            </p>
            <h2 className="text-3xl font-semibold tracking-tight text-slate-50">
              Latency workbench
            </h2>
            <p className="max-w-2xl text-sm leading-6 text-slate-300">
              Scope the comparison to the databases you care about, scan the aggregate spread, then
              drill into query-level behavior and SQL without leaving the screen.
            </p>
          </div>

          <div className="mt-5">
            <DatabaseMultiSelect
              databases={databases}
              selectedDatabases={includedDatabases}
              onSelectAll={() => setSelectedDatabases(databases)}
              onToggleDatabase={toggleDatabase}
            />
          </div>
        </div>

        <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h3 className="text-lg font-semibold text-slate-50">Aggregate overview</h3>
              <p className="mt-1 text-sm text-slate-400">
                Full-run duration by database on a direct scale. This top view should tell you where
                to investigate before diving into per-query detail.
              </p>
            </div>
            <div className="rounded-full border border-slate-800 bg-slate-950/80 px-3 py-1 text-xs font-medium text-slate-400">
              {includedDatabases.length} of {databases.length} databases
            </div>
          </div>

          <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_15rem]">
            <div
              className="h-full rounded-2xl border border-slate-800 bg-slate-950/50 p-4"
              style={{ minHeight: overviewChartHeight }}
            >
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={runChartData} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke="#1e293b" vertical={false} />
                  <XAxis
                    dataKey="db"
                    tick={{ fill: "#94a3b8", fontSize: 11 }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                  />
                  <YAxis
                    domain={[0, "auto"]}
                    tick={{ fill: "#94a3b8", fontSize: 11 }}
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

            <div className="space-y-3">
              {databaseAggregateStats.slice(0, 3).map((entry, index) => (
                <div
                  key={entry.db}
                  className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3"
                >
                  <p className="text-xs font-medium tracking-[0.18em] text-slate-500 uppercase">
                    Rank {index + 1}
                  </p>
                  <div className="mt-2 flex items-center justify-between gap-3">
                    <div>
                      <p className="text-lg font-semibold text-slate-100">{entry.db}</p>
                      <p className="text-xs text-slate-500">
                        {versionByDatabase[entry.db] ?? "Version unavailable"}
                      </p>
                    </div>
                    <p className="text-sm font-medium text-cyan-300">
                      {entry.score === null ? "—" : `${entry.score.toFixed(1)} score`}
                    </p>
                  </div>
                  <p className="mt-1 text-xs text-slate-400">
                    {entry.wins} query wins across {queryCount} benchmark queries
                  </p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="grid shrink-0 gap-3 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Score Leader"
          value={scoreLeader ? `${scoreLeader.db} · ${scoreLeader.score?.toFixed(1)}` : "—"}
          detail={
            scoreLeader
              ? `${versionByDatabase[scoreLeader.db] ?? "Version unavailable"} · 100 means fastest on every query.`
              : "Geometric score vs fastest query result."
          }
        />
        <StatCard
          label="Query Wins Leader"
          value={winsLeader ? `${winsLeader.db} · ${winsLeader.wins}` : "—"}
          detail={
            winsLeader
              ? `${versionByDatabase[winsLeader.db] ?? "Version unavailable"} · ${queryCount} queries across ${includedDatabases.length} included databases.`
              : `${queryCount} queries across ${includedDatabases.length} included databases.`
          }
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
              ? `${fastestRun.db_version} · Median query ${formatDurationSeconds(fastestRun.median_query_duration_s)}`
              : fastestRun
                ? `${fastestRun.db_version} · Median query unavailable`
                : undefined
          }
        />
        <StatCard
          label="Widest Spread"
          value={widestSpreadRow ? `${widestSpreadRow.spread_ratio.toFixed(2)}x` : "—"}
          detail={
            widestSpreadRow
              ? `${widestSpreadRow.query_label} · ${widestSpreadRow.category}`
              : undefined
          }
        />
      </div>

      <div className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[minmax(0,1.25fr)_minmax(24rem,0.95fr)]">
        <section className="flex min-h-0 flex-col rounded-3xl border border-slate-800 bg-slate-900/70">
          <div className="flex shrink-0 items-start justify-between gap-4 border-b border-slate-800 px-5 py-4">
            <div>
              <h3 className="text-lg font-semibold text-slate-50">Query latency comparison</h3>
              <p className="mt-1 text-sm text-slate-400">
                Click a row to inspect its latency spread and SQL. Scrolling stays inside the table.
              </p>
            </div>
            <DatabaseLegend databases={includedDatabases} databaseColors={databaseColors} />
          </div>

          <div className="min-h-0 flex-1 p-5 pt-4">
            <QueryComparisonTable
              rows={queryRows}
              databases={includedDatabases}
              databaseColors={databaseColors}
              selection={selection}
              maxDuration={globalMaxDuration}
              containerClassName="h-full min-h-0"
            />
          </div>
        </section>

        <section className="min-h-0">
          {selectedRow ? (
            <QueryDetailPanel
              row={selectedRow}
              databases={includedDatabases}
              databaseColors={databaseColors}
              sql={selectedSql}
              onClose={() => selection.setSelectedQuery(null)}
            />
          ) : (
            <div className="flex h-full min-h-0 flex-col justify-between rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
              <div>
                <p className="text-sm font-medium tracking-[0.18em] text-cyan-300 uppercase">
                  Inspector
                </p>
                <h3 className="mt-3 text-2xl font-semibold text-slate-50">Pick a query row</h3>
                <p className="mt-3 max-w-md text-sm leading-6 text-slate-400">
                  The detail pane stays pinned on the right. Select any query to inspect latency by
                  database and compare the SQL variants for only the databases currently included.
                </p>
              </div>

              <div className="grid gap-3">
                <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3">
                  <p className="text-xs font-medium tracking-[0.18em] text-slate-500 uppercase">
                    Rows available
                  </p>
                  <p className="mt-2 text-lg font-semibold text-slate-100">{queryRows.length}</p>
                </div>
                <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3">
                  <p className="text-xs font-medium tracking-[0.18em] text-slate-500 uppercase">
                    Active databases
                  </p>
                  <p className="mt-2 text-lg font-semibold text-slate-100">
                    {includedDatabases.length}
                  </p>
                </div>
              </div>
            </div>
          )}
        </section>
      </div>
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

function buildDatabaseAggregateStats(
  rows: QueryComparisonRow[],
  databases: string[],
): DatabaseAggregateStats[] {
  return databases
    .map((db) => {
      let wins = 0
      const ratios: number[] = []

      for (const row of rows) {
        const durations = Object.values(row.by_database).filter(
          (value): value is number => value !== null,
        )
        const duration = row.by_database[db]
        if (durations.length === 0 || duration === null || duration === undefined) continue

        const fastest = Math.min(...durations)
        if (duration === fastest) {
          wins += 1
        }

        ratios.push(fastest / duration)
      }

      const score =
        ratios.length === 0
          ? null
          : Math.exp(ratios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / ratios.length) * 100

      return { db, score, wins }
    })
    .sort((left, right) => {
      const scoreDelta = (right.score ?? -1) - (left.score ?? -1)
      if (scoreDelta !== 0) return scoreDelta
      return right.wins - left.wins || left.db.localeCompare(right.db)
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
