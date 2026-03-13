import { startTransition, useEffect, useMemo, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Rectangle,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { DatabaseLegend } from "../components/DatabaseLegend"
import { DurationScaleToggle } from "../components/DurationScaleToggle"
import { DatabaseMultiSelect } from "../components/filters/DatabaseMultiSelect"
import { ChartFrame, PanelCard, PanelHeader } from "../components/layout/Panel"
import { MetricsTimeSeriesPanel } from "../components/MetricsTimeSeriesPanel"
import { QueryComparisonTable, type QueryComparisonRow } from "../components/QueryComparisonTable"
import { QueryDetailPanel } from "../components/QueryDetailPanel"
import { Skeleton } from "../components/Skeleton"
import { useSelectionState } from "../hooks/useSelectionState"
import { getDatabaseColors } from "../lib/databaseColors"
import {
  formatDurationAxisTick,
  formatDurationSeconds,
  getDurationAxisDomain,
  getDurationAxisTicks,
  scaleDurationForChart,
  type DurationScaleMode,
} from "../lib/format"
import {
  fetchTimeSeriesMetricSamples,
  fetchTimeSeriesOperationSummaries,
  fetchQueriesManifest,
  fetchTimeSeriesQuerySummaries,
  fetchTimeSeriesRunSummaries,
} from "../lib/queries"
import type {
  QueriesManifest,
  TimeSeriesMetricSample,
  TimeSeriesOperation,
  TimeSeriesOperationSummary,
  TimeSeriesQuerySummary,
  TimeSeriesRunSummary,
} from "../lib/types"
import {
  TIME_SERIES_BOTTOM_GRID_CLASS,
  TIME_SERIES_DETAIL_SECTION_CLASS,
  TIME_SERIES_OVERVIEW_CHART_HEIGHT,
  TIME_SERIES_OVERVIEW_HEADER_CLASS,
  TIME_SERIES_QUERY_SECTION_CLASS,
  TIME_SERIES_QUERY_TABLE_CONTAINER_CLASS,
  TIME_SERIES_QUERY_TABLE_WRAPPER_CLASS,
  TIME_SERIES_TOP_CARD_MIN_HEIGHT_CLASS,
  TIME_SERIES_TOP_GRID_CLASS,
} from "./timeSeriesLayout"

interface TimeSeriesPageProps {
  system: string | null
  isSystemLoading?: boolean
}

interface TimeSeriesPageState {
  loading: boolean
  error: string | null
  runSummaries: TimeSeriesRunSummary[]
  operationSummaries: TimeSeriesOperationSummary[]
  metricSamples: TimeSeriesMetricSample[]
  querySummaries: TimeSeriesQuerySummary[]
  queriesManifest: QueriesManifest | null
}

interface OverviewChartRow {
  db: string
  db_version: string
  fill?: string
  populate_duration_s: number
  run_duration_s: number
  total_duration_s: number
  populate_chart_duration_s: number
  run_chart_duration_s: number
}

type OverviewOperationVisibility = Record<TimeSeriesOperation, boolean>

const LOG_FLOOR = 1e-6

function createInitialTimeSeriesPageState(): TimeSeriesPageState {
  return {
    loading: true,
    error: null,
    runSummaries: [],
    operationSummaries: [],
    metricSamples: [],
    querySummaries: [],
    queriesManifest: null,
  }
}

export function TimeSeriesPage({ system, isSystemLoading = false }: TimeSeriesPageProps) {
  const [state, setState] = useState<TimeSeriesPageState>({
    ...createInitialTimeSeriesPageState(),
  })
  const [selectedDatabases, setSelectedDatabases] = useState<string[]>([])
  const [overviewScaleMode, setOverviewScaleMode] = useState<DurationScaleMode>("log")
  const [queryTableScaleMode, setQueryTableScaleMode] = useState<DurationScaleMode>("log")
  const [overviewOperationVisibility, setOverviewOperationVisibility] =
    useState<OverviewOperationVisibility>({
      populate: true,
      run: true,
    })
  const selection = useSelectionState()
  const { selectedQuery, setSelectedQuery } = selection

  useEffect(() => {
    if (isSystemLoading || system === null) {
      startTransition(() => {
        setState(createInitialTimeSeriesPageState())
      })
      return
    }

    let cancelled = false

    setState(createInitialTimeSeriesPageState())

    Promise.all([
      fetchTimeSeriesRunSummaries(system),
      fetchTimeSeriesOperationSummaries(system),
      fetchTimeSeriesMetricSamples(system),
      fetchTimeSeriesQuerySummaries(system),
      fetchQueriesManifest().catch(() => null),
    ])
      .then(
        ([runSummaries, operationSummaries, metricSamples, querySummaries, queriesManifest]) => {
          if (cancelled) return

          startTransition(() => {
            setState({
              loading: false,
              error: null,
              runSummaries,
              operationSummaries,
              metricSamples,
              querySummaries,
              queriesManifest,
            })
          })
        },
      )
      .catch((nextError) => {
        if (cancelled) return

        startTransition(() => {
          setState({
            loading: false,
            error: String(nextError),
            runSummaries: [],
            operationSummaries: [],
            metricSamples: [],
            querySummaries: [],
            queriesManifest: null,
          })
        })
      })

    return () => {
      cancelled = true
    }
  }, [isSystemLoading, system])

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
  const filteredQuerySummaries = state.querySummaries.filter((row) =>
    includedDatabaseSet.has(row.db),
  )
  const filteredOperationSummaries = state.operationSummaries.filter((run) =>
    includedDatabaseSet.has(run.db),
  )

  const databaseColors = getDatabaseColors(databases)

  const queryRows = buildQueryComparisonRows(filteredQuerySummaries, includedDatabases)

  const globalMaxDuration = queryRows.reduce((max, row) => {
    for (const val of Object.values(row.by_database)) {
      if (val !== null && val > max) {
        max = val
      }
    }
    return max
  }, LOG_FLOOR)

  const runChartData = buildOverviewChartData(
    filteredOperationSummaries,
    overviewScaleMode,
    overviewOperationVisibility,
  ).map((entry) => ({
    ...entry,
    fill: databaseColors[entry.db] ?? "#94a3b8",
  }))
  const overviewMaxDuration = Math.max(0, ...runChartData.map((run) => run.total_duration_s))
  const overviewAxisDomain = getDurationAxisDomain(overviewMaxDuration, overviewScaleMode)
  const overviewAxisTicks = getDurationAxisTicks(overviewMaxDuration, overviewScaleMode)
  const hasVisibleOverviewSegments = runChartData.some((entry) => entry.total_duration_s > 0)

  const selectedRow = selectedQuery
    ? (queryRows.find((r) => r.query_name === selectedQuery) ?? null)
    : null

  const selectedSql = state.queriesManifest?.time_series?.[selectedQuery ?? ""] ?? null
  const isLoading = isSystemLoading || state.loading

  function toggleDatabase(database: string) {
    setSelectedDatabases((currentSelection) => {
      const nextSelection = currentSelection.includes(database)
        ? currentSelection.filter((value) => value !== database)
        : [...currentSelection, database].sort()

      return nextSelection.length > 0 ? nextSelection : currentSelection
    })
  }

  function toggleOverviewOperation(operation: TimeSeriesOperation) {
    setOverviewOperationVisibility((currentVisibility) => ({
      ...currentVisibility,
      [operation]: !currentVisibility[operation],
    }))
  }

  useEffect(() => {
    if (selectedQuery === null) return

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSelectedQuery(null)
      }
    }

    window.addEventListener("keydown", handleKeyDown)

    return () => {
      window.removeEventListener("keydown", handleKeyDown)
    }
  }, [selectedQuery, setSelectedQuery])

  if (!isLoading && state.error) {
    return (
      <section className="flex h-full min-h-0 w-full flex-1 items-center justify-center">
        <div className="rounded-3xl border border-red-500/30 bg-red-950/20 px-6 py-5 text-sm text-red-300">
          Failed to load time-series data: {state.error}
        </div>
      </section>
    )
  }

  if (!isLoading && state.runSummaries.length === 0) {
    return (
      <section className="flex h-full min-h-0 w-full flex-1 items-center justify-center">
        <div className="rounded-3xl border border-slate-800 bg-slate-900/70 px-6 py-5 text-sm text-slate-400">
          No completed time-series runs were found for {system ?? "the selected system"}.
        </div>
      </section>
    )
  }

  return (
    <section className="flex min-h-full w-full flex-col gap-4 pb-4">
      <div className={TIME_SERIES_TOP_GRID_CLASS}>
        <PanelCard className={TIME_SERIES_TOP_CARD_MIN_HEIGHT_CLASS}>
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
            {isLoading ? (
              <FilterChipsSkeleton />
            ) : (
              <DatabaseMultiSelect
                databases={databases}
                selectedDatabases={includedDatabases}
                onSelectAll={() => setSelectedDatabases(databases)}
                onToggleDatabase={toggleDatabase}
              />
            )}
          </div>
        </PanelCard>

        <PanelCard className={TIME_SERIES_TOP_CARD_MIN_HEIGHT_CLASS}>
          <PanelHeader className={TIME_SERIES_OVERVIEW_HEADER_CLASS}>
            <div>
              <h3 className="text-lg font-semibold text-slate-50">Aggregate overview</h3>
              <p className="mt-1 text-sm text-slate-400">
                Latest completed populate and run durations per database. Toggle either phase on or
                off, then switch between a zero-based log view and linear scale before diving into
                per-query detail. Each database keeps one color family: muted for populate, stronger
                for run.
              </p>
            </div>
            {isLoading ? (
              <OverviewControlsSkeleton />
            ) : (
              <div className="flex flex-wrap items-center justify-end gap-2">
                <div className="inline-flex rounded-full border border-slate-800 bg-slate-950/80 p-1">
                  {(
                    [
                      ["populate", "Populate", "rgba(148, 163, 184, 0.45)"],
                      ["run", "Run", "rgba(148, 163, 184, 1)"],
                    ] as const
                  ).map(([operation, label, chipColor]) => {
                    const isActive = overviewOperationVisibility[operation]

                    return (
                      <button
                        key={operation}
                        type="button"
                        aria-pressed={isActive}
                        onClick={() => toggleOverviewOperation(operation)}
                        className={
                          isActive
                            ? "inline-flex items-center gap-2 rounded-full bg-cyan-400/10 px-3 py-1 text-xs font-medium text-cyan-200 shadow-[inset_0_0_0_1px_rgba(34,211,238,0.5)]"
                            : "inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
                        }
                      >
                        <span
                          className="size-2 rounded-full"
                          style={{
                            backgroundColor: isActive ? chipColor : "rgba(71, 85, 105, 0.9)",
                          }}
                        />
                        {label}
                      </button>
                    )
                  })}
                </div>
                <DurationScaleToggle mode={overviewScaleMode} onChange={setOverviewScaleMode} />
                <div className="rounded-full border border-slate-800 bg-slate-950/80 px-3 py-1 text-xs font-medium whitespace-nowrap text-slate-400">
                  {includedDatabases.length} of {databases.length} databases
                </div>
              </div>
            )}
          </PanelHeader>

          <ChartFrame className="mt-4" height={TIME_SERIES_OVERVIEW_CHART_HEIGHT}>
            {isLoading ? (
              <OverviewChartSkeleton />
            ) : !hasVisibleOverviewSegments ? (
              <div className="flex h-full min-h-28 items-center justify-center rounded-2xl border border-dashed border-slate-800 bg-slate-950/30 px-6 text-center text-sm text-slate-500">
                Enable populate or run to display overview bars for the selected databases.
              </div>
            ) : (
              <ResponsiveContainer
                width="100%"
                height="100%"
                initialDimension={{ width: 640, height: TIME_SERIES_OVERVIEW_CHART_HEIGHT }}
              >
                <BarChart data={runChartData} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke="#1e293b" vertical={false} />
                  <XAxis
                    dataKey="db"
                    tick={{ fill: "#94a3b8", fontSize: 11 }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                  />
                  <YAxis
                    domain={overviewAxisDomain}
                    ticks={overviewAxisTicks}
                    tick={{ fill: "#94a3b8", fontSize: 11 }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                    tickFormatter={(value: number) =>
                      formatDurationAxisTick(value, overviewScaleMode)
                    }
                  />
                  <Tooltip
                    cursor={{ fill: "rgba(15, 23, 42, 0.55)" }}
                    contentStyle={{
                      backgroundColor: "#020617",
                      border: "1px solid #334155",
                      borderRadius: 16,
                      color: "#e2e8f0",
                    }}
                    labelStyle={{ color: "#e2e8f0" }}
                    itemStyle={{ color: "#e2e8f0" }}
                    formatter={(_value, name, item) => {
                      const row = item.payload as OverviewChartRow

                      return [
                        formatDurationSeconds(
                          name === "Populate" ? row.populate_duration_s : row.run_duration_s,
                        ),
                        name ?? "",
                      ] as const
                    }}
                    labelFormatter={(label, payload) => {
                      const row = payload?.[0]?.payload as OverviewChartRow | undefined
                      if (!row) return label

                      const labelText =
                        typeof label === "string" || typeof label === "number" ? String(label) : ""

                      return overviewOperationVisibility.populate && overviewOperationVisibility.run
                        ? `${labelText} · total ${formatDurationSeconds(row.total_duration_s)}`
                        : `${labelText} · ${formatDurationSeconds(row.total_duration_s)}`
                    }}
                  />
                  <Bar
                    dataKey="populate_chart_duration_s"
                    stackId="total"
                    hide={!overviewOperationVisibility.populate}
                    radius={overviewOperationVisibility.run ? [0, 0, 10, 10] : [10, 10, 10, 10]}
                    name="Populate"
                    shape={(props) => (
                      <Rectangle
                        {...props}
                        fill={withAlpha(
                          (props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8",
                          0.45,
                        )}
                      />
                    )}
                  />
                  <Bar
                    dataKey="run_chart_duration_s"
                    stackId="total"
                    hide={!overviewOperationVisibility.run}
                    radius={
                      overviewOperationVisibility.populate ? [10, 10, 0, 0] : [10, 10, 10, 10]
                    }
                    name="Run"
                    shape={(props) => (
                      <Rectangle
                        {...props}
                        fill={(props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8"}
                      />
                    )}
                  />
                </BarChart>
              </ResponsiveContainer>
            )}
          </ChartFrame>
        </PanelCard>
      </div>

      <MetricsTimeSeriesPanel
        samples={state.metricSamples}
        databases={includedDatabases}
        databaseColors={databaseColors}
        loading={isLoading}
      />

      <div className={TIME_SERIES_BOTTOM_GRID_CLASS}>
        <section className={TIME_SERIES_QUERY_SECTION_CLASS}>
          <div className="flex shrink-0 items-start justify-between gap-4 border-b border-slate-800 px-5 py-4">
            <div>
              <h3 className="text-lg font-semibold text-slate-50">Query latency comparison</h3>
              <p className="mt-1 text-sm text-slate-400">
                Click a row to inspect its latency spread and SQL.
              </p>
            </div>
            {isLoading ? (
              <LegendSkeleton />
            ) : (
              <DatabaseLegend databases={includedDatabases} databaseColors={databaseColors} />
            )}
          </div>

          <div className={TIME_SERIES_QUERY_TABLE_WRAPPER_CLASS}>
            {isLoading ? (
              <QueryTableSkeleton />
            ) : (
              <QueryComparisonTable
                rows={queryRows}
                databases={includedDatabases}
                databaseColors={databaseColors}
                selection={selection}
                maxDuration={globalMaxDuration}
                scaleMode={queryTableScaleMode}
                onScaleModeChange={setQueryTableScaleMode}
                containerClassName={TIME_SERIES_QUERY_TABLE_CONTAINER_CLASS}
              />
            )}
          </div>
        </section>

        <section className={TIME_SERIES_DETAIL_SECTION_CLASS}>
          {isLoading ? (
            <InspectorSkeleton />
          ) : selectedRow ? (
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
    .sort(([leftName], [rightName]) => compareTimeSeriesQueryNames(leftName, rightName))
    .map(([queryName, rows]) => {
      const { queryId, queryLabel, tableFamily } = parseTimeSeriesQueryName(queryName)
      const byDatabase = Object.fromEntries(
        databases.map((database) => [database, null]),
      ) as Record<string, number | null>
      const statsByDatabase = Object.fromEntries(
        databases.map((database) => [database, null]),
      ) as QueryComparisonRow["stats_by_database"]

      for (const row of rows) {
        byDatabase[row.db] = row.median_duration_s
        statsByDatabase[row.db] = {
          median_duration_s: row.median_duration_s,
          avg_duration_s: row.avg_duration_s,
          min_duration_s: row.min_duration_s,
          max_duration_s: row.max_duration_s,
          iterations: row.iterations,
        }
      }

      const numericDurations = Object.values(byDatabase).filter((value) => value !== null)
      const fastestDuration = Math.min(...numericDurations)
      const slowestDuration = Math.max(...numericDurations)
      const fastestDb = rows.find((row) => row.median_duration_s === fastestDuration)?.db ?? "—"

      return {
        query_name: queryName,
        query_label: queryLabel,
        table_family: tableFamily,
        query_id: queryId,
        fastest_db: fastestDb,
        spread_ratio: slowestDuration / fastestDuration,
        by_database: byDatabase,
        stats_by_database: statsByDatabase,
      }
    })
}

function FilterChipsSkeleton() {
  return (
    <div className="flex flex-wrap gap-2">
      <Skeleton className="h-11 w-28 rounded-full" />
      <Skeleton className="h-11 w-32 rounded-full" />
      <Skeleton className="h-11 w-24 rounded-full" />
      <Skeleton className="h-11 w-32 rounded-full" />
    </div>
  )
}

function OverviewControlsSkeleton() {
  return (
    <div className="flex flex-wrap items-center justify-end gap-2">
      <Skeleton className="h-8 w-24 rounded-full" />
      <Skeleton className="h-8 w-20 rounded-full" />
      <Skeleton className="h-8 w-28 rounded-full" />
    </div>
  )
}

function OverviewChartSkeleton() {
  return (
    <div className="grid h-full grid-cols-[4rem_minmax(0,1fr)] gap-4">
      <div className="flex flex-col justify-around py-3">
        <Skeleton className="h-3 w-10 rounded-full" />
        <Skeleton className="h-3 w-9 rounded-full" />
        <Skeleton className="h-3 w-11 rounded-full" />
        <Skeleton className="h-3 w-8 rounded-full" />
      </div>
      <div className="relative min-h-0 rounded-xl">
        <div className="absolute inset-x-0 bottom-0 border-t border-slate-800/80" />
        <div className="absolute inset-y-0 left-0 border-l border-slate-800/80" />
        <div className="absolute inset-x-0 top-[20%] border-t border-slate-800/40" />
        <div className="absolute inset-x-0 top-[45%] border-t border-slate-800/40" />
        <div className="absolute inset-x-0 top-[70%] border-t border-slate-800/40" />
        <div className="absolute inset-0 flex items-end gap-4 px-4 pt-4 pb-6">
          <Skeleton className="h-[72%] flex-1 rounded-xl" />
          <Skeleton className="h-[48%] flex-1 rounded-xl" />
          <Skeleton className="h-[28%] flex-1 rounded-xl" />
          <Skeleton className="h-[62%] flex-1 rounded-xl" />
        </div>
      </div>
    </div>
  )
}

function LegendSkeleton() {
  return (
    <div className="flex gap-2">
      <Skeleton className="h-6 w-16 rounded-full" />
      <Skeleton className="h-6 w-20 rounded-full" />
      <Skeleton className="h-6 w-20 rounded-full" />
    </div>
  )
}

function QueryTableSkeleton() {
  return (
    <div
      className={`flex min-h-0 flex-col overflow-hidden rounded-2xl border border-slate-800/40 bg-slate-950/35 ${TIME_SERIES_QUERY_TABLE_CONTAINER_CLASS}`}
    >
      <div className="grid shrink-0 grid-cols-[32%_34%_12%_10%_12%] gap-0 border-b border-slate-800/40 bg-slate-900/80 px-4 py-3">
        <Skeleton className="h-4 w-20" />
        <Skeleton className="h-4 w-28" />
        <Skeleton className="h-4 w-12" />
        <Skeleton className="h-4 w-12" />
        <Skeleton className="h-4 w-20" />
      </div>
      <div className="space-y-3 p-4">
        <Skeleton className="h-16 w-full rounded-2xl" />
        <Skeleton className="h-16 w-full rounded-2xl" />
        <Skeleton className="h-16 w-full rounded-2xl" />
        <Skeleton className="h-16 w-full rounded-2xl" />
        <Skeleton className="h-16 w-full rounded-2xl" />
      </div>
    </div>
  )
}

function InspectorSkeleton() {
  return (
    <div className="flex h-full min-h-0 flex-col justify-between rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
      <div>
        <p className="text-sm font-medium tracking-[0.18em] text-cyan-300 uppercase">Inspector</p>
        <h3 className="mt-3 text-2xl font-semibold text-slate-50">Pick a query row</h3>
        <p className="mt-3 max-w-md text-sm leading-6 text-slate-400">
          The detail pane stays pinned on the right. Select any query to inspect latency by database
          and compare the SQL variants for only the databases currently included.
        </p>
      </div>

      <div className="grid gap-3">
        <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3">
          <p className="text-xs font-medium tracking-[0.18em] text-slate-500 uppercase">
            Rows available
          </p>
          <Skeleton className="mt-2 h-7 w-16" />
        </div>
        <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3">
          <p className="text-xs font-medium tracking-[0.18em] text-slate-500 uppercase">
            Active databases
          </p>
          <Skeleton className="mt-2 h-7 w-14" />
        </div>
      </div>
    </div>
  )
}

function buildOverviewChartData(
  operationSummaries: TimeSeriesOperationSummary[],
  scaleMode: DurationScaleMode,
  visibleOperations: OverviewOperationVisibility,
): OverviewChartRow[] {
  const summariesByDatabase = new Map<
    string,
    {
      db: string
      db_version: string
      populate_duration_s: number
      run_duration_s: number
    }
  >()

  for (const summary of operationSummaries) {
    const existing = summariesByDatabase.get(summary.db) ?? {
      db: summary.db,
      db_version: summary.db_version,
      populate_duration_s: 0,
      run_duration_s: 0,
    }

    existing.db_version = summary.db_version
    if (summary.operation === "populate") {
      existing.populate_duration_s = summary.run_duration_s
    } else {
      existing.run_duration_s = summary.run_duration_s
    }

    summariesByDatabase.set(summary.db, existing)
  }

  return Array.from(summariesByDatabase.values())
    .map((entry) => {
      const visiblePopulateDuration = visibleOperations.populate ? entry.populate_duration_s : 0
      const visibleRunDuration = visibleOperations.run ? entry.run_duration_s : 0
      const totalDuration = visiblePopulateDuration + visibleRunDuration
      const populateTop = scaleDurationForChart(visiblePopulateDuration, scaleMode)
      const totalTop = scaleDurationForChart(totalDuration, scaleMode)

      return {
        db: entry.db,
        db_version: entry.db_version,
        populate_duration_s: entry.populate_duration_s,
        run_duration_s: entry.run_duration_s,
        total_duration_s: totalDuration,
        populate_chart_duration_s: populateTop,
        run_chart_duration_s: Math.max(0, totalTop - populateTop),
      }
    })
    .sort(
      (left, right) =>
        left.total_duration_s - right.total_duration_s || left.db.localeCompare(right.db),
    )
}

function withAlpha(hexColor: string, alpha: number): string {
  const normalized = hexColor.replace("#", "")
  if (normalized.length !== 6) return hexColor

  const red = Number.parseInt(normalized.slice(0, 2), 16)
  const green = Number.parseInt(normalized.slice(2, 4), 16)
  const blue = Number.parseInt(normalized.slice(4, 6), 16)

  return `rgba(${red}, ${green}, ${blue}, ${alpha})`
}

function parseTimeSeriesQueryName(queryName: string): {
  queryId: string
  queryLabel: string
  tableFamily: string
} {
  const match = /^(?<table>[a-z]+)_(?<queryId>\d+)_(?<description>.+)$/.exec(queryName)
  if (!match?.groups) {
    return {
      queryId: "00",
      queryLabel: toTitleCase(queryName.replace(/_/g, " ")),
      tableFamily: "Unknown",
    }
  }

  const queryId = match.groups.queryId ?? "00"
  const description = match.groups.description ?? queryName
  const table = match.groups.table ?? "unknown"

  return {
    queryId,
    queryLabel: toTitleCase(description.replace(/_/g, " ")),
    tableFamily: formatTimeSeriesTableFamily(table),
  }
}

function formatTimeSeriesTableFamily(value: string): string {
  if (value === "eav") return "EAV"
  if (value === "wide") return "Wide"
  if (value === "tall") return "Tall"
  if (value === "large") return "Large"
  return toTitleCase(value)
}

function toTitleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function compareTimeSeriesQueryNames(left: string, right: string): number {
  const leftMeta = parseTimeSeriesQueryName(left)
  const rightMeta = parseTimeSeriesQueryName(right)
  const tableDelta = leftMeta.tableFamily.localeCompare(rightMeta.tableFamily)
  if (tableDelta !== 0) return tableDelta

  const queryIdDelta =
    Number.parseInt(leftMeta.queryId, 10) - Number.parseInt(rightMeta.queryId, 10)
  if (queryIdDelta !== 0) return queryIdDelta

  const labelDelta = leftMeta.queryLabel.localeCompare(rightMeta.queryLabel)
  if (labelDelta !== 0) return labelDelta

  return left.localeCompare(right)
}
