import { startTransition, useEffect, useEffectEvent, useState } from "react"
import { createColumnHelper } from "@tanstack/react-table"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import { QueryTable } from "../components/QueryTable"
import { StatCard } from "../components/StatCard"
import { formatDurationSeconds, formatMultiplier } from "../lib/format"
import {
  fetchTimeSeriesQuerySummaries,
  fetchTimeSeriesRunSummaries,
} from "../lib/queries"
import type { TimeSeriesQuerySummary, TimeSeriesRunSummary } from "../lib/types"

interface TimeSeriesPageProps {
  system: string
}

interface TimeSeriesQueryComparisonRow {
  query_name: string
  query_label: string
  category: string
  scale: string
  fastest_db: string
  spread_ratio: number
  by_database: Record<string, number | null>
}

interface TimeSeriesChartRow {
  queryLabel: string
  [database: string]: number | string | null
}

interface TimeSeriesPageState {
  loading: boolean
  error: string | null
  runSummaries: TimeSeriesRunSummary[]
  querySummaries: TimeSeriesQuerySummary[]
}

const runColumnHelper = createColumnHelper<TimeSeriesRunSummary>()
const queryColumnHelper = createColumnHelper<TimeSeriesQueryComparisonRow>()

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

const DATABASE_COLORS = [
  "#38bdf8",
  "#f97316",
  "#34d399",
  "#facc15",
  "#f472b6",
  "#a78bfa",
]

export function TimeSeriesPage({ system }: TimeSeriesPageProps) {
  const [state, setState] = useState<TimeSeriesPageState>({
    loading: true,
    error: null,
    runSummaries: [],
    querySummaries: [],
  })

  const applyLoadedData = useEffectEvent(
    (
      runSummaries: TimeSeriesRunSummary[],
      querySummaries: TimeSeriesQuerySummary[],
    ) => {
      startTransition(() => {
        setState({
          loading: false,
          error: null,
          runSummaries,
          querySummaries,
        })
      })
    },
  )

  const applyLoadError = useEffectEvent((nextError: unknown) => {
    startTransition(() => {
      setState({
        loading: false,
        error: String(nextError),
        runSummaries: [],
        querySummaries: [],
      })
    })
  })

  useEffect(() => {
    let cancelled = false

    setState({
      loading: true,
      error: null,
      runSummaries: [],
      querySummaries: [],
    })

    Promise.all([
      fetchTimeSeriesRunSummaries(system),
      fetchTimeSeriesQuerySummaries(system),
    ])
      .then(([runSummaries, querySummaries]) => {
        if (cancelled) return
        applyLoadedData(runSummaries, querySummaries)
      })
      .catch((nextError) => {
        if (cancelled) return
        applyLoadError(nextError)
      })

    return () => {
      cancelled = true
    }
  }, [applyLoadedData, applyLoadError, system])

  const databases = Array.from(
    new Set(state.runSummaries.map((run) => run.db)),
  ).sort()
  const queryRows = buildTimeSeriesQueryRows(state.querySummaries, databases)
  const queryChartRows = buildTimeSeriesChartRows(queryRows, databases)
  const fastestRun = state.runSummaries[0] ?? null
  const slowestQuery =
    state.querySummaries.reduce<TimeSeriesQuerySummary | null>(
      (currentSlowest, querySummary) => {
        if (
          !currentSlowest ||
          querySummary.median_duration_s > currentSlowest.median_duration_s
        ) {
          return querySummary
        }
        return currentSlowest
      },
      null,
    )
  const queryCount = new Set(state.querySummaries.map((row) => row.query_name))
    .size

  const runChartData = state.runSummaries.map((run) => ({
    db: run.db,
    duration_s: run.run_duration_s,
  }))

  const queryColumns = [
    queryColumnHelper.accessor("query_label", {
      header: "Query",
    }),
    queryColumnHelper.accessor("category", {
      header: "Category",
    }),
    queryColumnHelper.accessor("scale", {
      header: "Scale",
    }),
    queryColumnHelper.accessor("fastest_db", {
      header: "Fastest DB",
    }),
    ...databases.map((database) =>
      queryColumnHelper.accessor((row) => row.by_database[database], {
        id: `${database}-median`,
        header: `${database} Median`,
        cell: (info) => {
          const value = info.getValue()
          return value === null || value === undefined
            ? "—"
            : formatDurationSeconds(value)
        },
      }),
    ),
    queryColumnHelper.accessor("spread_ratio", {
      header: "Spread",
      cell: (info) => formatMultiplier(info.getValue()),
    }),
  ]

  return (
    <section className="space-y-10">
      <header className="max-w-4xl space-y-4">
        <p className="text-sm font-medium uppercase tracking-[0.18em] text-cyan-300">
          Time Series
        </p>
        <h2 className="text-4xl font-semibold tracking-tight text-slate-50">
          Query-pattern latency should be the primary view
        </h2>
        <p className="text-lg leading-8 text-slate-300">
          The time-series suite mixes boundary lookups, aggregates, raw scans,
          resamples, scalar lookups, and export. The clearest visualization is a
          query-by-query latency comparison on one system, with total benchmark
          run duration kept as supporting context.
        </p>
      </header>

      {state.loading ? (
        <p className="text-sm text-slate-400">
          Loading completed time-series runs for {system}...
        </p>
      ) : null}

      {state.error ? (
        <p className="text-sm text-red-300">
          Failed to load time-series data: {state.error}
        </p>
      ) : null}

      {!state.loading && !state.error && state.runSummaries.length === 0 ? (
        <p className="text-sm text-slate-400">
          No completed time-series runs were found for {system}.
        </p>
      ) : null}

      {!state.loading && !state.error && state.runSummaries.length > 0 ? (
        <>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <StatCard
              label="System"
              value={system}
              detail="Global app scope. Cross-system comparisons stay out of the UI."
            />
            <StatCard
              label="Completed Runs"
              value={String(state.runSummaries.length)}
              detail={`${databases.length} databases across ${queryCount} benchmark queries.`}
            />
            <StatCard
              label="Fastest Full Run"
              value={
                fastestRun
                  ? `${fastestRun.db} · ${formatDurationSeconds(
                      fastestRun.run_duration_s,
                    )}`
                  : "—"
              }
              detail={
                fastestRun
                  ? fastestRun.median_query_duration_s === null
                    ? "Median query unavailable"
                    : `Median query ${formatDurationSeconds(
                        fastestRun.median_query_duration_s,
                      )}`
                  : undefined
              }
            />
            <StatCard
              label="Slowest Query Median"
              value={
                slowestQuery
                  ? `${formatDurationSeconds(slowestQuery.median_duration_s)}`
                  : "—"
              }
              detail={
                slowestQuery
                  ? `${formatTimeSeriesQueryName(slowestQuery.query_name).queryLabel} on ${slowestQuery.db}`
                  : undefined
              }
            />
          </div>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">
                Full benchmark run duration
              </h3>
              <p className="text-sm text-slate-400">
                Keep full-run time visible, but treat it as a secondary summary.
                The per-query view below explains where the differences come
                from.
              </p>
            </div>

            <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
              <ResponsiveContainer width="100%" height={280}>
                <BarChart
                  data={runChartData}
                  margin={{ top: 16, right: 16, bottom: 16, left: 16 }}
                >
                  <CartesianGrid stroke="#1e293b" vertical={false} />
                  <XAxis
                    dataKey="db"
                    tick={{ fill: "#94a3b8" }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                  />
                  <YAxis
                    tick={{ fill: "#94a3b8" }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                    tickFormatter={(value) =>
                      formatDurationSeconds(Number(value))
                    }
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
                  <Bar
                    dataKey="duration_s"
                    fill="#38bdf8"
                    radius={[10, 10, 0, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">
                Median query latency by workload
              </h3>
              <p className="text-sm text-slate-400">
                This is the recommended primary visualization for the
                time-series benchmark. It preserves workload identity and
                exposes scaling behavior between the small and large query
                variants.
              </p>
            </div>

            <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
              <ResponsiveContainer
                width="100%"
                height={Math.max(560, queryChartRows.length * 34)}
              >
                <BarChart
                  data={queryChartRows}
                  layout="vertical"
                  margin={{ top: 16, right: 20, bottom: 16, left: 16 }}
                >
                  <CartesianGrid stroke="#1e293b" horizontal={false} />
                  <XAxis
                    type="number"
                    tick={{ fill: "#94a3b8" }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                    tickFormatter={(value) =>
                      formatDurationSeconds(Number(value))
                    }
                  />
                  <YAxis
                    type="category"
                    dataKey="queryLabel"
                    width={260}
                    tick={{ fill: "#cbd5e1", fontSize: 12 }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={{ stroke: "#334155" }}
                  />
                  <Tooltip
                    cursor={{ fill: "rgba(15, 23, 42, 0.45)" }}
                    contentStyle={{
                      backgroundColor: "#020617",
                      border: "1px solid #334155",
                      borderRadius: 16,
                    }}
                    formatter={(value: number) => formatDurationSeconds(value)}
                  />
                  <Legend />
                  {databases.map((database, index) => (
                    <Bar
                      key={database}
                      dataKey={database}
                      fill={DATABASE_COLORS[index % DATABASE_COLORS.length]}
                      radius={[0, 6, 6, 0]}
                    />
                  ))}
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">
                Query comparison table
              </h3>
              <p className="text-sm text-slate-400">
                Use this table to scan categories, scale variants, the fastest
                engine per query, and the spread between databases.
              </p>
            </div>
            <QueryTable data={queryRows} columns={queryColumns} />
          </section>

          <section className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-slate-50">
                Completed run details
              </h3>
              <p className="text-sm text-slate-400">
                Full run metadata remains available, but only for completed
                runs.
              </p>
            </div>
            <QueryTable data={state.runSummaries} columns={RUN_COLUMNS} />
          </section>
        </>
      ) : null}
    </section>
  )
}

function buildTimeSeriesQueryRows(
  querySummaries: TimeSeriesQuerySummary[],
  databases: string[],
): TimeSeriesQueryComparisonRow[] {
  const groupedQueries = new Map<string, TimeSeriesQuerySummary[]>()

  for (const querySummary of querySummaries) {
    const existingRows = groupedQueries.get(querySummary.query_name) ?? []
    existingRows.push(querySummary)
    groupedQueries.set(querySummary.query_name, existingRows)
  }

  return Array.from(groupedQueries.entries())
    .sort(([leftName], [rightName]) => leftName.localeCompare(rightName))
    .map(([queryName, rows]) => {
      const { category, queryLabel, scale } =
        formatTimeSeriesQueryName(queryName)
      const byDatabase = Object.fromEntries(
        databases.map((database) => [database, null]),
      ) as Record<string, number | null>

      for (const row of rows) {
        byDatabase[row.db] = row.median_duration_s
      }

      const numericDurations = Object.values(byDatabase).filter(
        (value) => value !== null,
      )
      const fastestDuration = Math.min(...numericDurations)
      const slowestDuration = Math.max(...numericDurations)
      const fastestDb =
        rows.find((row) => row.median_duration_s === fastestDuration)?.db ?? "—"

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

function buildTimeSeriesChartRows(
  rows: TimeSeriesQueryComparisonRow[],
  databases: string[],
): TimeSeriesChartRow[] {
  return rows.map((row) => {
    const chartRow: TimeSeriesChartRow = {
      queryLabel: `${row.query_label} · ${row.scale}`,
    }

    for (const database of databases) {
      chartRow[database] = row.by_database[database] ?? null
    }

    return chartRow
  })
}

function formatTimeSeriesQueryName(
  queryName: string,
): {
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
  const cleanedName = normalizedName
    .replace(/_(small|large)_wide$/, "")
    .replace(/_wide$/, "")
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
  if (
    queryName.includes("max_time") ||
    queryName.includes("latest_time_range")
  ) {
    return "Time boundary"
  }
  return "Other"
}

function toTitleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toUpperCase())
}
