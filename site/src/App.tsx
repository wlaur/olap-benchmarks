import { useEffect, useState, useCallback } from "react"
import { createColumnHelper } from "@tanstack/react-table"
import { SystemSelector } from "./components/filters/SystemSelector"
import { FilterBar } from "./components/filters/FilterBar"
import { QueryTable } from "./components/QueryTable"
import { TimingChart } from "./components/Chart"
import {
  fetchSystems,
  fetchSuites,
  fetchDatabases,
  fetchRuns,
} from "./lib/queries"
import { query } from "./lib/duckdb"
import type { Filters, Run } from "./lib/types"

const columnHelper = createColumnHelper<Run>()

const columns = [
  columnHelper.accessor("suite", { header: "Suite" }),
  columnHelper.accessor("db", { header: "Database" }),
  columnHelper.accessor("db_version", { header: "Version" }),
  columnHelper.accessor("operation", { header: "Operation" }),
  columnHelper.accessor("status", {
    header: "Status",
    cell: (info) => {
      const status = info.getValue()
      const color =
        status === "completed"
          ? "text-green-400"
          : status === "failed"
            ? "text-red-400"
            : "text-yellow-400"
      return <span className={color}>{status}</span>
    },
  }),
  columnHelper.accessor("started_at", {
    header: "Started",
    cell: (info) => {
      const val = info.getValue()
      return val ? new Date(val).toLocaleString() : ""
    },
  }),
  columnHelper.accessor(
    (row) => {
      if (!row.started_at || !row.finished_at) return null
      return (
        (new Date(row.finished_at).getTime() -
          new Date(row.started_at).getTime()) /
        1000
      )
    },
    {
      id: "duration",
      header: "Duration (s)",
      cell: (info) => {
        const val = info.getValue()
        return val !== null && val !== undefined ? val.toFixed(1) : "—"
      },
    },
  ),
]

export function App() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [systems, setSystems] = useState<string[]>([])
  const [suites, setSuites] = useState<string[]>([])
  const [databases, setDatabases] = useState<string[]>([])
  const [runs, setRuns] = useState<Run[]>([])
  const [chartData, setChartData] = useState<{
    name: string
    duration_s: number
  }[]>([])
  const [filters, setFilters] = useState<Filters>({
    system: null,
    suite: null,
    db: null,
    operation: null,
  })

  useEffect(() => {
    fetchSystems()
      .then((sys) => {
        setSystems(sys)
        if (sys.length > 0) {
          setFilters((f) => ({ ...f, system: sys[0]! }))
        }
        setLoading(false)
      })
      .catch((err) => {
        setError(String(err))
        setLoading(false)
      })
  }, [])

  useEffect(() => {
    if (!filters.system) return
    fetchSuites(filters.system).then(setSuites)
    fetchDatabases(filters.system).then(setDatabases)
  }, [filters.system])

  const loadRuns = useCallback(async () => {
    if (!filters.system) return
    const data = await fetchRuns(filters)
    setRuns(data)
  }, [filters])

  useEffect(() => {
    loadRuns()
  }, [loadRuns])

  useEffect(() => {
    if (!filters.system) return
    const sysClause = `system = '${filters.system}'`
    const suiteClause = filters.suite ? ` AND suite = '${filters.suite}'` : ""
    const dbClause = filters.db ? ` AND db = '${filters.db}'` : ""
    query<{ name: string, duration_s: number }>(
      `SELECT db || ' / ' || suite AS name,
              EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_s
       FROM results.run
       WHERE ${sysClause}${suiteClause}${dbClause}
         AND operation = 'run' AND status = 'completed' AND finished_at IS NOT NULL
       ORDER BY duration_s`,
    ).then(setChartData)
  }, [filters.system, filters.suite, filters.db])

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <p className="text-gray-400">Loading DuckDB...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-screen items-center justify-center">
        <p className="text-red-400">Error: {error}</p>
      </div>
    )
  }

  return (
    <div className="mx-auto max-w-7xl px-4 py-8">
      <h1 className="mb-8 text-3xl font-bold">OLAP Benchmarks</h1>

      <div className="mb-6 flex flex-wrap gap-4">
        <SystemSelector
          systems={systems}
          selected={filters.system}
          onChange={(system) =>
            setFilters({ system, suite: null, db: null, operation: null })
          }
        />
        <FilterBar
          label="Suite"
          options={suites}
          selected={filters.suite}
          onChange={(suite) => setFilters((f) => ({ ...f, suite }))}
        />
        <FilterBar
          label="Database"
          options={databases}
          selected={filters.db}
          onChange={(db) => setFilters((f) => ({ ...f, db }))}
        />
        <FilterBar
          label="Operation"
          options={["populate", "run"]}
          selected={filters.operation}
          onChange={(operation) => setFilters((f) => ({ ...f, operation }))}
        />
      </div>

      {chartData.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-4 text-xl font-semibold">
            Run Duration Comparison
          </h2>
          <TimingChart data={chartData} />
        </div>
      )}

      <div>
        <h2 className="mb-4 text-xl font-semibold">Runs ({runs.length})</h2>
        {runs.length > 0 ? (
          <QueryTable data={runs} columns={columns} />
        ) : (
          <p className="text-gray-500">
            No runs found for the selected filters.
          </p>
        )}
      </div>
    </div>
  )
}
