import { Navigate, Route, Routes, useParams } from "react-router-dom"

import { Navbar } from "./components/layout/Navbar"
import { useSystem } from "./features/system/SystemContext"
import {
  benchmarkDefinitions,
  defaultBenchmarkId,
  getBenchmarkDefinition,
  type BenchmarkSuiteId,
} from "./lib/benchmarks"
import { BenchmarkPlaceholderPage } from "./pages/BenchmarkPlaceholderPage"
import { HomePage } from "./pages/HomePage"
import { TimeSeriesPage } from "./pages/TimeSeriesPage"

function BenchmarkRoute({ system }: { system: string }) {
  const { benchmarkId: rawId } = useParams<{ benchmarkId: string }>()
  const benchmark = getBenchmarkDefinition((rawId ?? defaultBenchmarkId) as BenchmarkSuiteId)

  if (benchmark.id === "time_series") {
    return <TimeSeriesPage system={system} />
  }

  return <BenchmarkPlaceholderPage benchmark={benchmark} system={system} />
}

export function App() {
  const { systems, selectedSystem, setSelectedSystem, loading, error } = useSystem()

  const navbar = (
    <Navbar
      benchmarks={benchmarkDefinitions}
      systems={systems}
      selectedSystem={selectedSystem}
      onSelectSystem={setSelectedSystem}
      isSystemLoading={loading}
    />
  )

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-slate-950 text-slate-100">
      {navbar}
      <Routes>
        <Route
          path="/"
          element={
            <main className="min-h-0 flex-1 overflow-y-auto">
              <div className="mx-auto w-full max-w-7xl px-4">
                <HomePage />
              </div>
            </main>
          }
        />
        <Route
          path="/benchmarks/:benchmarkId"
          element={
            <BenchmarkMain selectedSystem={selectedSystem} loading={loading} error={error} />
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  )
}

interface BenchmarkMainProps {
  selectedSystem: string | null
  loading: boolean
  error: string | null
}

function BenchmarkMain({ selectedSystem, loading, error }: BenchmarkMainProps) {
  const { benchmarkId: rawId } = useParams<{ benchmarkId: string }>()
  const isTimeSeries = (rawId ?? defaultBenchmarkId) === "time_series"

  const content = loading ? (
    isTimeSeries ? (
      <TimeSeriesPage system={selectedSystem} isSystemLoading />
    ) : null
  ) : error ? (
    <p className="text-sm text-red-300">Failed to load systems: {error}</p>
  ) : !selectedSystem ? (
    <p className="text-sm text-slate-400">No completed benchmark runs are available yet.</p>
  ) : (
    <BenchmarkRoute system={selectedSystem} />
  )

  return (
    <main
      className={
        isTimeSeries
          ? "flex min-h-0 w-full flex-1 overflow-x-hidden overflow-y-auto px-3 py-4 sm:px-4 lg:px-5"
          : "min-h-0 flex-1 overflow-y-auto px-4 py-10"
      }
    >
      {isTimeSeries ? content : <div className="mx-auto w-full max-w-7xl">{content}</div>}
    </main>
  )
}
