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

function BenchmarkRoute({
  selectedSystem,
  isSystemLoading,
}: {
  selectedSystem: string | null
  isSystemLoading: boolean
}) {
  const { benchmarkId: rawId } = useParams<{ benchmarkId: string }>()
  const benchmark = getBenchmarkDefinition((rawId ?? defaultBenchmarkId) as BenchmarkSuiteId)

  if (benchmark.id === "time_series") {
    return <TimeSeriesPage system={selectedSystem} isSystemLoading={isSystemLoading} />
  }

  return <BenchmarkPlaceholderPage benchmark={benchmark} system={selectedSystem} />
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
    <div className="flex h-dvh min-h-screen flex-col overflow-hidden bg-surface-primary text-slate-100">
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
  const content = error ? (
    <p className="text-sm text-red-300">Failed to load systems: {error}</p>
  ) : !selectedSystem ? (
    loading ? (
      <BenchmarkRoute selectedSystem={selectedSystem} isSystemLoading />
    ) : (
      <p className="text-sm text-slate-400">No completed benchmark runs are available yet.</p>
    )
  ) : (
    <BenchmarkRoute selectedSystem={selectedSystem} isSystemLoading={loading} />
  )

  return (
    <main className="min-h-0 flex-1 overflow-y-auto px-4 py-6 lg:py-8">
      <div className="mx-auto w-full max-w-7xl">{content}</div>
    </main>
  )
}
