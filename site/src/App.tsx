import { lazy, Suspense } from "react"
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
import { TimeSeriesPageSkeleton } from "./pages/TimeSeriesPageSkeleton"

const TimeSeriesPage = lazy(async () => {
  const module = await import("./pages/TimeSeriesPage")
  return { default: module.TimeSeriesPage }
})

function BenchmarkRoute({ system }: { system: string }) {
  const { benchmarkId: rawId } = useParams<{ benchmarkId: string }>()
  const benchmark = getBenchmarkDefinition((rawId ?? defaultBenchmarkId) as BenchmarkSuiteId)

  if (benchmark.id === "time_series") {
    return (
      <Suspense fallback={<TimeSeriesPageSkeleton />}>
        <TimeSeriesPage system={system} />
      </Suspense>
    )
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
    <Routes>
      <Route
        path="/"
        element={
          <div className="flex min-h-screen flex-col bg-slate-950 text-slate-100">
            {navbar}
            <main className="mx-auto w-full max-w-7xl flex-1 px-4">
              <HomePage />
            </main>
          </div>
        }
      />
      <Route
        path="/benchmarks/:benchmarkId"
        element={
          <BenchmarkLayout
            navbar={navbar}
            systems={systems}
            selectedSystem={selectedSystem}
            loading={loading}
            error={error}
          />
        }
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

interface BenchmarkLayoutProps {
  navbar: React.ReactNode
  systems: string[]
  selectedSystem: string | null
  loading: boolean
  error: string | null
}

function BenchmarkLayout({ navbar, selectedSystem, loading, error }: BenchmarkLayoutProps) {
  const { benchmarkId: rawId } = useParams<{ benchmarkId: string }>()
  const isTimeSeries = (rawId ?? defaultBenchmarkId) === "time_series"

  const content = loading ? (
    isTimeSeries ? (
      <TimeSeriesPageSkeleton />
    ) : null
  ) : error ? (
    <p className="text-sm text-red-300">Failed to load systems: {error}</p>
  ) : !selectedSystem ? (
    <p className="text-sm text-slate-400">No completed benchmark runs are available yet.</p>
  ) : (
    <BenchmarkRoute system={selectedSystem} />
  )

  return (
    <div
      className={
        isTimeSeries
          ? "flex h-screen flex-col overflow-hidden bg-slate-950 text-slate-100"
          : "flex min-h-screen flex-col bg-slate-950 text-slate-100"
      }
    >
      {navbar}
      <main
        className={
          isTimeSeries
            ? "flex min-h-0 w-full flex-1 overflow-x-hidden overflow-y-auto px-3 py-4 sm:px-4 lg:px-5"
            : "mx-auto w-full max-w-7xl flex-1 px-4 py-10"
        }
      >
        {content}
      </main>
    </div>
  )
}
