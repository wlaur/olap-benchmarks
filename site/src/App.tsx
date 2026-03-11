import { lazy, Suspense } from "react"

import { Navbar } from "./components/layout/Navbar"
import { Skeleton } from "./components/Skeleton"
import { useSystem } from "./features/system/SystemContext"
import { useBenchmarkRoute } from "./hooks/useBenchmarkRoute"
import { benchmarkDefinitions, getBenchmarkDefinition } from "./lib/benchmarks"
import { BenchmarkPlaceholderPage } from "./pages/BenchmarkPlaceholderPage"
import { TimeSeriesPageSkeleton } from "./pages/TimeSeriesPageSkeleton"

const TimeSeriesPage = lazy(async () => {
  const module = await import("./pages/TimeSeriesPage")
  return { default: module.TimeSeriesPage }
})

export function App() {
  const currentBenchmark = useBenchmarkRoute()
  const benchmark = getBenchmarkDefinition(currentBenchmark)
  const { systems, selectedSystem, setSelectedSystem, loading, error } = useSystem()

  if (loading) {
    return (
      <div
        className={
          currentBenchmark === "time_series"
            ? "flex h-screen flex-col overflow-hidden bg-slate-950 text-slate-100"
            : "flex min-h-screen flex-col bg-slate-950 text-slate-100"
        }
      >
        <Navbar
          benchmarks={benchmarkDefinitions}
          currentBenchmark={currentBenchmark}
          systems={systems}
          selectedSystem={selectedSystem}
          onSelectSystem={setSelectedSystem}
          isSystemLoading
        />
        <main
          className={
            currentBenchmark === "time_series"
              ? "flex min-h-0 w-full flex-1 overflow-hidden px-3 py-4 sm:px-4 lg:px-5"
              : "mx-auto w-full max-w-7xl flex-1 px-4 py-10"
          }
        >
          {currentBenchmark === "time_series" ? (
            <TimeSeriesPageSkeleton />
          ) : (
            <div className="space-y-4">
              <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6">
                <Skeleton className="h-4 w-32 rounded-full" />
                <Skeleton className="mt-4 h-10 w-72" />
                <Skeleton className="mt-3 h-4 w-full max-w-2xl" />
              </div>
              <div className="grid gap-4 md:grid-cols-3">
                <Skeleton className="h-36 rounded-3xl" />
                <Skeleton className="h-36 rounded-3xl" />
                <Skeleton className="h-36 rounded-3xl" />
              </div>
            </div>
          )}
        </main>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <Navbar
          benchmarks={benchmarkDefinitions}
          currentBenchmark={currentBenchmark}
          systems={systems}
          selectedSystem={selectedSystem}
          onSelectSystem={setSelectedSystem}
        />
        <div className="mx-auto max-w-7xl px-4 py-24">
          <p className="text-sm text-red-300">Failed to load systems: {error}</p>
        </div>
      </div>
    )
  }

  if (!selectedSystem) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <Navbar
          benchmarks={benchmarkDefinitions}
          currentBenchmark={currentBenchmark}
          systems={systems}
          selectedSystem={selectedSystem}
          onSelectSystem={setSelectedSystem}
        />
        <div className="mx-auto max-w-7xl px-4 py-24">
          <p className="text-sm text-slate-400">No completed benchmark runs are available yet.</p>
        </div>
      </div>
    )
  }

  return (
    <div
      className={
        currentBenchmark === "time_series"
          ? "flex h-screen flex-col overflow-hidden bg-slate-950 text-slate-100"
          : "flex min-h-screen flex-col bg-slate-950 text-slate-100"
      }
    >
      <Navbar
        benchmarks={benchmarkDefinitions}
        currentBenchmark={currentBenchmark}
        systems={systems}
        selectedSystem={selectedSystem}
        onSelectSystem={setSelectedSystem}
      />
      <main
        className={
          currentBenchmark === "time_series"
            ? "flex min-h-0 w-full flex-1 overflow-hidden px-3 py-4 sm:px-4 lg:px-5"
            : "mx-auto w-full max-w-7xl flex-1 px-4 py-10"
        }
      >
        {currentBenchmark === "time_series" ? (
          <Suspense fallback={<TimeSeriesPageSkeleton />}>
            <TimeSeriesPage system={selectedSystem} />
          </Suspense>
        ) : (
          <BenchmarkPlaceholderPage benchmark={benchmark} system={selectedSystem} />
        )}
      </main>
    </div>
  )
}
