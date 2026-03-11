import { lazy, Suspense } from "react"

import { Navbar } from "./components/layout/Navbar"
import { useSystem } from "./features/system/SystemContext"
import { useBenchmarkRoute } from "./hooks/useBenchmarkRoute"
import { benchmarkDefinitions, getBenchmarkDefinition } from "./lib/benchmarks"
import { BenchmarkPlaceholderPage } from "./pages/BenchmarkPlaceholderPage"

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
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <Navbar
          benchmarks={benchmarkDefinitions}
          currentBenchmark={currentBenchmark}
          systems={systems}
          selectedSystem={selectedSystem}
          onSelectSystem={setSelectedSystem}
          isSystemLoading
        />
        <div className="mx-auto flex max-w-7xl items-center px-4 py-24">
          <p className="text-sm text-slate-400">Loading completed benchmark data...</p>
        </div>
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
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <Navbar
        benchmarks={benchmarkDefinitions}
        currentBenchmark={currentBenchmark}
        systems={systems}
        selectedSystem={selectedSystem}
        onSelectSystem={setSelectedSystem}
      />
      <main className="mx-auto max-w-7xl px-4 py-10">
        {currentBenchmark === "time_series" ? (
          <Suspense
            fallback={
              <p className="text-sm text-slate-400">Loading time-series visualization...</p>
            }
          >
            <TimeSeriesPage system={selectedSystem} />
          </Suspense>
        ) : (
          <BenchmarkPlaceholderPage benchmark={benchmark} system={selectedSystem} />
        )}
      </main>
    </div>
  )
}
