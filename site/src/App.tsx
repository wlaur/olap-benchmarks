import { useEffect } from "react"
import { Navigate, Route, Routes, useParams } from "react-router-dom"

import { Navbar } from "./components/layout/Navbar"
import {
  getDefaultBenchmarkId,
  isBenchmarkSuiteId,
  type BenchmarkDefinition,
} from "./lib/benchmarks"
import { ExplorerPage } from "./pages/ExplorerPage"
import { ExplorerPrototypePage } from "./pages/ExplorerPrototypePage"
import { HomePage } from "./pages/HomePage"
import { useAppStore } from "./stores/useAppStore"

function ExplorerRoute({
  selectedSystem,
  isSystemLoading,
  benchmarkDefinitions,
  suitesLoading,
  suitesError,
}: {
  selectedSystem: string | null
  isSystemLoading: boolean
  benchmarkDefinitions: BenchmarkDefinition[]
  suitesLoading: boolean
  suitesError: string | null
}) {
  const { suiteId: rawId } = useParams<{ suiteId: string }>()

  if (suitesError) {
    return <p className="text-sm text-red-300">Failed to load suites: {suitesError}</p>
  }

  if (suitesLoading || benchmarkDefinitions.length === 0) {
    return <p className="text-sm text-slate-400">Loading suites...</p>
  }

  const suiteId = isBenchmarkSuiteId(rawId, benchmarkDefinitions)
    ? rawId
    : getDefaultBenchmarkId(benchmarkDefinitions)
  const suiteDefinition = benchmarkDefinitions.find((definition) => definition.id === suiteId)

  if (!suiteDefinition) {
    return <p className="text-sm text-red-300">Suite {suiteId} is not configured.</p>
  }

  return (
    <ExplorerPage
      system={selectedSystem}
      suiteId={suiteId}
      suiteDefinition={suiteDefinition}
      isSystemLoading={isSystemLoading}
    />
  )
}

export function App() {
  const benchmarkDefinitions = useAppStore((s) => s.benchmarkDefinitions)
  const suitesLoading = useAppStore((s) => s.suitesLoading)
  const systems = useAppStore((s) => s.systems)
  const selectedSystem = useAppStore((s) => s.selectedSystem)
  const setSelectedSystem = useAppStore((s) => s.setSelectedSystem)
  const loading = useAppStore((s) => s.systemLoading)
  const error = useAppStore((s) => s.systemError)
  const loadSuites = useAppStore((s) => s.loadSuites)
  const loadSystems = useAppStore((s) => s.loadSystems)

  useEffect(() => {
    loadSuites()
    loadSystems()
  }, [loadSuites, loadSystems])

  const navbar = (
    <Navbar
      benchmarkDefinitions={benchmarkDefinitions}
      suitesLoading={suitesLoading}
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
              <div className="mx-auto w-full max-w-4xl px-4">
                <HomePage />
              </div>
            </main>
          }
        />
        <Route
          path="/explorer/:suiteId"
          element={<ExplorerMain selectedSystem={selectedSystem} loading={loading} error={error} />}
        />
        <Route
          path="/prototype/explorer"
          element={
            <main className="flex min-h-0 flex-1 overflow-x-hidden overflow-y-auto px-3 py-4 lg:px-4 lg:py-5">
              <div className="flex min-h-0 w-full flex-1 flex-col">
                <ExplorerPrototypePage />
              </div>
            </main>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  )
}

interface ExplorerMainProps {
  selectedSystem: string | null
  loading: boolean
  error: string | null
}

function ExplorerMain({ selectedSystem, loading, error }: ExplorerMainProps) {
  const benchmarkDefinitions = useAppStore((s) => s.benchmarkDefinitions)
  const suitesLoading = useAppStore((s) => s.suitesLoading)
  const suitesError = useAppStore((s) => s.suitesError)

  const content = error ? (
    <p className="text-sm text-red-300">Failed to load systems: {error}</p>
  ) : !selectedSystem ? (
    loading ? (
      <ExplorerRoute
        selectedSystem={selectedSystem}
        isSystemLoading
        benchmarkDefinitions={benchmarkDefinitions}
        suitesLoading={suitesLoading}
        suitesError={suitesError}
      />
    ) : (
      <p className="text-sm text-slate-400">No completed benchmark runs are available yet.</p>
    )
  ) : (
    <ExplorerRoute
      selectedSystem={selectedSystem}
      isSystemLoading={loading}
      benchmarkDefinitions={benchmarkDefinitions}
      suitesLoading={suitesLoading}
      suitesError={suitesError}
    />
  )

  return (
    <main className="flex min-h-0 flex-1 overflow-x-hidden overflow-y-auto px-3 py-4 lg:px-4 lg:py-5">
      <div className="flex min-h-0 w-full flex-1 flex-col">{content}</div>
    </main>
  )
}
