import { useEffect } from "react"
import { Navigate, Route, Routes, useParams } from "react-router-dom"

import { Navbar } from "./components/layout/Navbar"
import {
  getDefaultBenchmarkId,
  isBenchmarkSuiteId,
  type BenchmarkDefinition,
} from "./lib/benchmarks"
import { CatalogPage } from "./pages/CatalogPage"
import { ExplorerPage } from "./pages/ExplorerPage"
import { HomePage } from "./pages/HomePage"
import { useAppStore } from "./stores/useAppStore"

function ExplorerRoute({
  preferredSystem,
  benchmarkDefinitions,
  suitesLoading,
  suitesError,
}: {
  preferredSystem: string | null
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
      suiteId={suiteId}
      suiteDefinition={suiteDefinition}
      benchmarkDefinitions={benchmarkDefinitions}
      preferredSystem={preferredSystem}
    />
  )
}

export function App() {
  const benchmarkDefinitions = useAppStore((s) => s.benchmarkDefinitions)
  const suitesLoading = useAppStore((s) => s.suitesLoading)
  const suitesError = useAppStore((s) => s.suitesError)
  const systems = useAppStore((s) => s.systems)
  const selectedSystem = useAppStore((s) => s.selectedSystem)
  const setSelectedSystem = useAppStore((s) => s.setSelectedSystem)
  const loading = useAppStore((s) => s.systemLoading)
  const loadSuites = useAppStore((s) => s.loadSuites)
  const loadSystems = useAppStore((s) => s.loadSystems)

  useEffect(() => {
    loadSuites()
    loadSystems()
  }, [loadSuites, loadSystems])

  const navbar = (
    <Navbar
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
              <div className="mx-auto w-full max-w-[76rem] px-3 sm:px-4 lg:px-6">
                <HomePage />
              </div>
            </main>
          }
        />
        <Route
          path="/explorer/:suiteId"
          element={
            <main className="flex min-h-0 flex-1 overflow-x-hidden overflow-y-auto px-3 py-4 lg:px-4 lg:py-5">
              <div className="mx-auto w-full max-w-[84rem]">
                <ExplorerRoute
                  preferredSystem={selectedSystem}
                  benchmarkDefinitions={benchmarkDefinitions}
                  suitesLoading={suitesLoading}
                  suitesError={suitesError}
                />
              </div>
            </main>
          }
        />
        <Route
          path="/catalog"
          element={
            <main className="min-h-0 flex-1 overflow-x-hidden overflow-y-auto px-3 py-4 lg:px-4 lg:py-5">
              <div className="mx-auto flex min-h-0 w-full max-w-[84rem] flex-1 flex-col">
                <CatalogPage />
              </div>
            </main>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  )
}
