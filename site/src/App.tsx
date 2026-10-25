import { useEffect } from "react"
import { Navigate, Route, Routes, useLocation, useParams } from "react-router-dom"

import { Navbar } from "./components/layout/Navbar"
import { NoticeStrip, PanelCard } from "./components/layout/Panel"
import { BodyText, SectionTitle } from "./components/Typography"
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
  const location = useLocation()

  if (suitesError || suitesLoading || benchmarkDefinitions.length === 0) {
    return (
      <PanelCard className="space-y-4 p-4 sm:p-6" aria-busy={suitesLoading}>
        <SectionTitle as="h1">Explorer</SectionTitle>
        <div role={suitesError ? "alert" : "status"}>
          <BodyText>
            {suitesError
              ? `Failed to load suites: ${suitesError}`
              : suitesLoading
                ? "Loading workloads…"
                : "No benchmark suites are available."}
          </BodyText>
        </div>
      </PanelCard>
    )
  }

  const suiteId = isBenchmarkSuiteId(rawId, benchmarkDefinitions)
    ? rawId
    : getDefaultBenchmarkId(benchmarkDefinitions)
  const suiteDefinition = benchmarkDefinitions.find((definition) => definition.id === suiteId)

  if (!isBenchmarkSuiteId(rawId, benchmarkDefinitions)) {
    return <Navigate to={`/explorer/${suiteId}${location.search}`} replace />
  }

  if (!suiteDefinition) {
    return <p className="text-sm text-red-300">Suite {suiteId} is not configured.</p>
  }

  return (
    <ExplorerPage
      key={suiteId}
      suiteId={suiteId}
      suiteDefinition={suiteDefinition}
      benchmarkDefinitions={benchmarkDefinitions}
      preferredSystem={preferredSystem}
    />
  )
}

export function App() {
  const location = useLocation()
  const explorerPath = useAppStore((s) => s.explorerPath)
  const setExplorerPath = useAppStore((s) => s.setExplorerPath)
  const benchmarkDefinitions = useAppStore((s) => s.benchmarkDefinitions)
  const suitesLoading = useAppStore((s) => s.suitesLoading)
  const suitesError = useAppStore((s) => s.suitesError)
  const systems = useAppStore((s) => s.systems)
  const selectedSystem = useAppStore((s) => s.selectedSystem)
  const setSelectedSystem = useAppStore((s) => s.setSelectedSystem)
  const loading = useAppStore((s) => s.systemLoading)
  const systemError = useAppStore((s) => s.systemError)
  const loadSuites = useAppStore((s) => s.loadSuites)
  const loadSystems = useAppStore((s) => s.loadSystems)

  useEffect(() => {
    loadSuites()
    loadSystems()
  }, [loadSuites, loadSystems])

  useEffect(() => {
    if (location.pathname.startsWith("/explorer/"))
      setExplorerPath(location.pathname + location.search)
  }, [location.pathname, location.search, setExplorerPath])

  const navbar = (
    <Navbar
      systems={systems}
      selectedSystem={selectedSystem}
      onSelectSystem={setSelectedSystem}
      isSystemLoading={loading}
      explorerPath={explorerPath}
    />
  )

  return (
    <div className="flex h-dvh min-h-screen flex-col overflow-hidden bg-surface-primary text-slate-100">
      {navbar}
      {systemError ? (
        <NoticeStrip role="alert" tone="warning" className="mx-4 my-3 shrink-0">
          Failed to load results: {systemError}
        </NoticeStrip>
      ) : null}
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
