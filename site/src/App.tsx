import { useEffect } from "react"
import { Navigate, Route, Routes, useNavigate, useParams } from "react-router-dom"

import { SuiteSelector } from "./components/filters/SuiteSelector"
import { Navbar } from "./components/layout/Navbar"
import { defaultBenchmarkId, type BenchmarkSuiteId } from "./lib/benchmarks"
import { ExplorerPage } from "./pages/ExplorerPage"
import { HomePage } from "./pages/HomePage"
import { useAppStore } from "./stores/useAppStore"

function ExplorerRoute({
  selectedSystem,
  isSystemLoading,
}: {
  selectedSystem: string | null
  isSystemLoading: boolean
}) {
  const { suiteId: rawId } = useParams<{ suiteId: string }>()
  const navigate = useNavigate()
  const suiteId = (rawId ?? defaultBenchmarkId) as BenchmarkSuiteId

  return (
    <>
      <div className="mb-4 flex items-center gap-3 px-4">
        <SuiteSelector selected={suiteId} onChange={(next) => navigate(`/explorer/${next}`)} />
      </div>
      <ExplorerPage system={selectedSystem} suiteId={suiteId} isSystemLoading={isSystemLoading} />
    </>
  )
}

export function App() {
  const systems = useAppStore((s) => s.systems)
  const selectedSystem = useAppStore((s) => s.selectedSystem)
  const setSelectedSystem = useAppStore((s) => s.setSelectedSystem)
  const loading = useAppStore((s) => s.systemLoading)
  const error = useAppStore((s) => s.systemError)
  const loadSystems = useAppStore((s) => s.loadSystems)

  useEffect(() => {
    loadSystems()
  }, [loadSystems])

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
  const content = error ? (
    <p className="text-sm text-red-300">Failed to load systems: {error}</p>
  ) : !selectedSystem ? (
    loading ? (
      <ExplorerRoute selectedSystem={selectedSystem} isSystemLoading />
    ) : (
      <p className="text-sm text-slate-400">No completed benchmark runs are available yet.</p>
    )
  ) : (
    <ExplorerRoute selectedSystem={selectedSystem} isSystemLoading={loading} />
  )

  return (
    <main className="flex min-h-0 flex-1 overflow-x-hidden overflow-y-auto py-4 lg:py-5">
      <div className="flex min-h-0 w-full flex-1 flex-col">{content}</div>
    </main>
  )
}
