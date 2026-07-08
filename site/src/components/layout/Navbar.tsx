import { FlaskConical, Home } from "lucide-react"
import { NavLink, useLocation, useNavigate } from "react-router-dom"

import {
  getDefaultBenchmarkId,
  isBenchmarkSuiteId,
  type BenchmarkDefinition,
  type BenchmarkSuiteId,
} from "../../lib/benchmarks"
import { SuiteSelector } from "../filters/SuiteSelector"
import { SystemSelector, SystemSelectorSkeleton } from "../filters/SystemSelector"

interface NavbarProps {
  benchmarkDefinitions: BenchmarkDefinition[]
  suitesLoading: boolean
  systems: string[]
  selectedSystem: string | null
  onSelectSystem: (system: string) => void
  isSystemLoading?: boolean
}

const activeNavClass =
  "bg-[linear-gradient(180deg,rgba(33,48,78,0.96),rgba(21,31,52,0.96))] text-slate-50 shadow-[inset_0_0_0_1px_rgba(114,168,255,0.42),0_12px_28px_rgba(19,45,94,0.28)]"
const inactiveNavClass =
  "bg-surface-raised/88 text-slate-400 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] hover:bg-surface-raised hover:text-slate-100 hover:shadow-[inset_0_0_0_1px_rgba(120,154,214,0.18)]"
const inactiveNavTextClass =
  "bg-surface-raised/88 text-slate-300 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] hover:bg-surface-raised hover:text-slate-100 hover:shadow-[inset_0_0_0_1px_rgba(120,154,214,0.18)]"

export function Navbar({
  benchmarkDefinitions,
  suitesLoading,
  systems,
  selectedSystem,
  onSelectSystem,
  isSystemLoading = false,
}: NavbarProps) {
  const location = useLocation()
  const navigate = useNavigate()
  const isExplorerActive = location.pathname.startsWith("/explorer")
  const suiteMatch = location.pathname.match(/^\/explorer\/([^/]+)/)
  const routeSuiteId = suiteMatch?.[1]
  const resolvedSuiteId: BenchmarkSuiteId = isBenchmarkSuiteId(routeSuiteId, benchmarkDefinitions)
    ? routeSuiteId
    : getDefaultBenchmarkId(benchmarkDefinitions)

  return (
    <header className="border-b border-border-default bg-surface-primary/95 backdrop-blur">
      <div className="px-4 py-3">
        <div className="flex flex-wrap items-center gap-4">
          <div className="flex min-w-0 shrink-0 items-center gap-2">
            <NavLink to="/" className="flex shrink-0 items-center gap-3">
              <img src={`${import.meta.env.BASE_URL}logo.svg`} alt="" className="h-9 w-9" />
              <h1 className="text-xl font-semibold text-slate-50 max-sm:hidden">OLAP Benchmarks</h1>
            </NavLink>
          </div>

          <nav className="flex shrink-0 items-center gap-2">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                `inline-flex h-11 w-11 shrink-0 items-center justify-center rounded-full transition ${
                  isActive ? activeNavClass : inactiveNavClass
                }`
              }
              title="Home"
            >
              <Home size={18} />
            </NavLink>
            <NavLink
              to="/explorer/time_series"
              className={`inline-flex h-11 shrink-0 items-center gap-2 rounded-full px-4 text-sm transition ${
                isExplorerActive ? activeNavClass : inactiveNavTextClass
              }`}
            >
              <FlaskConical size={16} strokeWidth={1.8} />
              Explorer
            </NavLink>
          </nav>

          <div className="ml-auto flex min-w-0 flex-1 flex-wrap items-center justify-end gap-3">
            {isExplorerActive ? (
              <SuiteSelector
                benchmarkDefinitions={benchmarkDefinitions}
                selected={resolvedSuiteId}
                onChange={(next) => navigate(`/explorer/${next}`)}
                disabled={suitesLoading || benchmarkDefinitions.length === 0}
              />
            ) : null}
            <div className="flex shrink-0 justify-end">
              {isSystemLoading ? (
                <SystemSelectorSkeleton />
              ) : systems.length > 0 ? (
                <SystemSelector
                  systems={systems}
                  selected={selectedSystem}
                  onChange={onSelectSystem}
                  disabled={isSystemLoading}
                />
              ) : (
                <p className="text-sm text-slate-500">No completed systems found.</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </header>
  )
}
