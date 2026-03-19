import { FlaskConical, Home } from "lucide-react"
import { NavLink, useLocation } from "react-router-dom"

import { SystemSelector, SystemSelectorSkeleton } from "../filters/SystemSelector"

interface NavbarProps {
  systems: string[]
  selectedSystem: string | null
  onSelectSystem: (system: string) => void
  isSystemLoading?: boolean
}

const activeNavClass =
  "bg-accent-400/10 text-accent-200 shadow-[inset_0_0_0_1px_rgba(108,142,239,0.5)]"
const inactiveNavClass =
  "bg-surface-raised text-slate-400 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] hover:text-slate-100 hover:shadow-[inset_0_0_0_1px_rgba(148,163,184,0.18)]"
const inactiveNavTextClass =
  "bg-surface-raised text-slate-300 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] hover:text-slate-100 hover:shadow-[inset_0_0_0_1px_rgba(148,163,184,0.18)]"

export function Navbar({
  systems,
  selectedSystem,
  onSelectSystem,
  isSystemLoading = false,
}: NavbarProps) {
  const location = useLocation()
  const isExplorerActive = location.pathname.startsWith("/explorer")

  return (
    <header className="border-b border-border-default bg-surface-primary/95 backdrop-blur">
      <div className="mx-auto max-w-7xl px-4 py-3">
        <div className="flex items-center justify-between gap-4">
          <div className="flex shrink-0 items-center gap-2">
            <NavLink to="/" className="flex shrink-0 items-center gap-3">
              <img src={`${import.meta.env.BASE_URL}logo.svg`} alt="" className="h-9 w-9" />
              <h1 className="text-xl font-semibold text-slate-50 max-sm:hidden">OLAP Benchmarks</h1>
            </NavLink>
          </div>

          <div className="flex min-w-0 flex-1 items-center justify-center py-1">
            <nav className="flex items-center gap-2">
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
          </div>

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
    </header>
  )
}
