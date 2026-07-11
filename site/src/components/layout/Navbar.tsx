import { BookOpenText, FlaskConical, Home } from "lucide-react"
import { NavLink, useLocation } from "react-router-dom"

import { SystemSelector, SystemSelectorSkeleton } from "../filters/SystemSelector"

interface NavbarProps {
  systems: string[]
  selectedSystem: string | null
  onSelectSystem: (system: string) => void
  isSystemLoading?: boolean
}

const activeNavClass =
  "border-accent-400/60 bg-surface-elevated text-slate-50 shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]"
const inactiveNavTextClass =
  "border-border-subtle bg-surface-inset text-slate-300 hover:border-border-default hover:bg-surface-raised hover:text-slate-100"

export function Navbar({
  systems,
  selectedSystem,
  onSelectSystem,
  isSystemLoading = false,
}: NavbarProps) {
  const location = useLocation()
  const isExplorerActive = location.pathname.startsWith("/explorer")
  const isCatalogActive = location.pathname.startsWith("/catalog")

  return (
    <header className="border-b border-border-default bg-surface-primary/95 backdrop-blur">
      <div className="px-4 py-2">
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex min-w-0 shrink-0 items-center gap-2">
            <NavLink to="/" className="flex shrink-0 items-center gap-3">
              <img src={`${import.meta.env.BASE_URL}logo.svg`} alt="" className="h-8 w-8" />
              <span className="text-base font-semibold tracking-tight text-slate-50 max-sm:hidden">
                OLAP Benchmarks
              </span>
            </NavLink>
          </div>

          <nav className="flex shrink-0 items-center gap-2">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                `inline-flex h-9 shrink-0 items-center gap-2 rounded-md border px-3 text-xs transition ${
                  isActive ? activeNavClass : inactiveNavTextClass
                }`
              }
            >
              <Home size={16} strokeWidth={1.8} />
              Home
            </NavLink>
            <NavLink
              to="/explorer/time_series"
              className={`inline-flex h-9 shrink-0 items-center gap-2 rounded-md border px-3 text-xs transition ${
                isExplorerActive ? activeNavClass : inactiveNavTextClass
              }`}
            >
              <FlaskConical size={16} strokeWidth={1.8} />
              Explorer
            </NavLink>
            <NavLink
              to="/catalog"
              className={`inline-flex h-9 shrink-0 items-center gap-2 rounded-md border px-3 text-xs transition ${
                isCatalogActive ? activeNavClass : inactiveNavTextClass
              }`}
            >
              <BookOpenText size={16} strokeWidth={1.8} />
              Catalog
            </NavLink>
          </nav>

          {!isExplorerActive && !isCatalogActive ? (
            <div className="flex w-full min-w-0 basis-full flex-wrap items-center justify-end gap-3 lg:ml-auto lg:w-auto lg:flex-1 lg:basis-auto">
              <div className="flex w-full shrink-0 justify-end lg:w-auto">
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
          ) : null}
        </div>
      </div>
    </header>
  )
}
