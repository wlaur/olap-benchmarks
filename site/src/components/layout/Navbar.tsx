import { Home } from "lucide-react"
import { NavLink } from "react-router-dom"

import type { BenchmarkDefinition } from "../../lib/benchmarks"
import { SystemSelector } from "../filters/SystemSelector"
import { Skeleton } from "../Skeleton"

interface NavbarProps {
  benchmarks: BenchmarkDefinition[]
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
  benchmarks,
  systems,
  selectedSystem,
  onSelectSystem,
  isSystemLoading = false,
}: NavbarProps) {
  return (
    <header className="border-b border-border-default bg-surface-primary/95 backdrop-blur">
      <div className="mx-auto max-w-7xl px-4 py-3">
        <div className="flex items-center justify-between gap-4">
          <div className="flex shrink-0 items-center gap-2">
            <NavLink to="/" className="flex shrink-0 items-center gap-3">
              <img src={`${import.meta.env.BASE_URL}logo.svg`} alt="" className="h-9 w-9" />
              <h1 className="text-xl font-semibold text-slate-50 max-sm:hidden">OLAP Benchmarks</h1>
            </NavLink>
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                `inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full transition md:hidden ${
                  isActive ? activeNavClass : inactiveNavClass
                }`
              }
              title="Home"
            >
              <Home size={16} />
            </NavLink>
          </div>

          <div className="hidden min-w-0 flex-1 py-1 md:block">
            <nav className="flex min-w-full items-center gap-2">
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
              {benchmarks.map((benchmark) => (
                <NavLink
                  key={benchmark.id}
                  to={`/benchmarks/${benchmark.id}`}
                  className={({ isActive }) =>
                    `inline-flex h-11 shrink-0 items-center rounded-full px-4 text-sm transition ${
                      isActive ? activeNavClass : inactiveNavTextClass
                    }`
                  }
                >
                  {benchmark.navLabel}
                </NavLink>
              ))}
            </nav>
          </div>

          <div className="flex shrink-0 justify-end">
            {isSystemLoading ? (
              <Skeleton className="h-10 w-[16.5rem] rounded-full max-sm:w-32" />
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

        <div className="mt-2 md:hidden">
          <nav className="flex flex-wrap items-center gap-2">
            {benchmarks.map((benchmark) => (
              <NavLink
                key={benchmark.id}
                to={`/benchmarks/${benchmark.id}`}
                className={({ isActive }) =>
                  `inline-flex h-9 shrink-0 items-center rounded-full px-3 text-xs transition ${
                    isActive ? activeNavClass : inactiveNavTextClass
                  }`
                }
              >
                {benchmark.navLabel}
              </NavLink>
            ))}
          </nav>
        </div>
      </div>
    </header>
  )
}
