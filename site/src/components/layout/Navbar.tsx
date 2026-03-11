import {
  getBenchmarkHref,
  type BenchmarkDefinition,
  type BenchmarkSuiteId,
} from "../../lib/benchmarks"
import { SystemSelector } from "../filters/SystemSelector"

interface NavbarProps {
  benchmarks: BenchmarkDefinition[]
  currentBenchmark: BenchmarkSuiteId
  systems: string[]
  selectedSystem: string | null
  onSelectSystem: (system: string) => void
  isSystemLoading?: boolean
}

export function Navbar({
  benchmarks,
  currentBenchmark,
  systems,
  selectedSystem,
  onSelectSystem,
  isSystemLoading = false,
}: NavbarProps) {
  return (
    <header className="border-b border-slate-800 bg-slate-950/95 backdrop-blur">
      <div className="mx-auto max-w-7xl px-4 py-4">
        <div className="flex items-center gap-4">
          <div className="flex shrink-0 items-center gap-3">
            <img src={`${import.meta.env.BASE_URL}logo.svg`} alt="" className="h-9 w-9" />
            <h1 className="text-xl font-semibold text-slate-50">OLAP Benchmarks</h1>
          </div>

          <div className="min-w-0 flex-1 overflow-x-auto py-1">
            <nav className="flex min-w-full items-center gap-2">
              {benchmarks.map((benchmark) => {
                const isActive = benchmark.id === currentBenchmark
                return (
                  <a
                    key={benchmark.id}
                    href={getBenchmarkHref(benchmark.id)}
                    className={`inline-flex h-11 shrink-0 items-center rounded-full px-4 text-sm transition ${
                      isActive
                        ? "bg-cyan-400/10 text-cyan-200 shadow-[inset_0_0_0_1px_rgba(34,211,238,0.8)]"
                        : "bg-slate-900 text-slate-300 shadow-[inset_0_0_0_1px_rgba(51,65,85,0.95)] hover:text-slate-100 hover:shadow-[inset_0_0_0_1px_rgba(100,116,139,0.95)]"
                    }`}
                  >
                    {benchmark.navLabel}
                  </a>
                )
              })}
            </nav>
          </div>

          <div className="flex shrink-0 justify-end">
            {systems.length > 0 ? (
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
