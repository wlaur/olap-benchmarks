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
      <div className="mx-auto max-w-7xl px-4 py-5">
        <div className="flex flex-col gap-5">
          <div className="flex items-center gap-3">
            <img src={`${import.meta.env.BASE_URL}logo.svg`} alt="" className="h-9 w-9" />
            <div>
              <h1 className="text-xl font-semibold text-slate-50">OLAP Benchmarks</h1>
              <p className="text-sm text-slate-400">
                Benchmark-specific views, scoped to one system at a time.
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3">
            <nav className="flex min-w-0 flex-1 flex-wrap gap-2">
              {benchmarks.map((benchmark) => {
                const isActive = benchmark.id === currentBenchmark
                return (
                  <a
                    key={benchmark.id}
                    href={getBenchmarkHref(benchmark.id)}
                    className={`rounded-full border px-4 py-2 text-sm transition ${
                      isActive
                        ? "border-cyan-400 bg-cyan-400/10 text-cyan-200"
                        : "border-slate-700 bg-slate-900 text-slate-300 hover:border-slate-500 hover:text-slate-100"
                    }`}
                  >
                    {benchmark.navLabel}
                  </a>
                )
              })}
            </nav>

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
      </div>
    </header>
  )
}
