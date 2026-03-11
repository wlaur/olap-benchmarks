import type { BenchmarkDefinition } from "../lib/benchmarks"

interface BenchmarkPlaceholderPageProps {
  benchmark: BenchmarkDefinition
  system: string
}

export function BenchmarkPlaceholderPage({
  benchmark,
  system,
}: BenchmarkPlaceholderPageProps) {
  return (
    <section className="space-y-6">
      <header className="max-w-3xl space-y-3">
        <p className="text-sm font-medium uppercase tracking-[0.18em] text-cyan-300">
          {benchmark.title}
        </p>
        <h2 className="text-4xl font-semibold tracking-tight text-slate-50">
          Dedicated visualization pending
        </h2>
        <p className="text-lg leading-8 text-slate-300">{benchmark.summary}</p>
      </header>

      <div className="rounded-3xl border border-dashed border-slate-700 bg-slate-900/40 p-8">
        <p className="text-sm text-slate-300">
          This page is intentionally scaffolded but empty for now. It is already
          wired into the app shell, uses the global system scope, and is ready
          for a suite-specific visualization design.
        </p>
        <p className="mt-4 text-sm text-slate-500">
          Active system: <span className="text-slate-300">{system}</span>
        </p>
      </div>
    </section>
  )
}
