import { BodyText, DisplayTitle, Eyebrow } from "../components/Typography"
import type { BenchmarkDefinition } from "../lib/benchmarks"

interface BenchmarkPlaceholderPageProps {
  benchmark: BenchmarkDefinition
  system: string | null
}

export function BenchmarkPlaceholderPage({ benchmark, system }: BenchmarkPlaceholderPageProps) {
  return (
    <section className="space-y-6">
      <header className="max-w-3xl space-y-3">
        <Eyebrow>{benchmark.title}</Eyebrow>
        <DisplayTitle as="h2" className="text-4xl">
          Dedicated visualization pending
        </DisplayTitle>
        <BodyText className="text-lg leading-8 text-slate-300">{benchmark.summary}</BodyText>
      </header>

      <div className="rounded-3xl border border-dashed border-slate-700 bg-slate-900/40 p-8">
        <p className="text-sm text-slate-300">
          This page is intentionally scaffolded but empty for now. It is already wired into the app
          shell, uses the global system scope, and is ready for a suite-specific visualization
          design.
        </p>
        <p className="mt-4 text-sm text-slate-500">
          Active system: <span className="text-slate-300">{system ?? "Loading..."}</span>
        </p>
      </div>
    </section>
  )
}
