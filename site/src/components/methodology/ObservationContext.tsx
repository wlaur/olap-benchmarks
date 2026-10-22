import { useState } from "react"

import type { ComparisonAnnotation } from "../../lib/comparison"
import { record } from "../../lib/comparison"
import { formatLatencyMs } from "../../lib/format"
import type { FamilyEngineResult, QueryObservation, RunObservation } from "../../lib/observations"
import { ChartFrame } from "../layout/Panel"
import { BodyText, CaptionText, MetaLabel } from "../Typography"

function fact(value: unknown): string {
  if (value === null || value === undefined || value === "") return "Unknown — not recorded"
  return typeof value === "object" ? JSON.stringify(value) : String(value)
}

export function RunContext({
  run,
  annotations,
}: {
  run: RunObservation | null
  annotations: readonly ComparisonAnnotation[]
}) {
  const metadata = run?.metadata ?? {}
  const repository = record(metadata.repository)
  const execution = record(metadata.execution)
  const input = record(metadata.input)
  const queries = record(metadata.queries)
  const options = record(metadata.options)
  const settings = record(metadata.settings)
  return (
    <details className="min-w-0 text-xs text-slate-400">
      <summary className="cursor-pointer py-2 text-slate-300">
        Run context &amp; preparation{run ? ` · run ${run.runId}` : " · no recorded run"}
      </summary>
      <ChartFrame className="mt-1 space-y-3">
        <dl className="grid min-w-0 gap-x-5 gap-y-2 sm:grid-cols-2">
          {Object.entries({
            "Selected run / status": run ? `${run.runId} / ${run.status}` : null,
            "Engine / driver": run
              ? `${run.db} ${run.version} / ${run.driver ?? "driver unknown"}`
              : null,
            "Execution mode": execution.mode,
            "Resource budget": run
              ? `CPU threads: ${fact(settings.OLAP_BENCHMARKS_BENCHMARK_THREADS)}; benchmark memory GiB: ${fact(settings.OLAP_BENCHMARKS_BENCHMARK_MEMORY_GB)}; client memory limit GiB: ${fact(settings.OLAP_BENCHMARKS_CLIENT_MEMORY_LIMIT_GB)}`
              : null,
            "Recorded settings": Object.keys(settings).length ? settings : null,
            "System snapshot": run ? { id: run.snapshotId, ...run.snapshot } : null,
            "Input fingerprint": input.manifest_sha256,
            "Query fingerprint": queries.digest,
            "Schema fingerprint (not schema proof)": record(metadata.schema).digest,
            "Source commit": repository.commit,
            "Working tree dirty / diff fingerprint": repository.commit
              ? {
                  dirty: repository.dirty ?? "unknown",
                  diff_sha256: repository.diff_sha256 ?? "not recorded",
                }
              : null,
            "Recorded layout / options": Object.keys(options).length ? options : null,
            Invalidation: metadata.invalidation ?? (run ? "None recorded" : null),
            "Recorded run error": run?.error ?? (run ? "None recorded" : null),
          }).map(([label, value]) => (
            <div key={label} className="min-w-0">
              <dt className="text-slate-500">{label}</dt>
              <dd className="mt-0.5 font-mono [overflow-wrap:anywhere] break-words text-slate-300">
                {fact(value)}
              </dd>
            </div>
          ))}
        </dl>
        <MetaLabel>Preparation &amp; tuning evidence</MetaLabel>
        {annotations.length === 0 ? (
          <BodyText className="text-xs">
            Preparation unknown; tuning unknown. No sourced annotation supplied.
          </BodyText>
        ) : (
          annotations.map((annotation, index) => (
            <p key={index} className="leading-5">
              Preparation: {annotation.preparation}. Tuning: {annotation.tuning ?? "unknown"}.{" "}
              {annotation.summary}{" "}
              <a
                href={annotation.evidence_url}
                target="_blank"
                rel="noreferrer"
                className="text-accent-300 underline"
              >
                Source
              </a>
            </p>
          ))
        )}
      </ChartFrame>
    </details>
  )
}

export function ValidationSummary({ queries }: { queries: readonly QueryObservation[] }) {
  const counts = new Map<string, number>()
  for (const query of queries) counts.set(query.evidence, (counts.get(query.evidence) ?? 0) + 1)
  return (
    <p className="text-xs leading-5 text-slate-400">
      {counts.size > 0
        ? [...counts].map(([evidence, count]) => `${count} ${evidence}`).join(" · ")
        : "Unknown — no query evidence recorded"}
    </p>
  )
}

export function QueryEvidence({
  query,
  comparable,
}: {
  query: QueryObservation
  comparable: boolean
}) {
  return (
    <div className="min-w-0 rounded-md border border-border-subtle p-2 text-xs text-slate-400">
      <div className="flex flex-wrap justify-between gap-2">
        <strong className="text-slate-200">{query.queryName}</strong>
        <span>{comparable ? "Performance set" : "Coverage only"}</span>
      </div>
      <p className="mt-1">
        {query.warmup} warmup + {query.measured}/3 measured ·{" "}
        {query.medianS === null
          ? "latency unknown"
          : `${formatLatencyMs(query.medianS * 1000)} median`}
      </p>
      <p className="mt-1">
        {query.evidence}: {query.evidenceReason}
      </p>
      {query.reasons.length > 0 ? (
        <p className="mt-1 text-amber-200">{query.reasons.join("; ")}</p>
      ) : null}
      <details className="mt-1">
        <summary className="cursor-pointer">Iterations, statuses &amp; hashes</summary>
        <div className="mt-1 space-y-1 font-mono [overflow-wrap:anywhere] break-words">
          {query.steps.length === 0
            ? "No iterations recorded"
            : query.steps.map((step) => (
                <p key={step.id}>
                  step {step.id} · {step.role ?? "role unknown"}{" "}
                  {step.iteration ?? "iteration unknown"} · {step.status}/
                  {step.resultStatus ?? "result unknown"} · rows {step.rowCount ?? "unknown"} ·{" "}
                  {step.hashVersion ?? "hash version unknown"} ·{" "}
                  {step.hash ?? step.hashMissingReason ?? "hash unknown"}
                  {step.skipReason ? ` · recorded exclusion: ${step.skipReason}` : ""}
                  {step.error ? ` · ${step.error}` : ""}
                </p>
              ))}
        </div>
      </details>
    </div>
  )
}

export function EngineEvidence({
  row,
  performanceQueries,
}: {
  row: FamilyEngineResult
  performanceQueries: readonly string[]
}) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div className="min-w-0">
      <RunContext run={row.run} annotations={row.annotations} />
      {row.queries.length > 0 ? (
        <details
          className="text-xs text-slate-400"
          onToggle={(event) => setExpanded(event.currentTarget.open)}
        >
          <summary className="cursor-pointer py-2">
            Query execution &amp; validation evidence
          </summary>
          {expanded ? (
            <div className="grid gap-2 py-2 sm:grid-cols-2">
              {row.queries.map((query) => (
                <QueryEvidence
                  key={query.queryName}
                  query={query}
                  comparable={performanceQueries.includes(query.queryName)}
                />
              ))}
            </div>
          ) : null}
        </details>
      ) : null}
      <CaptionText className="mt-1 leading-4">
        Hashes compare canonicalized answers. They do not prove exact schemas or agreement with an
        independent oracle.
      </CaptionText>
    </div>
  )
}
