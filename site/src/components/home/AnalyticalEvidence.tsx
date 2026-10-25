import type { ReactNode } from "react"

import { useAnalyticalData } from "../../hooks/useAnalyticalData"
import {
  parseAnalyticalSummary,
  type AnalyticalRun,
  type AnalyticalSummary,
} from "../../lib/analytical"
import { qpsCause, stoppedOutcome, stoppedPhase } from "../../lib/analyticalOutcome"
import { formatDatabaseName } from "../../lib/databaseNames"
import { formatLatencyMs } from "../../lib/format"
import { useAppStore } from "../../stores/useAppStore"
import { DetailsDialog } from "../layout/DetailsDialog"
import { Disclosure } from "../layout/Disclosure"
import {
  ChartFrame,
  NoticeStrip,
  PanelDisclosure,
  StatusBadge,
  type StatusTone,
} from "../layout/Panel"
import {
  BodyText,
  CaptionText,
  ItemTitle,
  LeadText,
  MetaLabel,
  NumericText,
  SecondaryText,
} from "../Typography"

const unknown = "Unknown"
const integer = (value: number | null) => (value === null ? unknown : value.toLocaleString("en-US"))
const seconds = (value: number | null) => (value === null ? unknown : `${value.toFixed(2)}s`)
const qps = (value: number | null) =>
  value === null
    ? unknown
    : value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const clientCount = (value: number | null) =>
  value === null ? "clients unknown" : `${integer(value)} client${value === 1 ? "" : "s"}`
const gib = (value: number | null) =>
  value === null ? unknown : `${(value / 1024 ** 3).toFixed(1)} GiB`

function engineLabel(run: AnalyticalRun) {
  return `${formatDatabaseName(run.row.db)} ${run.row.db_version || ""}`.trim()
}

function runOutcome(run: AnalyticalRun): { label: string; tone: StatusTone } {
  if (run.row.status !== "completed")
    return {
      label: stoppedOutcome(run),
      tone: run.row.status === "failed" ? "failure" : "neutral",
    }
  const { errors, timeouts } = run.overall
  if (errors === null || timeouts === null)
    return { label: "Completed · error counts unknown", tone: "warning" }
  if (errors > 0 || timeouts > 0)
    return { label: `Completed · ${errors} errors, ${timeouts} timeouts`, tone: "warning" }
  return { label: "Completed", tone: "positive" }
}

function RunSummary({ run }: { run: AnalyticalRun }) {
  const completed = run.row.status === "completed"
  const phase = completed ? null : stoppedPhase(run)
  const cause = qpsCause(run)
  const warmup =
    run.recorded?.warmup_summary === undefined
      ? null
      : parseAnalyticalSummary(run.recorded.warmup_summary, null)
  return (
    <div className="space-y-1" data-analytical-outcome>
      <LeadText className="font-semibold">
        {completed ? runOutcome(run).label : `${stoppedOutcome(run)}${phase ? ` ${phase}` : ""}`}
      </LeadText>
      {cause ? <BodyText>{cause}</BodyText> : null}
      {!completed && warmup && warmup.offered !== null ? (
        <div data-analytical-warmup>
          <SecondaryText>
            Warmup requests: {integer(warmup.completed)} of {integer(warmup.offered)} completed ·{" "}
            {integer(warmup.errors)} errors · {integer(warmup.timeouts)} timed out
          </SecondaryText>
        </div>
      ) : null}
    </div>
  )
}

function analyticalTakeaway(runs: readonly AnalyticalRun[]): string {
  return `${runs.filter((run) => run.row.status === "completed").length}/${runs.length} runs completed`
}

function Latency({ summary }: { summary: AnalyticalSummary }) {
  return (
    <div className="space-y-0.5 whitespace-nowrap">
      {summary.percentiles.map((percentile, index) => (
        <div
          key={index}
          title={
            percentile.status === "insufficient_samples"
              ? `insufficient_samples: n=${summary.samples}, minimum_n=${percentile.minimumN}`
              : undefined
          }
        >
          <span className="text-slate-400">p{[50, 95, 99][index]} </span>
          {percentile.valueMs === null ? unknown : formatLatencyMs(percentile.valueMs)}
          {percentile.status === "insufficient_samples" ? (
            <span className="text-slate-400"> (&lt;{percentile.minimumN})</span>
          ) : null}
        </div>
      ))}
      <div className="text-slate-400">n={integer(summary.samples)}</div>
    </div>
  )
}

function RecordedObject({ value }: { value: Record<string, unknown> | null }) {
  return value === null || Object.keys(value).length === 0 ? (
    <SecondaryText>{unknown}</SecondaryText>
  ) : (
    <pre className="max-h-64 overflow-auto font-mono text-sm break-all whitespace-pre-wrap text-slate-300">
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <MetaLabel as="dt">{label}</MetaLabel>
      <dd className="mt-1 text-sm leading-6 break-words text-slate-100">{children}</dd>
    </div>
  )
}

function RunEvidence({ run }: { run: AnalyticalRun }) {
  const error = [run.row.error_type, run.row.error_message]
    .filter((value) => value?.trim())
    .join(": ")
  return (
    <>
      <RunSummary run={run} />
      <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-3">
        <Fact label="Workload">
          {run.row.suite} · SF {run.row.suite_scale_factor} · {run.row.system}
        </Fact>
        <Fact label="Engine">
          {engineLabel(run)}
          {run.row.db_driver ? ` · ${run.row.db_driver}` : ""}
        </Fact>
        <Fact label="Status">
          {run.row.status || unknown} · run {run.row.id}
        </Fact>
        <Fact label="Window actual / requested">
          {seconds(run.elapsedSeconds)} / {seconds(run.requestedSeconds)}
        </Fact>
        <Fact label="Completed / offered">
          {integer(run.overall.completed)} / {integer(run.overall.offered)}
        </Fact>
        <Fact label="Errors / timeouts">
          {integer(run.overall.errors)} / {integer(run.overall.timeouts)}
        </Fact>
        <Fact label="Throughput">{qps(run.overall.qps)} QPS</Fact>
        <Fact label="Mixed-query latency">
          <Latency summary={run.overall} />
        </Fact>
      </dl>
      <SecondaryText>
        Measurement: {run.startedAt ?? unknown} → {run.finishedAt ?? unknown}. Drain:{" "}
        {seconds(run.drainSeconds)}. Run finished: {run.row.finished_at ?? unknown}.
      </SecondaryText>
      {error ? (
        <Disclosure title="Recorded run error">
          <SecondaryText className="font-mono break-words">{error}</SecondaryText>
        </Disclosure>
      ) : null}
      {run.limitations.length > 0 ? (
        <ul className="list-disc space-y-1 pl-5 text-sm leading-6 text-amber-200">
          {run.limitations.map((limitation) => (
            <li key={limitation}>{limitation}</li>
          ))}
        </ul>
      ) : null}
      <SecondaryText>
        Full canonical manifest required: {integer(run.canonicalCount)} queries. Selected:{" "}
        {integer(run.selectedCount)}; documented skips: {integer(run.skipCount)}. Coverage:{" "}
        {run.manifestStatus}. Every canonical position must be selected or have a documented skip;
        skips change the workload. Manifest hashes identify recorded SQL; coverage alone does not
        establish equivalent inputs, semantics or correctness.
      </SecondaryText>
      <Disclosure title="Per-query attempts" meta={`${run.queries.length} queries`}>
        <div className="relative overflow-x-auto">
          <table
            aria-label={`Run ${run.row.id} per-query evidence`}
            className="w-full text-left text-sm leading-6 [&_td]:p-2 [&_td]:align-top [&_th]:p-2 [&_th]:font-medium [&_th]:text-slate-300"
          >
            <thead>
              <tr>
                <th scope="col">Query / manifest</th>
                <th scope="col">Offered</th>
                <th scope="col">Completed</th>
                <th scope="col">Errors / timeouts</th>
                <th scope="col">QPS</th>
                <th scope="col">Completed latency</th>
              </tr>
            </thead>
            <tbody className="text-slate-200">
              {run.queries.map((query) => (
                <tr key={query.name} className="border-t border-border-subtle">
                  <th scope="row" className="max-w-80 break-words">
                    <span className="text-slate-50">{query.name}</span> · {query.manifestStatus}
                    {query.hash ? (
                      <div title={query.hash} className="font-mono">
                        SQL sha256 {query.hash.slice(0, 12)}…
                      </div>
                    ) : null}
                    {query.skip ? <div>{query.skip}</div> : null}
                  </th>
                  <td>{integer(query.summary.offered)}</td>
                  <td>{integer(query.summary.completed)}</td>
                  <td>
                    {integer(query.summary.errors)} / {integer(query.summary.timeouts)}
                  </td>
                  <td>{qps(query.summary.qps)}</td>
                  <td>
                    <Latency summary={query.summary} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {run.queries.length === 0 ? (
            <SecondaryText className="p-2">Per-query evidence unknown.</SecondaryText>
          ) : null}
        </div>
      </Disclosure>
      <Disclosure title="Recorded budget scope">
        <SecondaryText>{run.budget.scope ?? unknown}</SecondaryText>
        <SecondaryText>
          Shared across clients:{" "}
          {run.budget.shared === null
            ? unknown
            : run.budget.shared
              ? "Yes (recorded)"
              : "No (recorded)"}
          . Configured threads: {integer(run.budget.threads)}; engine memory:{" "}
          {gib(run.budget.engineBytes)}; operation RSS guard: {gib(run.budget.rssBytes)}.
        </SecondaryText>
        <SecondaryText>RSS scope: {run.budget.rssScope ?? unknown}</SecondaryText>
        <SecondaryText>
          Engine settings, process pools and server cgroups have different enforcement scopes.
          Readbacks do not establish whole-host resource equivalence or cache-hit behavior.
        </SecondaryText>
        <Disclosure title="Effective budget readback">
          <RecordedObject value={run.budget.effective} />
        </Disclosure>
      </Disclosure>
      <Disclosure title="Per-worker runtime & cache evidence">
        <RecordedObject value={run.workerRuntime} />
      </Disclosure>
      <Disclosure title="Recorded analytical metadata">
        <RecordedObject value={run.recorded} />
      </Disclosure>
    </>
  )
}

function groupRuns(runs: readonly AnalyticalRun[], suiteTitles: ReadonlyMap<string, string>) {
  const groups = new Map<string, AnalyticalRun[]>()
  for (const run of runs) {
    const key = `${suiteTitles.get(run.row.suite) ?? run.row.suite} · SF ${run.row.suite_scale_factor} · ${run.row.system}`
    groups.set(key, [...(groups.get(key) ?? []), run])
  }
  return [...groups].map(([title, entries]) => ({
    title,
    runs: entries.sort(
      (a, b) =>
        a.row.db.localeCompare(b.row.db) ||
        (a.clients ?? 0) - (b.clients ?? 0) ||
        a.row.id - b.row.id,
    ),
  }))
}

export function AnalyticalEvidence() {
  const { runs, loading, error } = useAnalyticalData()
  const benchmarkDefinitions = useAppStore((state) => state.benchmarkDefinitions)
  const suiteTitles = new Map(
    benchmarkDefinitions.map((definition) => [definition.id, definition.title]),
  )
  return (
    <>
      {error ? (
        <NoticeStrip tone="warning" role="alert">
          Failed to load analytical evidence: {error}
        </NoticeStrip>
      ) : null}
      <section aria-label="Analytical concurrency evidence" aria-busy={loading}>
        <PanelDisclosure
          title="Analytical concurrency evidence"
          meta="Throughput with concurrent clients · separate from the SELECT ranking"
        >
          {loading ? (
            <div role="status">
              <SecondaryText>Loading analytical measurements…</SecondaryText>
            </div>
          ) : runs.length === 0 ? (
            <BodyText>
              {error
                ? "Analytical results unavailable."
                : "No analytical runs recorded for this publication."}
            </BodyText>
          ) : (
            <>
              <div className="space-y-2">
                <LeadText>{analyticalTakeaway(runs)}</LeadText>
                <SecondaryText>
                  Engines achieved different query mixes within each window, so throughput is not a
                  ranking and no combined score is calculated. Unknown values stay unknown.
                </SecondaryText>
              </div>
              {groupRuns(runs, suiteTitles).map((group) => (
                <ChartFrame key={group.title} className="relative overflow-x-auto p-0">
                  <table
                    aria-label={`Analytical stream measurements · ${group.title}`}
                    className="w-full min-w-[30rem] text-left text-sm leading-6 [&_td]:px-3 [&_td]:py-2 [&_td]:align-middle [&_th]:px-3 [&_th]:py-2"
                  >
                    <caption className="border-b border-border-default px-3 py-2 text-left">
                      <ItemTitle>{group.title}</ItemTitle>
                    </caption>
                    <thead className="text-slate-400">
                      <tr>
                        <th scope="col" className="font-medium">
                          Engine
                        </th>
                        <th scope="col" className="text-right font-medium">
                          Clients
                        </th>
                        <th scope="col" className="text-right font-medium">
                          QPS
                        </th>
                        <th scope="col" className="font-medium">
                          Outcome
                        </th>
                        <th scope="col">
                          <span className="sr-only">Details</span>
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border-subtle text-slate-100">
                      {group.runs.map((run) => {
                        const outcome = runOutcome(run)
                        return (
                          <tr key={run.row.id} data-analytical-run={run.row.id}>
                            <th scope="row" className="font-medium">
                              {engineLabel(run)}
                            </th>
                            <td className="text-right">
                              <NumericText>{integer(run.clients)}</NumericText>
                            </td>
                            <td className="text-right" data-analytical-qps>
                              <NumericText>{qps(run.overall.qps)}</NumericText>
                            </td>
                            <td>
                              <StatusBadge tone={outcome.tone}>{outcome.label}</StatusBadge>
                            </td>
                            <td className="text-right">
                              <DetailsDialog
                                title={`Run ${run.row.id} · ${engineLabel(run)} · ${clientCount(run.clients)}`}
                                trigger="Details"
                                triggerLabel={`Run ${run.row.id} details`}
                              >
                                <RunEvidence run={run} />
                              </DetailsDialog>
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </ChartFrame>
              ))}
              <Disclosure title="How to read concurrency results">
                <BodyText>
                  Duration-based streams (analytical-streams-v1): closed-loop clients with one
                  outstanding fetch each. Every retained run is shown, including failed attempts.
                </BodyText>
                <BodyText>
                  Actual windows include drain, answer hashing and request logging; requested
                  duration limits new requests. Warmup is excluded. Overall latency reflects each
                  run&apos;s achieved query mix, which can differ across engines; inspect per-query
                  counts and latencies before comparing. These measurements are separate from the
                  resident-hot fixed SELECT ranking. Full-suite evidence requires the full canonical
                  query manifest; documented skips and filtered subsets change the workload and
                  remain listed in each run&apos;s evidence.
                </BodyText>
                <BodyText>
                  Latency uses completed samples and recorded nearest-rank summaries. Minimum n: p50
                  ≥ 5, p95 ≥ 100, p99 ≥ 1,000. Unknown (&lt;minimum n) means insufficient_samples;
                  other missing or invalid evidence stays Unknown. Thresholds do not establish
                  percentile confidence or query correctness.
                </BodyText>
              </Disclosure>
              <CaptionText>
                Each run&apos;s details hold windows, percentiles, manifests and budgets.
              </CaptionText>
            </>
          )}
        </PanelDisclosure>
      </section>
    </>
  )
}
