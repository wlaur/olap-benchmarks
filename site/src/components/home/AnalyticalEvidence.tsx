import { Fragment } from "react"

import { useAnalyticalData } from "../../hooks/useAnalyticalData"
import type { AnalyticalRun, AnalyticalSummary } from "../../lib/analytical"
import { formatLatencyMs } from "../../lib/format"
import { ChartFrame, NoticeStrip, PanelCard, PanelHeader } from "../layout/Panel"
import { BodyText, MetaLabel, SectionTitle } from "../Typography"

const unknown = "Unknown"
const integer = (value: number | null) => (value === null ? unknown : value.toLocaleString("en-US"))
const seconds = (value: number | null) => (value === null ? unknown : `${value.toFixed(2)}s`)
const qps = (value: number | null) =>
  value === null ? unknown : value.toLocaleString("en-US", { maximumSignificantDigits: 4 })
const gib = (value: number | null) =>
  value === null ? unknown : `${(value / 1024 ** 3).toFixed(1)} GiB`

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
    <BodyText>{unknown}</BodyText>
  ) : (
    <pre className="max-h-64 overflow-auto text-xs break-all whitespace-pre-wrap text-slate-300">
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}

function RunEvidence({ run }: { run: AnalyticalRun }) {
  const error = [run.row.error_type, run.row.error_message]
    .filter((value) => value?.trim())
    .join(": ")
  return (
    <details className="text-xs leading-5 text-slate-300">
      <summary className="cursor-pointer">
        Run {run.row.id} · per-query, manifest &amp; budget evidence
      </summary>
      <div className="mt-3 space-y-3">
        <BodyText className="text-xs">
          Measurement: {run.startedAt ?? unknown} → {run.finishedAt ?? unknown}. Drain:{" "}
          {seconds(run.drainSeconds)}. Run finished: {run.row.finished_at ?? unknown}.
        </BodyText>
        {error ? <BodyText className="text-xs">Recorded run error: {error}</BodyText> : null}
        {run.limitations.length > 0 ? (
          <ul className="list-disc space-y-1 pl-4 text-amber-200/90">
            {run.limitations.map((limitation) => (
              <li key={limitation}>{limitation}</li>
            ))}
          </ul>
        ) : null}
        <BodyText className="text-xs">
          Full canonical manifest required: {integer(run.canonicalCount)} queries. Selected:{" "}
          {integer(run.selectedCount)}; documented skips: {integer(run.skipCount)}. Coverage:{" "}
          {run.manifestStatus}. Every canonical position must be selected or have a documented skip;
          skips change the workload. Manifest hashes identify recorded SQL; coverage alone does not
          establish equivalent inputs, semantics or correctness.
        </BodyText>
        <div className="overflow-x-auto">
          <table
            aria-label={`Run ${run.row.id} per-query evidence`}
            className="w-full text-left text-xs [&_td]:p-2 [&_td]:align-top [&_th]:p-2 [&_th]:font-medium [&_th]:text-slate-400"
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
            <tbody>
              {run.queries.map((query) => (
                <tr key={query.name} className="border-t border-border-subtle">
                  <th scope="row" className="max-w-80 break-words">
                    <span className="text-slate-100">{query.name}</span> · {query.manifestStatus}
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
            <BodyText className="p-2 text-xs">Per-query evidence unknown.</BodyText>
          ) : null}
        </div>
        <ChartFrame className="space-y-2">
          <MetaLabel>Recorded budget scope</MetaLabel>
          <BodyText className="text-xs">{run.budget.scope ?? unknown}</BodyText>
          <BodyText className="text-xs">
            Shared across clients:{" "}
            {run.budget.shared === null
              ? unknown
              : run.budget.shared
                ? "Yes (recorded)"
                : "No (recorded)"}
            . Configured threads: {integer(run.budget.threads)}; engine memory:{" "}
            {gib(run.budget.engineBytes)}; operation RSS guard: {gib(run.budget.rssBytes)}.
          </BodyText>
          <BodyText className="text-xs">RSS scope: {run.budget.rssScope ?? unknown}</BodyText>
          <BodyText className="text-xs">
            Engine settings, process pools and server cgroups have different enforcement scopes.
            Readbacks do not establish whole-host resource equivalence or cache-hit behavior.
          </BodyText>
          <details>
            <summary className="cursor-pointer">Effective budget readback</summary>
            <RecordedObject value={run.budget.effective} />
          </details>
          <details>
            <summary className="cursor-pointer">Per-worker runtime &amp; cache evidence</summary>
            <RecordedObject value={run.workerRuntime} />
          </details>
        </ChartFrame>
        <details>
          <summary className="cursor-pointer">Recorded analytical metadata</summary>
          <RecordedObject value={run.recorded} />
        </details>
      </div>
    </details>
  )
}

export function AnalyticalEvidence() {
  const { runs, loading, error } = useAnalyticalData()
  return (
    <>
      {error ? (
        <NoticeStrip tone="warning">Failed to load analytical evidence: {error}</NoticeStrip>
      ) : null}
      <section
        hidden={runs.length === 0}
        aria-label="Analytical concurrency evidence"
        aria-busy={loading}
      >
        <PanelCard className="space-y-3 p-4 sm:p-5">
          <PanelHeader>
            <div className="space-y-1">
              <MetaLabel>Duration-based streams · analytical-streams-v1</MetaLabel>
              <SectionTitle as="h2">Analytical concurrency evidence</SectionTitle>
              <BodyText>
                Closed-loop clients with one outstanding fetch each. Every retained run is shown,
                including failed attempts.
              </BodyText>
            </div>
          </PanelHeader>
          <BodyText className="text-xs leading-5">
            Actual windows include drain, answer hashing and request logging; requested duration
            limits new requests. Warmup is excluded. Overall latency reflects each run’s achieved
            query mix, which can differ across engines; inspect per-query counts and latencies
            before comparing. No combined score is calculated. These measurements are separate from
            the resident-hot fixed SELECT ranking. Full-suite evidence requires the full canonical
            query manifest; documented skips and filtered subsets change the workload and remain
            listed in each run’s evidence.
          </BodyText>
          <BodyText className="text-xs leading-5">
            Latency uses completed samples and recorded nearest-rank summaries. Minimum n: p50 ≥ 5,
            p95 ≥ 100, p99 ≥ 1,000. Unknown (&lt;minimum n) means insufficient_samples; other
            missing or invalid evidence stays Unknown. Thresholds do not establish percentile
            confidence or query correctness.
          </BodyText>
          <ChartFrame className="overflow-x-auto p-0">
            {loading ? (
              <div role="status" className="p-3">
                <BodyText>Loading analytical measurements…</BodyText>
              </div>
            ) : (
              <table
                aria-label="Analytical stream measurements"
                className="w-full text-left text-xs [&_td]:px-3 [&_td]:py-2 [&_td]:align-top [&_th]:px-3 [&_th]:py-2 [&_th]:font-medium [&_th]:text-slate-400"
              >
                <thead>
                  <tr>
                    <th scope="col">Suite / SF / system</th>
                    <th scope="col">Engine / version</th>
                    <th scope="col">Clients</th>
                    <th scope="col">Run</th>
                    <th scope="col">
                      Window
                      <br />
                      actual / requested
                    </th>
                    <th scope="col">Completed</th>
                    <th scope="col">Errors / timeouts</th>
                    <th scope="col">QPS</th>
                    <th scope="col">Mixed-query latency</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <Fragment key={run.row.id}>
                      <tr
                        data-analytical-run={run.row.id}
                        className="border-t border-border-default"
                      >
                        <th scope="row">
                          <span className="text-slate-100">{run.row.suite}</span>
                          <div>
                            SF {run.row.suite_scale_factor} · {run.row.system}
                          </div>
                        </th>
                        <td>
                          {run.row.db}
                          <div className="text-slate-400">
                            {run.row.db_version || unknown}
                            {run.row.db_driver ? ` · ${run.row.db_driver}` : ""}
                          </div>
                        </td>
                        <td>{integer(run.clients)}</td>
                        <td>
                          {run.row.status || unknown}
                          <div className="text-slate-400">#{run.row.id}</div>
                        </td>
                        <td className="whitespace-nowrap">
                          {seconds(run.elapsedSeconds)} / {seconds(run.requestedSeconds)}
                        </td>
                        <td>
                          {integer(run.overall.completed)}
                          <div className="text-slate-400">
                            of {integer(run.overall.offered)} offered
                          </div>
                        </td>
                        <td>
                          {integer(run.overall.errors)} / {integer(run.overall.timeouts)}
                        </td>
                        <td>{qps(run.overall.qps)}</td>
                        <td>
                          <Latency summary={run.overall} />
                        </td>
                      </tr>
                      <tr>
                        <td colSpan={9} className="border-t border-border-subtle">
                          <RunEvidence run={run} />
                        </td>
                      </tr>
                    </Fragment>
                  ))}
                </tbody>
              </table>
            )}
          </ChartFrame>
        </PanelCard>
      </section>
    </>
  )
}
