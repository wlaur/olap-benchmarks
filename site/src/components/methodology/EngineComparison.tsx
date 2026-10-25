import type { ComparisonFamily } from "../../lib/comparison"
import { formatDatabaseName } from "../../lib/databaseNames"
import { runPerformanceProblems, type FamilyEngineResult } from "../../lib/observations"
import { executionIssues, isCampaignExclusion } from "../../lib/queryOutcomes"
import { DetailsDialog } from "../layout/DetailsDialog"
import { Disclosure } from "../layout/Disclosure"
import { ChartFrame, StatusBadge, type StatusTone } from "../layout/Panel"
import {
  BodyText,
  CaptionText,
  ItemTitle,
  LeadText,
  MetaLabel,
  NumericText,
  SecondaryText,
} from "../Typography"
import { EngineEvidence, ValidationSummary } from "./ObservationContext"

type OutcomeKind =
  | "eligible"
  | "unimplemented"
  | "not run"
  | "smoke"
  | "blocked"
  | "incomplete"
  | "excluded"
  | "input mismatch"

interface ComparisonOutcome {
  kind: OutcomeKind
  label: string
  tone: StatusTone
}

const MEASURED_FAILURES = ["wrong_result", "timeout", "error"]
const QUERY_STATUSES = [...MEASURED_FAILURES, "unsupported", "skipped"]

function comparisonOutcome(row: FamilyEngineResult, family: ComparisonFamily): ComparisonOutcome {
  if (row.eligible) return { kind: "eligible", label: "Eligible", tone: "positive" }
  if (!row.run)
    return row.registration === "unimplemented"
      ? { kind: "unimplemented", label: "Not eligible: adapter not implemented", tone: "neutral" }
      : { kind: "not run", label: "No recorded run · execution unknown", tone: "neutral" }
  if (family.track === "smoke")
    return { kind: "smoke", label: "Smoke track: not compared", tone: "neutral" }
  const problems = runPerformanceProblems(row.run, row.annotations)
  if (problems.length > 0)
    return { kind: "blocked", label: `Not eligible: ${problems.join(", ")}`, tone: "failure" }
  const incomplete = row.queries.filter(
    (query) => family.query_names.includes(query.queryName) && !query.complete,
  )
  if (incomplete.length > 0) {
    const counts = new Map<string, number>()
    for (const query of incomplete) {
      const status =
        (isCampaignExclusion(query)
          ? "skipped by campaign"
          : QUERY_STATUSES.find((entry) =>
              query.steps.some((step) => step.resultStatus === entry),
            )) ?? (query.steps.length === 0 ? "not run" : "incomplete")
      counts.set(status, (counts.get(status) ?? 0) + 1)
    }
    const measuredFailure = [...counts.keys()].some((status) => MEASURED_FAILURES.includes(status))
    return {
      kind: "incomplete",
      label: `Not eligible: ${[...counts]
        .map(([status, count]) => `${count} ${status.replaceAll("_", " ")}`)
        .join(", ")}`,
      tone: measuredFailure ? "failure" : "warning",
    }
  }
  if (row.queries.some((query) => query.evidence === "excluded"))
    return { kind: "excluded", label: "Not eligible: comparison exclusion", tone: "warning" }
  return { kind: "input mismatch", label: "Not eligible: input mismatch", tone: "warning" }
}

function engineName(row: FamilyEngineResult, rows: readonly FamilyEngineResult[]): string {
  const name = formatDatabaseName(row.db)
  const duplicated = rows.filter((entry) => entry.db === row.db).length > 1
  return duplicated && row.run ? `${name} ${row.run.version}` : name
}

function formatRatio(score: number): string {
  return `${score.toFixed(2)}×`
}

function comparisonOutcomeSummary(
  rows: readonly FamilyEngineResult[],
  family: ComparisonFamily,
): { lead: string; qualification: string } {
  const eligible = rows
    .filter((row) => row.eligible && row.score !== null)
    .sort((a, b) => a.score! - b.score!)
  const qualification = `${eligible.length} comparable ${eligible.length === 1 ? "result" : "results"}.`
  const [best, next] = eligible
  if (family.track === "smoke")
    return {
      lead: "Execution check · no latency ranking",
      qualification,
    }
  if (!best)
    return {
      lead: "No eligible results for this workload and scale",
      qualification,
    }
  if (!next)
    return {
      lead: `${engineName(best, rows)} · only one eligible result`,
      qualification,
    }
  if (next.score === best.score)
    return {
      lead: `Joint lowest ratio: ${engineName(best, rows)} and ${engineName(next, rows)} · ${formatRatio(best.score!)}`,
      qualification,
    }
  return {
    lead: `Lowest latency ratio: ${engineName(best, rows)} · ${formatRatio(best.score!)}`,
    qualification,
  }
}

export function EngineComparison({
  rows,
  family,
  eligibleCount,
  cohortRows,
}: {
  rows: readonly FamilyEngineResult[]
  family: ComparisonFamily
  eligibleCount: number
  cohortRows?: readonly FamilyEngineResult[]
}) {
  const cohort = cohortRows ?? rows
  const summary = comparisonOutcomeSummary(cohort, family)
  const maxRatio = Math.max(...cohort.map((row) => row.score ?? 0))
  const ranks = new Map(
    cohort
      .filter((row) => row.score !== null)
      .sort((a, b) => a.score! - b.score!)
      .map((row, index) => [row.key, index + 1]),
  )
  return (
    <div className="space-y-4">
      <div className="space-y-1.5" data-comparison-lead>
        <LeadText>{summary.lead}</LeadText>
        <SecondaryText>
          {summary.qualification}
          {rows.length !== cohort.length ? ` ${rows.length} databases shown.` : ""} Geometric mean
          query-latency ratio; lower is better. Baseline: each query&apos;s fastest eligible median.
        </SecondaryText>
      </div>
      <ChartFrame className="@container p-0">
        <div
          aria-hidden="true"
          className="hidden grid-cols-[1.5rem_minmax(7rem,1fr)_minmax(5rem,1fr)_4rem_minmax(9rem,1.1fr)_4.5rem] items-center gap-3 border-b border-border-default px-4 py-2 @[48rem]:grid"
        >
          <SecondaryText>#</SecondaryText>
          <SecondaryText>Engine</SecondaryText>
          <SecondaryText>Latency ratio · lower is better</SecondaryText>
          <SecondaryText>Completed · all queries</SecondaryText>
          <SecondaryText>Outcome</SecondaryText>
          <span />
        </div>
        <div
          role="list"
          aria-label="Family engine comparison"
          className="divide-y divide-border-subtle"
        >
          {rows.map((row) => {
            const outcome = comparisonOutcome(row, family)
            const name = engineName(row, cohort)
            return (
              <div
                key={row.key}
                role="listitem"
                aria-label={`${formatDatabaseName(row.db)} comparison`}
                data-comparison-engine={row.key}
                className="flex flex-wrap items-center gap-x-3 gap-y-2 px-3 py-3 sm:px-4 @[48rem]:grid @[48rem]:grid-cols-[1.5rem_minmax(7rem,1fr)_minmax(5rem,1fr)_4rem_minmax(9rem,1.1fr)_4.5rem] @[48rem]:gap-x-3"
              >
                <NumericText className="shrink-0 text-slate-400">
                  {ranks.get(row.key) ?? "—"}
                </NumericText>
                <div className="min-w-0 flex-1 basis-[7rem]">
                  <ItemTitle className="block [overflow-wrap:anywhere]">{name}</ItemTitle>
                </div>
                <div className="ml-auto flex min-w-0 items-center justify-end gap-3 @[48rem]:ml-0">
                  {row.score === null ? (
                    <SecondaryText>
                      <span className="sr-only">No latency ratio</span>—
                    </SecondaryText>
                  ) : (
                    <>
                      <div
                        aria-hidden="true"
                        className="hidden h-2 min-w-0 flex-1 overflow-hidden rounded-full bg-surface-elevated @[48rem]:block"
                      >
                        <div
                          className="h-full rounded-full bg-slate-300"
                          style={{ width: `${(row.score / maxRatio) * 100}%` }}
                        />
                      </div>
                      <NumericText className="shrink-0 text-right text-base font-semibold @[48rem]:w-16">
                        <span className="sr-only">Latency ratio </span>
                        <span data-publication-score={row.score}>{formatRatio(row.score)}</span>
                      </NumericText>
                    </>
                  )}
                </div>
                <div className="order-last flex w-full flex-wrap items-center gap-x-3 gap-y-1 @[48rem]:contents">
                  <SecondaryText>
                    <NumericText>
                      {row.completed}/{row.total}
                    </NumericText>
                    <span className="@[48rem]:sr-only"> queries completed (all positions)</span>
                  </SecondaryText>
                  <div className="min-w-0">
                    <StatusBadge tone={outcome.tone}>{outcome.label}</StatusBadge>
                  </div>
                </div>
                <div className="shrink-0 justify-self-end">
                  <DetailsDialog
                    title={`${name} · ${family.title} evidence`}
                    trigger="Details"
                    triggerLabel={`${name} evidence details`}
                  >
                    <EngineDetails row={row} family={family} />
                  </DetailsDialog>
                </div>
              </div>
            )
          })}
        </div>
        {rows.length === 0 ? (
          <BodyText className="p-4">
            All engines are hidden. Publication scores are retained.
          </BodyText>
        ) : null}
      </ChartFrame>
      <StrictRatioMethod eligibleCount={eligibleCount} />
    </div>
  )
}

function EngineDetails({ row, family }: { row: FamilyEngineResult; family: ComparisonFamily }) {
  const campaignExclusions = row.queries.filter(isCampaignExclusion)
  const issues = executionIssues(row.queries)
  return (
    <>
      {row.annotations.length > 0 ? (
        <div className="space-y-2">
          <MetaLabel>Recorded explanation</MetaLabel>
          {row.annotations.map((annotation, index) => (
            <BodyText key={index} className="text-sm">
              {annotation.summary}{" "}
              <a
                href={annotation.evidence_url}
                target="_blank"
                rel="noreferrer"
                className="text-accent-300 underline"
              >
                Source
              </a>
            </BodyText>
          ))}
        </div>
      ) : null}
      {campaignExclusions.length > 0 ? (
        <BodyText className="text-sm">
          Skipped by campaign selection:{" "}
          {campaignExclusions.length <= 4
            ? campaignExclusions.map((query) => query.queryName).join(", ")
            : `${campaignExclusions.length} queries`}
          . These queries were not retried in this run.
        </BodyText>
      ) : null}
      {issues.length > 0 ? (
        <div className="space-y-2">
          <MetaLabel>Recorded execution reasons</MetaLabel>
          <ul className="space-y-2 text-sm leading-6 text-slate-200">
            {issues.map((issue) => (
              <li key={issue.key}>
                {issue.names.length} {issue.status}: {issue.reason}
                <Disclosure title="Affected queries" lazy>
                  <BodyText className="text-sm [overflow-wrap:anywhere]">
                    {issue.names.join(", ")}
                  </BodyText>
                </Disclosure>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-2">
        <div>
          <MetaLabel as="dt">Engine</MetaLabel>
          <dd className="mt-1 text-base text-slate-100">
            {row.run
              ? `${formatDatabaseName(row.db)} ${row.run.version} · ${row.run.driver ?? "driver unknown"} · run ${row.run.runId}`
              : row.registration === "unimplemented"
                ? "Adapter unimplemented"
                : "Execution unknown"}
          </dd>
        </div>
        <div>
          <MetaLabel as="dt">Latency ratio</MetaLabel>
          <dd className="mt-1 text-base text-slate-100">
            {row.score === null
              ? "Not eligible"
              : `${formatRatio(row.score)} · fastest median on ${row.wins}/${family.query_names.length} queries`}
          </dd>
        </div>
        <div>
          <MetaLabel as="dt">All query positions · execution</MetaLabel>
          <dd className="mt-1 text-base text-slate-100">
            {row.completed}/{row.total} queries with complete iteration evidence
            <CaptionText>
              {family.query_names.length} performance queries set the latency ratio.{" "}
              {family.coverage_query_names.length - family.query_names.length > 0
                ? `${family.coverage_query_names.length - family.query_names.length} additional coverage ${family.coverage_query_names.length - family.query_names.length === 1 ? "position is" : "positions are"} excluded from scoring.`
                : "All positions are performance queries."}
            </CaptionText>
            <CaptionText>{statusSummary(row)}</CaptionText>
          </dd>
        </div>
        <div>
          <MetaLabel as="dt">Validation evidence</MetaLabel>
          <dd className="mt-1">
            <ValidationSummary queries={row.queries} />
          </dd>
        </div>
      </dl>
      {row.reasons.length > 0 ? (
        <div>
          <MetaLabel>Eligibility reasons</MetaLabel>
          <ul className="mt-1 list-disc space-y-1 pl-5 text-sm leading-6 text-slate-200">
            {row.reasons.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
          </ul>
        </div>
      ) : null}
      <EngineEvidence row={row} performanceQueries={family.query_names} />
    </>
  )
}

function StrictRatioMethod({ eligibleCount }: { eligibleCount: number }) {
  return (
    <DetailsDialog title="Workload comparison method" trigger="How workload ratios work">
      <BodyText>
        {eligibleCount} comparable publication results set the fixed reference medians. Hiding a
        database does not change ratios.
      </BodyText>
      <BodyText>
        Each family and scale uses its full canonical query set. Eligibility requires a completed,
        non-invalidated run, every required warmup and three successful measured iterations per
        performance query, and no recorded errors, wrong results, hash conflicts or documented
        incomplete preparation. Eligible participants must have the same recorded input fingerprint.
        Partial engines stay visible. No success intersection, latency smoothing or missing-result
        penalties are used.
      </BodyText>
      <BodyText>
        The latency ratio is the geometric mean of per-query median latency divided by the fastest
        eligible publication median. Baselines include the entire eligible publication cohort at
        this system, family and scale; hiding an engine never changes them. An eligible cohort of
        one has a ratio of 1.00× and establishes no comparative advantage.
      </BodyText>
      <BodyText>
        Strict family eligibility and latency ratios remain separate from the overall 0–100 index.
        Prepared and smoke workloads are separate tracks. Coverage always retains the full
        metadata-defined set, including queries excluded from comparable performance.
      </BodyText>
    </DetailsDialog>
  )
}

function statusSummary(row: FamilyEngineResult): string {
  if (!row.run)
    return "No recorded query statuses; support cannot be inferred from missing results."
  const counts = new Map<string, number>()
  for (const query of row.queries) {
    const statuses = [...new Set(query.steps.map((step) => step.resultStatus ?? "unknown"))]
    if (statuses.length === 0) statuses.push("unobserved")
    for (const status of statuses) counts.set(status, (counts.get(status) ?? 0) + 1)
  }
  return (
    "Recorded query positions: " +
    [...counts].map(([status, count]) => `${count} ${status}`).join(" · ")
  )
}
