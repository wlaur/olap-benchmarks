import { ChevronDown } from "lucide-react"

import { formatDatabaseName } from "../../lib/databaseNames"
import type { OverallEngineResult } from "../../lib/overall"
import { QuietButton } from "../controls/Control"
import { Disclosure } from "../layout/Disclosure"
import { ChartFrame } from "../layout/Panel"
import { CaptionText, ItemTitle, MetaLabel, NumericText, SecondaryText } from "../Typography"

const GRID =
  "sm:grid sm:grid-cols-[2rem_minmax(8rem,12rem)_minmax(0,1fr)_4.5rem_minmax(8rem,10rem)_1.25rem] sm:items-center sm:gap-x-3"

const GAP_GROUPS = [
  {
    label: "Measured failures",
    match: (reason: string) =>
      ["timeout", "error", "wrong result", "Answer conflict"].includes(reason),
  },
  {
    label: "Unsupported or skipped",
    match: (reason: string) => ["unsupported", "skipped", "Comparison excluded"].includes(reason),
  },
  {
    label: "Not run",
    match: (reason: string) => ["Not measured", "Adapter unimplemented"].includes(reason),
  },
  { label: "Run failed, blocked or incomplete", match: () => true },
] as const

export function groupGaps(gaps: Readonly<Record<string, number>>) {
  const groups = new Map<string, [string, number][]>()
  for (const [reason, count] of Object.entries(gaps)) {
    const label = GAP_GROUPS.find((group) => group.match(reason))!.label
    groups.set(label, [...(groups.get(label) ?? []), [reason, count]])
  }
  return GAP_GROUPS.flatMap(({ label }) => {
    const reasons = groups.get(label)
    return reasons ? [{ label, reasons }] : []
  })
}

export function CoverageMeter({ row }: { row: OverallEngineResult }) {
  return (
    <div
      aria-hidden="true"
      className="flex h-2 w-full overflow-hidden rounded-full bg-surface-elevated"
    >
      <div className="h-full bg-sky-300" style={{ width: `${row.coverage}%` }} />
      <div className="h-full bg-amber-300" style={{ width: `${row.recordedGapCoverage}%` }} />
    </div>
  )
}

export function CoverageLegend() {
  return (
    <CaptionText as="div" className="flex flex-wrap items-center gap-x-4 gap-y-1">
      <span className="inline-flex items-center gap-1.5">
        <span aria-hidden="true" className="size-2.5 rounded-full bg-sky-300" />
        Usable
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span aria-hidden="true" className="size-2.5 rounded-full bg-amber-300" />
        Run, no usable result
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span
          aria-hidden="true"
          className="size-2.5 rounded-full bg-surface-elevated ring-1 ring-border-strong"
        />
        Not run
      </span>
    </CaptionText>
  )
}

export function OverallRankingTable({
  rows,
  familyCount,
  onSelectFamily,
}: {
  rows: readonly OverallEngineResult[]
  familyCount: number
  onSelectFamily: (familyId: string) => void
}) {
  return (
    <ChartFrame className="p-0">
      <div aria-hidden="true" className={`hidden ${GRID} border-b border-border-default px-4 py-2`}>
        <MetaLabel>#</MetaLabel>
        <MetaLabel>Database</MetaLabel>
        <MetaLabel>Score /100</MetaLabel>
        <MetaLabel className="text-right">Score</MetaLabel>
        <MetaLabel>Weighted coverage</MetaLabel>
        <span />
      </div>
      <div
        role="list"
        aria-label="Overall database ranking"
        className="divide-y divide-border-subtle"
      >
        {rows.map((row) => {
          const name = formatDatabaseName(row.db)
          const coverage = (
            <>
              <span data-overall-weighted-coverage={row.coverage}>{row.coverage.toFixed(0)}%</span>
              <span className="sm:sr-only"> weighted coverage</span>
            </>
          )
          return (
            <div
              key={row.db}
              role="listitem"
              aria-label={`${name} overall ranking`}
              data-overall-engine={row.db}
            >
              <details className="group">
                <summary
                  className={`flex min-h-14 cursor-pointer list-none flex-wrap items-center gap-x-3 gap-y-1.5 px-3 py-2.5 outline-none hover:bg-surface-raised focus-visible:ring-2 focus-visible:ring-accent-300/60 focus-visible:ring-inset sm:px-4 ${GRID} [&::-webkit-details-marker]:hidden`}
                >
                  <div className="flex min-w-0 flex-[1_1_9rem] items-baseline gap-3 sm:contents">
                    <NumericText className="shrink-0 text-base text-slate-300">
                      <span className="sr-only">Rank </span>
                      <span data-overall-rank={row.rank ?? "unranked"}>{row.rank ?? "—"}</span>
                    </NumericText>
                    <ItemTitle className="min-w-0 [overflow-wrap:anywhere] sm:[overflow-wrap:break-word]">
                      {name}
                    </ItemTitle>
                  </div>
                  <div
                    aria-hidden="true"
                    className="hidden h-2.5 overflow-hidden rounded-full bg-surface-elevated sm:block"
                  >
                    <div
                      className="h-full rounded-full bg-slate-200"
                      style={{ width: `${row.score}%` }}
                    />
                  </div>
                  <NumericText className="ml-auto shrink-0 text-right text-lg font-semibold sm:ml-0">
                    <span className="sr-only">Score </span>
                    <span data-overall-score={row.score}>{row.score.toFixed(1)}</span>
                    <span className="text-sm font-normal text-slate-400 sm:sr-only">
                      <span className="sr-only"> out of 100</span>
                      <span aria-hidden="true">/100</span>
                    </span>
                  </NumericText>
                  <div
                    data-overall-coverage-summary
                    className="order-last flex min-w-0 basis-full flex-wrap items-center gap-x-2 gap-y-1 sm:order-none sm:flex-nowrap"
                  >
                    <SecondaryText as="span" className="min-w-0 sm:w-10 sm:shrink-0 sm:text-right">
                      {coverage}
                    </SecondaryText>
                    <div className="min-w-16 flex-1">
                      <CoverageMeter row={row} />
                    </div>
                  </div>
                  <ChevronDown
                    aria-hidden="true"
                    className="size-4 shrink-0 text-slate-400 transition-transform group-open:rotate-180 sm:justify-self-end"
                  />
                </summary>
                <EngineDetails
                  row={row}
                  familyCount={familyCount}
                  onSelectFamily={onSelectFamily}
                />
              </details>
            </div>
          )
        })}
      </div>
      {rows.length === 0 ? (
        <SecondaryText className="p-4">No databases have overall results.</SecondaryText>
      ) : null}
    </ChartFrame>
  )
}

function EngineDetails({
  row,
  familyCount,
  onSelectFamily,
}: {
  row: OverallEngineResult
  familyCount: number
  onSelectFamily: (familyId: string) => void
}) {
  const name = formatDatabaseName(row.db)
  return (
    <div className="space-y-4 border-t border-border-subtle bg-surface-primary/40 px-3 py-4 sm:px-4 sm:pl-16">
      <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-4">
        <div>
          <MetaLabel as="dt">Weighted coverage</MetaLabel>
          <dd className="mt-1 space-y-0.5 text-sm leading-6 text-slate-200">
            <p>
              Usable <span data-overall-coverage={row.coverage}>{row.coverage.toFixed(1)}%</span>
            </p>
            <p>
              Run, no usable result{" "}
              <span data-overall-recorded-gaps={row.recordedGapCoverage}>
                {row.recordedGapCoverage.toFixed(1)}%
              </span>
            </p>
            <p>
              Not run{" "}
              <span data-overall-no-run={row.unmeasuredCoverage}>
                {row.unmeasuredCoverage.toFixed(1)}%
              </span>
            </p>
          </dd>
        </div>
        <div>
          <MetaLabel as="dt">Queries scored (unweighted)</MetaLabel>
          <dd className="mt-1 text-sm leading-6 text-slate-200" data-overall-queries>
            {row.scoredQueries} of {row.totalQueries}
          </dd>
        </div>
        <div>
          <MetaLabel as="dt">Workloads with usable results</MetaLabel>
          <dd className="mt-1 text-sm leading-6 text-slate-200">
            {row.workloadsWithData} of {familyCount}
          </dd>
        </div>
        <div>
          <MetaLabel as="dt">Rank sensitivity</MetaLabel>
          <dd className="mt-1 text-sm leading-6 text-slate-200">
            {row.rankRange
              ? `Ranks ${row.rankRange[0]}–${row.rankRange[1]} when any one workload is omitted (not a confidence interval).`
              : "Unavailable without comparable results."}
            {row.unrankedOmissions > 0
              ? ` ${row.unrankedOmissions} workload omission${row.unrankedOmissions === 1 ? " leaves" : "s leave"} no scored results and no rank.`
              : ""}
          </dd>
        </div>
      </dl>
      <CaptionText>
        Coverage describes evidence in this publication, not database capability. Reference medians
        include complete queries from partial completed runs.
        {row.versions.length > 0 ? ` Published versions: ${row.versions.join(", ")}.` : ""}
      </CaptionText>
      <div className="relative overflow-x-auto">
        <table
          aria-label={`${name} workload contributions`}
          className="w-full min-w-[32rem] text-left text-sm leading-6 [&_td]:py-2 [&_td]:pr-3 [&_td]:align-top [&_th]:py-2 [&_th]:pr-3"
        >
          <thead className="text-slate-400">
            <tr className="border-b border-border-subtle">
              <th scope="col" className="font-medium">
                Workload
              </th>
              <th scope="col" className="text-right font-medium">
                Score
              </th>
              <th scope="col" className="text-right font-medium">
                Scored
              </th>
              <th scope="col" className="font-medium">
                Gaps
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle text-slate-200">
            {row.families.map((family) => (
              <tr key={family.id} data-overall-family={family.id}>
                <th scope="row" className="font-normal">
                  <QuietButton
                    size="xs"
                    className="text-left whitespace-normal"
                    aria-label={`Compare ${family.title} by workload`}
                    onClick={() => onSelectFamily(family.id)}
                  >
                    {family.title}
                  </QuietButton>
                </th>
                <td className="text-right">
                  <NumericText>{family.score.toFixed(1)}</NumericText>
                </td>
                <td className="text-right whitespace-nowrap">
                  {family.scoredQueries}/{family.totalQueries}
                </td>
                <td className="text-slate-300">
                  {Object.keys(family.gaps).length > 0 ? (
                    <ul className="space-y-1">
                      {groupGaps(family.gaps).map((group) => (
                        <li key={group.label} data-overall-gap-group={group.label}>
                          <span className="text-slate-100">{group.label}</span>:{" "}
                          {group.reasons.map(([reason, count]) => `${reason} ${count}`).join(" · ")}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    "None"
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Disclosure title="Run IDs by workload" lazy={false}>
        <ul className="space-y-1 text-sm leading-6 text-slate-300">
          {row.families.map((family) => (
            <li key={family.id}>
              <span className="text-slate-100">{family.title}</span>:{" "}
              {family.runIds.length > 0
                ? `Scored run IDs ${family.runIds.join(", ")}`
                : "No scored run IDs"}
              {family.recordedRunIds.length > 0
                ? ` · recorded run IDs ${family.recordedRunIds.join(", ")}`
                : " · no recorded run IDs"}
            </li>
          ))}
        </ul>
      </Disclosure>
    </div>
  )
}
