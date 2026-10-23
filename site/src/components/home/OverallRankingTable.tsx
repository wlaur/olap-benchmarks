import { ChevronDown } from "lucide-react"

import type { OverallEngineResult } from "../../lib/overall"
import { QuietButton } from "../controls/Control"
import { ChartFrame } from "../layout/Panel"
import { BodyText, MetaLabel, NumericText } from "../Typography"

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
      <div
        aria-hidden="true"
        className="grid grid-cols-[1.5rem_minmax(0,1fr)_5rem] items-center gap-3 border-b border-border-default px-3 py-2 sm:grid-cols-[2rem_minmax(0,1fr)_12rem] sm:px-4"
      >
        <MetaLabel>#</MetaLabel>
        <MetaLabel>Database</MetaLabel>
        <MetaLabel className="text-right tracking-normal whitespace-nowrap">Score / 100</MetaLabel>
      </div>
      <div
        role="list"
        aria-label="Overall database ranking"
        className="divide-y divide-border-subtle"
      >
        {rows.map((row) => (
          <div
            key={row.db}
            role="listitem"
            aria-label={`${row.db} overall ranking`}
            data-overall-engine={row.db}
          >
            <details className="group">
              <summary className="grid cursor-pointer list-none grid-cols-[1.5rem_minmax(0,1fr)_5rem] items-center gap-3 px-3 py-3 outline-none hover:bg-surface-raised focus-visible:ring-2 focus-visible:ring-accent-300 focus-visible:ring-inset sm:grid-cols-[2rem_minmax(0,1fr)_12rem] sm:px-4 [&::-webkit-details-marker]:hidden">
                <NumericText className="text-slate-400">
                  <span className="sr-only">Rank </span>
                  <span data-overall-rank={row.rank ?? "unranked"}>{row.rank ?? "—"}</span>
                </NumericText>
                <div className="flex min-w-0 items-center gap-2">
                  <ChevronDown
                    aria-hidden="true"
                    className="size-3.5 shrink-0 text-slate-500 transition-transform group-open:rotate-180"
                  />
                  <div className="min-w-0 sm:flex sm:flex-wrap sm:items-baseline sm:gap-x-3">
                    <strong className="block text-sm break-words text-slate-100">{row.db}</strong>
                    <span className="block text-[11px] text-slate-400">
                      {row.scoredQueries === 0
                        ? "No comparable results"
                        : `${row.scoredQueries}/${row.totalQueries} queries scored`}
                    </span>
                  </div>
                </div>
                <div className="flex items-center justify-end gap-3">
                  <div
                    aria-hidden="true"
                    className="hidden h-1.5 flex-1 overflow-hidden rounded-full bg-surface-elevated sm:block"
                  >
                    <div
                      className="h-full rounded-full bg-slate-400"
                      style={{ width: `${row.score}%` }}
                    />
                  </div>
                  <NumericText className="w-12 text-right text-base font-semibold">
                    <span className="sr-only">Score </span>
                    <span data-overall-score={row.score}>{row.score.toFixed(1)}</span>
                    <span className="sr-only"> out of 100</span>
                  </NumericText>
                </div>
              </summary>
              <div className="space-y-3 border-t border-border-subtle px-3 py-3 sm:pr-4 sm:pl-16">
                <BodyText className="text-xs leading-5">
                  {row.versions.length > 0
                    ? `Published versions: ${row.versions.join(", ")}. `
                    : ""}
                  {row.workloadsWithData}/{familyCount} workloads have usable results. Reference
                  medians include complete queries from partial completed runs.
                </BodyText>
                <BodyText className="text-xs leading-5">
                  Weighted coverage: usable{" "}
                  <span data-overall-coverage={row.coverage}>{row.coverage.toFixed(1)}%</span>
                  {" · "}recorded gaps{" "}
                  <span data-overall-recorded-gaps={row.recordedGapCoverage}>
                    {row.recordedGapCoverage.toFixed(1)}%
                  </span>
                  {" · "}no run{" "}
                  <span data-overall-no-run={row.unmeasuredCoverage}>
                    {row.unmeasuredCoverage.toFixed(1)}%
                  </span>
                  . These describe evidence in this publication, not database capability.
                </BodyText>
                <BodyText className="text-xs leading-5">
                  {row.rankRange
                    ? `Rank sensitivity: ${row.rankRange[0]}–${row.rankRange[1]} across the full score and one-workload omissions. This is not a confidence interval.`
                    : "Rank sensitivity unavailable without comparable results."}
                  {row.unrankedOmissions > 0
                    ? ` ${row.unrankedOmissions} workload omission${row.unrankedOmissions === 1 ? " leaves" : "s leave"} no scored results and no rank.`
                    : ""}
                </BodyText>
                <ul
                  aria-label={`${row.db} workload contributions`}
                  className="divide-y divide-border-subtle"
                >
                  {row.families.map((family) => (
                    <li
                      key={family.id}
                      data-overall-family={family.id}
                      className="grid min-w-0 gap-2 py-3 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,1.5fr)]"
                    >
                      <div className="min-w-0">
                        <QuietButton
                          size="xs"
                          className="max-w-full text-left whitespace-normal"
                          aria-label={`Compare ${family.title} by workload`}
                          onClick={() => onSelectFamily(family.id)}
                        >
                          {family.title}
                        </QuietButton>
                      </div>
                      <div className="text-xs leading-5 text-slate-300">
                        <p>
                          Score{" "}
                          <NumericText className="text-xs">{family.score.toFixed(1)}</NumericText>
                          {" / 100 · "}
                          {family.scoredQueries}/{family.totalQueries} queries scored
                        </p>
                      </div>
                      <div className="min-w-0 text-xs leading-5 break-words text-slate-400">
                        <p>
                          {Object.entries(family.gaps).length > 0
                            ? Object.entries(family.gaps)
                                .map(([reason, count]) => `${reason}: ${count}`)
                                .join(" · ")
                            : "No evidence gaps"}
                        </p>
                        <p>
                          {family.recordedRunIds.length > 0
                            ? `Recorded run IDs: ${family.recordedRunIds.join(", ")}`
                            : "No recorded run IDs"}
                        </p>
                        <p>
                          {family.runIds.length > 0
                            ? `Scored run IDs: ${family.runIds.join(", ")}`
                            : "No scored run IDs"}
                        </p>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            </details>
          </div>
        ))}
      </div>
      {rows.length === 0 ? (
        <BodyText className="p-3">All engines are hidden. Scores and ranks are retained.</BodyText>
      ) : null}
    </ChartFrame>
  )
}
