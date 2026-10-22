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
        className="hidden grid-cols-[3.5rem_minmax(0,1fr)_5rem_13rem_4.5rem] items-center gap-3 border-b border-border-default px-3 py-2 sm:grid"
      >
        <MetaLabel># / band</MetaLabel>
        <MetaLabel>Database</MetaLabel>
        <MetaLabel className="text-right">Index / 100</MetaLabel>
        <MetaLabel className="text-right">Usable / gaps / no run</MetaLabel>
        <MetaLabel className="text-right">Workloads</MetaLabel>
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
              <summary className="grid cursor-pointer list-none grid-cols-6 items-center gap-x-3 gap-y-2 px-3 py-2.5 outline-none hover:bg-surface-raised focus-visible:ring-2 focus-visible:ring-accent-300 focus-visible:ring-inset sm:grid-cols-[3.5rem_minmax(0,1fr)_5rem_13rem_4.5rem] [&::-webkit-details-marker]:hidden">
                <div className="col-start-1 row-start-1">
                  <NumericText className="text-slate-300">
                    <span data-overall-rank={row.rank ?? "unranked"}>{row.rank ?? "—"}</span>
                  </NumericText>
                  {row.rankRange ? (
                    <span
                      data-overall-band={row.rankRange.join("–")}
                      aria-label={`Rank band ${row.rankRange[0]} to ${row.rankRange[1]}`}
                      className="block text-[10px] text-slate-400"
                    >
                      {row.rankRange[0] === row.rankRange[1]
                        ? "stable"
                        : `${row.rankRange[0]}–${row.rankRange[1]}`}
                      {row.unrankedOmissions > 0 ? "*" : ""}
                    </span>
                  ) : null}
                </div>
                <div className="col-span-5 col-start-2 row-start-1 flex min-w-0 items-center gap-2 sm:col-span-1">
                  <ChevronDown
                    aria-hidden="true"
                    className="size-3.5 shrink-0 text-slate-500 transition-transform group-open:rotate-180"
                  />
                  <div className="min-w-0">
                    <strong className="block text-sm break-words text-slate-100">{row.db}</strong>
                    <span className="block text-[11px] break-words text-slate-400 sm:hidden">
                      {row.scoredQueries === 0
                        ? "No comparable evidence"
                        : `${row.workloadsWithData}/${familyCount} workloads`}
                    </span>
                  </div>
                </div>
                <div className="col-span-2 min-w-0 text-right sm:col-span-1">
                  <MetaLabel className="mb-1 sm:sr-only">Index / 100</MetaLabel>
                  <NumericText className="text-base font-semibold">
                    <span data-overall-score={row.score}>{row.score.toFixed(1)}</span>
                  </NumericText>
                </div>
                <div className="col-span-4 text-right sm:col-span-1">
                  <MetaLabel className="mb-1 sm:sr-only">Usable / gaps / no run</MetaLabel>
                  <div className="grid grid-cols-3 gap-1 font-mono text-xs tabular-nums sm:text-sm">
                    <span
                      data-overall-coverage={row.coverage}
                      aria-label={`Usable ${row.coverage.toFixed(1)} percent`}
                    >
                      {row.coverage.toFixed(1)}%
                    </span>
                    <span
                      data-overall-recorded-gaps={row.recordedGapCoverage}
                      aria-label={`Recorded gaps ${row.recordedGapCoverage.toFixed(1)} percent`}
                      className="text-slate-400"
                    >
                      {row.recordedGapCoverage.toFixed(1)}%
                    </span>
                    <span
                      data-overall-no-run={row.unmeasuredCoverage}
                      aria-label={`No run ${row.unmeasuredCoverage.toFixed(1)} percent`}
                      className="text-slate-500"
                    >
                      {row.unmeasuredCoverage.toFixed(1)}%
                    </span>
                  </div>
                </div>
                <div className="hidden text-right sm:block">
                  <MetaLabel className="mb-1 sm:sr-only">Workloads</MetaLabel>
                  <NumericText>
                    {row.workloadsWithData}/{familyCount}
                  </NumericText>
                </div>
              </summary>
              <div className="space-y-3 border-t border-border-subtle px-3 py-3 sm:pl-14">
                <BodyText className="text-xs leading-5">
                  {row.versions.length > 0
                    ? `Published versions: ${row.versions.join(", ")}. `
                    : ""}
                  {row.scoredQueries}/{row.totalQueries} query positions have usable results.
                  Reference medians include complete queries from partial completed runs.
                </BodyText>
                <BodyText className="text-xs leading-5">
                  {row.rankRange
                    ? `Rank sensitivity: ${row.rankRange[0]}–${row.rankRange[1]} across the full index and one-family omissions. This is a sensitivity check, not a confidence interval.`
                    : "Rank sensitivity unavailable without comparable evidence."}
                  {row.unrankedOmissions > 0
                    ? ` ${row.unrankedOmissions} family omission${row.unrankedOmissions === 1 ? " leaves" : "s leave"} no scored evidence and no rank.`
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
                          Index{" "}
                          <NumericText className="text-xs">{family.score.toFixed(1)}</NumericText>
                          {" · "}Coverage{" "}
                          <NumericText className="text-xs">
                            {family.coverage.toFixed(1)}%
                          </NumericText>
                        </p>
                        <p className="text-slate-400">
                          {family.scoredQueries}/{family.totalQueries} query positions
                        </p>
                        {family.cappedQueries > 0 ? (
                          <p className="text-slate-400">
                            {family.cappedQueries} at least 100× slower; zero index credit
                          </p>
                        ) : null}
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
        <BodyText className="p-3">
          All engines are hidden. Publication scores and ranks are retained.
        </BodyText>
      ) : null}
      <BodyText className="border-t border-border-subtle p-3 text-xs leading-5">
        Coverage uses the fixed workload weights. Usable results / recorded gaps / no run sum to
        100%. Gaps include failed runs, skips, unsupported queries and incomplete preparation; no
        run includes missing adapters. Neither describes database capability. Rank bands remove one
        family at a time; close indexes depend on the workload mix. An asterisk means an omission
        leaves no evidence and no rank. Expand a row for details.
      </BodyText>
    </ChartFrame>
  )
}
