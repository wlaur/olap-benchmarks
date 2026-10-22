import type { ComparisonFamily } from "../../lib/comparison"
import type { FamilyEngineResult } from "../../lib/observations"
import { ChartFrame, PanelHeader } from "../layout/Panel"
import { BodyText, MetaLabel, NumericText } from "../Typography"
import { EngineEvidence, ValidationSummary } from "./ObservationContext"

export function EngineComparison({
  rows,
  family,
  eligibleCount,
}: {
  rows: readonly FamilyEngineResult[]
  family: ComparisonFamily
  eligibleCount: number
}) {
  return (
    <div className="space-y-3" role="list" aria-label="Family engine comparison">
      <BodyText className="text-xs">
        {eligibleCount} eligible publication {eligibleCount === 1 ? "variant" : "variants"} · lower
        ratio is faster · scores stay fixed when engines are hidden.
      </BodyText>
      {rows.map((row) => (
        <ChartFrame key={row.key} className="space-y-3">
          <div role="listitem" aria-label={`${row.db} comparison`} data-comparison-engine={row.key}>
            <PanelHeader className="flex-wrap items-baseline">
              <strong className="text-sm text-slate-100">{row.label}</strong>
              <span className="text-xs break-words text-slate-400">
                {row.run
                  ? `${row.run.version} · ${row.run.driver ?? "driver unknown"} · run ${row.run.runId}`
                  : row.registration === "unimplemented"
                    ? "Adapter unimplemented"
                    : "Execution unknown"}
              </span>
            </PanelHeader>
            <div className="mt-3 grid min-w-0 gap-3 md:grid-cols-3">
              <div>
                <MetaLabel>Eligible performance</MetaLabel>
                <NumericText className="mt-1 block text-lg">
                  {row.score === null ? (
                    "Not eligible"
                  ) : (
                    <span data-publication-score={row.score}>{row.score.toFixed(2)}×</span>
                  )}
                </NumericText>
                <p className="mt-1 text-xs leading-5 text-slate-400">
                  {row.eligible
                    ? `${family.query_names.length}/${family.query_names.length} fixed queries · ${row.wins} fastest medians`
                    : row.reasons.join("; ")}
                </p>
              </div>
              <div>
                <MetaLabel>Execution coverage</MetaLabel>
                <NumericText className="mt-1 block text-lg">
                  {row.completed}/{row.total}
                </NumericText>
                <p className="mt-1 text-xs leading-5 text-slate-400">
                  Complete iteration evidence / canonical coverage set
                </p>
                <p className="mt-1 text-xs text-slate-500">{statusSummary(row)}</p>
              </div>
              <div>
                <MetaLabel>Validation evidence</MetaLabel>
                <div className="mt-1">
                  <ValidationSummary queries={row.queries} />
                </div>
              </div>
            </div>
            <div className="mt-3 border-t border-border-subtle pt-2">
              <EngineEvidence row={row} performanceQueries={family.query_names} />
            </div>
          </div>
        </ChartFrame>
      ))}
      {rows.length === 0 ? (
        <BodyText>All engines are hidden. Publication scores are retained.</BodyText>
      ) : null}
    </div>
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
