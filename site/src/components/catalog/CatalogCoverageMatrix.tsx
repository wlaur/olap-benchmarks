import {
  catalogMissingAvailability,
  catalogRunAvailability,
  catalogTrackLabel,
  type CatalogSuiteSummary,
} from "../../lib/catalog"
import { formatDatabaseName } from "../../lib/databaseNames"
import { BodyText } from "../Typography"

export function CatalogCoverageMatrix({ summary }: { summary: CatalogSuiteSummary }) {
  if (summary.scales.length === 0) return <BodyText>No published scales or recorded runs.</BodyText>

  return (
    <>
      <BodyText className="max-w-prose text-sm">
        Each entry is a recorded run within one track. Query counts describe complete execution
        evidence within that track’s coverage set, including queries excluded from performance
        comparisons. Run and preparation blockers do not erase completed query evidence.
      </BodyText>
      <div
        role="region"
        aria-label="Coverage by database and scale"
        tabIndex={0}
        className="panel-scrollbar overflow-x-auto rounded-md outline-none focus-visible:ring-2 focus-visible:ring-accent-300 focus-visible:ring-inset"
      >
        <table className="w-full min-w-[38rem] border-collapse text-left text-sm">
          <caption className="sr-only">
            Recorded workload coverage by database, scale and system
          </caption>
          <thead>
            <tr>
              <th
                scope="col"
                className="border-b border-border-default px-3 py-3 font-semibold text-slate-300"
              >
                Database
              </th>
              {summary.scales.map((scale) => (
                <th
                  key={scale}
                  scope="col"
                  className="border-b border-border-default px-3 py-3 font-semibold text-slate-300"
                >
                  SF {scale}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {summary.databases.map((database) => (
              <tr key={database.database}>
                <th
                  scope="row"
                  className="border-b border-border-subtle px-3 py-4 align-top font-semibold text-slate-100"
                >
                  {formatDatabaseName(database.database)}
                </th>
                {summary.scales.map((scale) => (
                  <td
                    key={scale}
                    className="min-w-64 border-b border-border-subtle px-3 py-4 align-top text-slate-300"
                  >
                    <div className="space-y-4">
                      {database.families
                        .filter(({ family }) => family.scale_factors.includes(scale))
                        .map(({ family, runs }) => {
                          const scopedRuns = runs.filter((run) => run.scale === scale)
                          return (
                            <div
                              key={family.id}
                              data-catalog-family={family.id}
                              className="space-y-2"
                            >
                              <p className="font-semibold text-slate-100">
                                {catalogTrackLabel(family)}
                              </p>
                              {scopedRuns.length > 0 ? (
                                <ul className="space-y-4">
                                  {scopedRuns.map((run) => (
                                    <li
                                      key={run.runId}
                                      className="space-y-1"
                                      data-catalog-run={run.runId}
                                    >
                                      <p className="font-medium text-slate-100">
                                        {catalogRunAvailability(run)}
                                      </p>
                                      {run.queryOutcomes.length > 0 ? (
                                        <p>{run.queryOutcomes.join(" · ")}</p>
                                      ) : null}
                                      {run.comparisonBlockers.length > 0 ? (
                                        <p>
                                          Comparison blockers: {run.comparisonBlockers.join(" · ")}
                                        </p>
                                      ) : null}
                                      <p className="break-words text-slate-400">
                                        {run.system} · {run.version} · run {run.runId}
                                      </p>
                                    </li>
                                  ))}
                                </ul>
                              ) : (
                                catalogMissingAvailability(database, scale)
                              )}
                            </div>
                          )
                        })}
                      {!database.families.some(({ family }) => family.scale_factors.includes(scale))
                        ? "No published track at this scale"
                        : null}
                    </div>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}
