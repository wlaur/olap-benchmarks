import { Check } from "lucide-react"

import type { CatalogSuiteSummary } from "../../lib/catalog"
import { Skeleton } from "../Skeleton"

export function CatalogCoverageMatrix({
  summary,
  loading,
}: {
  summary: CatalogSuiteSummary | null
  loading: boolean
}) {
  if (loading || summary === null) {
    return (
      <div className="space-y-2 py-2">
        {Array.from({ length: 6 }, (_, index) => (
          <Skeleton key={index} className="h-9 w-full" />
        ))}
      </div>
    )
  }

  return (
    <>
      <div className="divide-y divide-border-subtle sm:hidden" role="list" aria-label="Coverage">
        {summary.databases.map((database) => (
          <div
            key={database.database}
            role="listitem"
            aria-label={`${formatDatabaseName(database.database)} coverage`}
            className="px-1 py-3"
          >
            <div className="flex min-w-0 items-baseline justify-between gap-3">
              <span className="truncate text-xs font-semibold text-slate-200">
                {formatDatabaseName(database.database)}
              </span>
              <span
                className="max-w-[55%] truncate text-right text-xs text-slate-500"
                title={database.versions.join(", ")}
              >
                {database.versions.length > 0 ? database.versions.join(", ") : "No results"}
              </span>
            </div>
            <div className="mt-2 flex flex-wrap gap-2">
              {summary.scales.map((scale) => {
                const systems = database.systemsByScale.get(scale) ?? []
                return (
                  <span
                    key={scale}
                    className="inline-flex items-center gap-1.5 rounded-md border border-border-subtle bg-surface-inset px-2 py-1 text-[0.7rem] text-slate-400"
                    title={systems.join(", ")}
                  >
                    <strong className="font-semibold text-slate-300">SF {scale}</strong>
                    {systems.length > 0 ? (
                      <>
                        <Check className="h-3 w-3 text-emerald-300" strokeWidth={2} />
                        {systems.length}
                      </>
                    ) : (
                      "No result"
                    )}
                  </span>
                )
              })}
            </div>
          </div>
        ))}
      </div>

      <div className="panel-scrollbar hidden overflow-x-auto sm:block">
        <table className="w-full min-w-[38rem] border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr>
              <th className="border-b border-border-default px-3 py-2 font-semibold text-slate-400">
                Database
              </th>
              <th className="border-b border-border-default px-3 py-2 font-semibold text-slate-400">
                Versions
              </th>
              {summary.scales.map((scale) => (
                <th
                  key={scale}
                  className="border-b border-border-default px-3 py-2 text-center font-semibold text-slate-400"
                >
                  SF {scale}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {summary.databases.map((database) => (
              <tr key={database.database} className="group">
                <th className="border-b border-border-subtle px-3 py-2.5 font-semibold text-slate-200 group-last:border-b-0">
                  {formatDatabaseName(database.database)}
                </th>
                <td className="max-w-56 border-b border-border-subtle px-3 py-2.5 text-slate-500 group-last:border-b-0">
                  <span className="block truncate" title={database.versions.join(", ")}>
                    {database.versions.length > 0 ? database.versions.join(", ") : "No results"}
                  </span>
                </td>
                {summary.scales.map((scale) => {
                  const systems = database.systemsByScale.get(scale) ?? []
                  return (
                    <td
                      key={scale}
                      className="border-b border-border-subtle px-3 py-2.5 text-center group-last:border-b-0"
                      title={systems.join(", ")}
                    >
                      {systems.length > 0 ? (
                        <span className="inline-flex items-center gap-1.5 font-medium text-slate-300">
                          <Check className="h-3.5 w-3.5 text-emerald-300" strokeWidth={2} />
                          {systems.length} {systems.length === 1 ? "system" : "systems"}
                        </span>
                      ) : (
                        <span className="text-slate-700">-</span>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

function formatDatabaseName(database: string) {
  const labels: Record<string, string> = {
    clickhouse: "ClickHouse",
    datafusion: "DataFusion",
    duckdb: "DuckDB",
    monetdb: "MonetDB",
    postgres: "PostgreSQL",
    questdb: "QuestDB",
    starrocks: "StarRocks",
    timescaledb: "TimescaleDB",
  }
  return labels[database] ?? database
}
