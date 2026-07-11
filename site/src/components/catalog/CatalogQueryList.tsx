import { Code2, Search } from "lucide-react"
import { useEffect, useMemo, useRef, useState } from "react"

import { getSqlDialects } from "../../lib/catalog"
import { cn } from "../../lib/cn"
import type { QuerySqlEntry } from "../../lib/types"
import { SegmentedButton } from "../controls/Control"
import { ChartFrame } from "../layout/Panel"
import { Skeleton } from "../Skeleton"
import { SqlCodeView } from "../SqlCodeView"
import { MetaLabel } from "../Typography"

export function CatalogQueryList({
  queryNames,
  selectedQuery,
  selectedDialect,
  sqlEntry,
  loading,
  onSelectQuery,
  onSelectDialect,
}: {
  queryNames: readonly string[]
  selectedQuery: string
  selectedDialect: string
  sqlEntry: QuerySqlEntry | undefined
  loading: boolean
  onSelectQuery: (query: string) => void
  onSelectDialect: (dialect: string) => void
}) {
  const [filter, setFilter] = useState("")
  const queryListRef = useRef<HTMLDivElement>(null)
  const selectedQueryRef = useRef<HTMLButtonElement>(null)
  const filteredQueries = useMemo(() => {
    const normalizedFilter = filter.trim().toLowerCase()
    if (!normalizedFilter) return queryNames
    return queryNames.filter((queryName) => queryName.toLowerCase().includes(normalizedFilter))
  }, [filter, queryNames])
  const dialects = getSqlDialects(sqlEntry)
  const selectedSql = resolveSql(sqlEntry, selectedDialect)

  useEffect(() => {
    const list = queryListRef.current
    const selected = selectedQueryRef.current
    if (!list || !selected) return
    const centeredTop =
      selected.offsetTop - list.offsetTop - (list.clientHeight - selected.clientHeight) / 2
    list.scrollTop = Math.max(0, centeredTop)
  }, [filteredQueries, selectedQuery])

  if (loading) {
    return (
      <div className="grid gap-4 lg:grid-cols-[minmax(14rem,18rem)_minmax(0,1fr)]">
        <Skeleton className="h-80 w-full rounded-lg" />
        <Skeleton className="h-80 w-full rounded-lg" />
      </div>
    )
  }

  return (
    <div className="grid min-w-0 items-start gap-4 lg:grid-cols-[minmax(14rem,18rem)_minmax(0,1fr)]">
      <ChartFrame className="flex min-h-0 flex-col p-3">
        <label className="flex min-w-0 items-center gap-2 border-b border-border-default px-1 pb-2 text-slate-400 focus-within:border-slate-500 focus-within:text-slate-300">
          <Search className="h-4 w-4 shrink-0" strokeWidth={1.8} />
          <span className="sr-only">Filter queries</span>
          <input
            type="search"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
            placeholder="Filter queries"
            className="min-w-0 flex-1 bg-transparent text-sm text-slate-100 outline-none placeholder:text-slate-600"
          />
          <span className="shrink-0 text-xs text-slate-500 tabular-nums">
            {filteredQueries.length}/{queryNames.length}
          </span>
        </label>

        {filteredQueries.length > 0 ? (
          <div
            ref={queryListRef}
            className="panel-scrollbar mt-2 max-h-56 min-h-0 space-y-1 overflow-y-auto pr-1 lg:max-h-[32rem]"
          >
            {filteredQueries.map((queryName) => {
              const selected = queryName === selectedQuery
              return (
                <button
                  key={queryName}
                  ref={selected ? selectedQueryRef : undefined}
                  type="button"
                  aria-pressed={selected}
                  onClick={() => onSelectQuery(queryName)}
                  className={cn(
                    "flex w-full min-w-0 items-center gap-2 rounded-md border px-2.5 py-2 text-left text-xs font-medium transition-colors outline-none",
                    "focus-visible:border-slate-300/60 focus-visible:ring-2 focus-visible:ring-slate-300/15",
                    selected
                      ? "border-border-strong bg-surface-elevated text-slate-50"
                      : "border-transparent text-slate-400 hover:bg-surface-raised hover:text-slate-200",
                  )}
                >
                  <Code2
                    className={cn(
                      "h-3.5 w-3.5 shrink-0",
                      selected ? "text-accent-300" : "text-slate-600",
                    )}
                    strokeWidth={1.8}
                  />
                  <span className="truncate" title={queryName}>
                    {queryName}
                  </span>
                </button>
              )
            })}
          </div>
        ) : (
          <p className="py-8 text-center text-sm text-slate-500">No matching queries.</p>
        )}
      </ChartFrame>

      <ChartFrame className="flex min-h-[24rem] min-w-0 flex-col overflow-hidden p-3 sm:p-4">
        <div className="mb-3 shrink-0 space-y-3">
          <div>
            <MetaLabel>Query SQL</MetaLabel>
            <p
              className="mt-1 truncate text-base font-semibold text-slate-100"
              title={selectedQuery}
            >
              {selectedQuery || "Select a query"}
            </p>
          </div>
          {dialects.length > 0 ? (
            <div
              className="panel-scrollbar flex gap-1.5 overflow-x-auto pb-1"
              aria-label="SQL dialect"
            >
              {dialects.map((dialect) => (
                <SegmentedButton
                  key={dialect}
                  selected={selectedDialect === dialect}
                  aria-pressed={selectedDialect === dialect}
                  onClick={() => onSelectDialect(dialect)}
                  size="sm"
                  className="shrink-0 rounded-md"
                >
                  {formatDialectName(dialect)}
                </SegmentedButton>
              ))}
            </div>
          ) : null}
        </div>
        <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border-subtle bg-surface-primary/40">
          <SqlCodeView
            code={selectedSql}
            wrapLines
            className="min-h-[20rem] text-xs leading-relaxed"
          />
        </div>
      </ChartFrame>
    </div>
  )
}

function resolveSql(entry: QuerySqlEntry | undefined, dialect: string) {
  if (!entry) return "-- SQL is not available for this query."
  if (dialect === "base") return entry.sql ?? "-- Base SQL is not available for this query."
  return entry.db_overrides[dialect] ?? entry.sql ?? "-- SQL is not available for this dialect."
}

function formatDialectName(dialect: string) {
  const labels: Record<string, string> = {
    base: "Base SQL",
    clickhouse: "ClickHouse",
    datafusion: "DataFusion",
    duckdb: "DuckDB",
    monetdb: "MonetDB",
    postgres: "PostgreSQL",
    questdb: "QuestDB",
    starrocks: "StarRocks",
    timescaledb: "TimescaleDB",
  }
  return labels[dialect] ?? dialect
}
