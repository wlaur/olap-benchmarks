import { Search } from "lucide-react"
import { useMemo, useState } from "react"

import { getSqlDialects } from "../../lib/catalog"
import { formatDatabaseName } from "../../lib/databaseNames"
import type { QuerySqlEntry } from "../../lib/types"
import { QuietButton, SegmentedButton } from "../controls/Control"
import { DetailsDialog } from "../layout/DetailsDialog"
import { ChartFrame } from "../layout/Panel"
import { SqlCodeView } from "../SqlCodeView"
import { BodyText, SectionTitle } from "../Typography"

export function CatalogQueryList({
  queryNames,
  selectedQuery,
  selectedDialect,
  sqlEntry,
  onSelectQuery,
  onSelectDialect,
}: {
  queryNames: readonly string[]
  selectedQuery: string
  selectedDialect: string
  sqlEntry: QuerySqlEntry | undefined
  onSelectQuery: (query: string) => void
  onSelectDialect: (dialect: string) => void
}) {
  const [filter, setFilter] = useState("")
  const filteredQueries = useMemo(() => {
    const normalized = filter.trim().toLowerCase()
    return queryNames.filter((name) => name.toLowerCase().includes(normalized))
  }, [filter, queryNames])
  const dialects = getSqlDialects(sqlEntry)
  const sql = selectedDialect === "base" ? sqlEntry?.sql : sqlEntry?.db_overrides[selectedDialect]

  if (queryNames.length === 0)
    return <BodyText>No query definitions are published for this workload.</BodyText>

  return (
    <div className="grid min-w-0 gap-4 md:grid-cols-2">
      <ChartFrame className="space-y-3">
        <label className="flex min-h-11 items-center gap-2 rounded-md border border-border-default px-3 focus-within:ring-2 focus-within:ring-accent-300">
          <Search className="size-4 shrink-0 text-slate-400" aria-hidden="true" />
          <span className="sr-only">Filter queries</span>
          <input
            type="search"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
            placeholder="Filter queries"
            className="min-w-0 flex-1 bg-transparent text-base text-slate-100 outline-none placeholder:text-slate-400"
          />
        </label>
        <p role="status" className="text-sm text-slate-400">
          {filteredQueries.length} of {queryNames.length} queries
        </p>
        {filteredQueries.length > 0 ? (
          <div
            role="group"
            aria-label="Query selection"
            className="panel-scrollbar max-h-80 space-y-2 overflow-y-auto p-1"
          >
            {filteredQueries.map((query) => (
              <SegmentedButton
                key={query}
                selected={selectedQuery === query}
                aria-pressed={selectedQuery === query}
                onClick={() => onSelectQuery(query)}
                className="min-h-11 w-full justify-start px-3 py-2 text-left text-sm [overflow-wrap:anywhere]"
              >
                {query}
              </SegmentedButton>
            ))}
          </div>
        ) : (
          <div className="space-y-3">
            <BodyText>No matching queries.</BodyText>
            <QuietButton onClick={() => setFilter("")}>Clear query filter</QuietButton>
          </div>
        )}
      </ChartFrame>
      <ChartFrame className="space-y-3">
        <SectionTitle as="h3" className="[overflow-wrap:anywhere]">
          {selectedQuery}
        </SectionTitle>
        <BodyText className="text-sm">SQL and database dialects open on request.</BodyText>
        <DetailsDialog key={selectedQuery} title={`SQL · ${selectedQuery}`} trigger="View SQL">
          <div role="group" aria-label="SQL dialect" className="mb-4 flex flex-wrap gap-2">
            {dialects.map((dialect) => (
              <SegmentedButton
                key={dialect}
                selected={selectedDialect === dialect}
                aria-pressed={selectedDialect === dialect}
                onClick={() => onSelectDialect(dialect)}
                className="min-h-11 text-sm"
              >
                {dialect === "base" ? "Base SQL" : formatDatabaseName(dialect)}
              </SegmentedButton>
            ))}
          </div>
          {sql ? (
            <ChartFrame className="p-0">
              <SqlCodeView
                code={sql}
                wrapLines
                fillHeight={false}
                className="text-sm leading-relaxed"
              />
            </ChartFrame>
          ) : (
            <BodyText>SQL is not available for this query and dialect.</BodyText>
          )}
        </DetailsDialog>
      </ChartFrame>
    </div>
  )
}
