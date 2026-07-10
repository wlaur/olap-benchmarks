import { Search } from "lucide-react"
import { useMemo, useState } from "react"

import { Skeleton } from "../Skeleton"

export function CatalogQueryList({
  queryNames,
  loading,
}: {
  queryNames: readonly string[]
  loading: boolean
}) {
  const [filter, setFilter] = useState("")
  const filteredQueries = useMemo(() => {
    const normalizedFilter = filter.trim().toLowerCase()
    if (!normalizedFilter) return queryNames
    return queryNames.filter((queryName) => queryName.toLowerCase().includes(normalizedFilter))
  }, [filter, queryNames])

  return (
    <div className="space-y-3">
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
        {!loading ? (
          <span className="shrink-0 text-xs text-slate-500 tabular-nums">
            {filteredQueries.length}/{queryNames.length}
          </span>
        ) : null}
      </label>

      {loading ? (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-4">
          {Array.from({ length: 12 }, (_, index) => (
            <Skeleton key={index} className="h-8 w-full" />
          ))}
        </div>
      ) : filteredQueries.length > 0 ? (
        <div className="panel-scrollbar grid max-h-64 grid-cols-2 gap-x-4 overflow-y-auto sm:grid-cols-3 xl:grid-cols-4">
          {filteredQueries.map((queryName) => (
            <div
              key={queryName}
              className="truncate border-b border-border-subtle px-1 py-2 text-xs font-medium text-slate-300"
              title={queryName}
            >
              {queryName}
            </div>
          ))}
        </div>
      ) : (
        <p className="py-6 text-center text-sm text-slate-500">No matching queries.</p>
      )}
    </div>
  )
}
