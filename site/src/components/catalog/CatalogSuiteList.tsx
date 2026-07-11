import { BookOpenText } from "lucide-react"

import type { CatalogSuiteSummary } from "../../lib/catalog"
import { cn } from "../../lib/cn"
import { Skeleton } from "../Skeleton"

export function CatalogSuiteList({
  summaries,
  selectedSuiteId,
  loading,
  onSelect,
}: {
  summaries: readonly CatalogSuiteSummary[]
  selectedSuiteId: string | null
  loading: boolean
  onSelect: (suiteId: string) => void
}) {
  if (loading) {
    return (
      <div className="divide-y divide-border-subtle">
        {Array.from({ length: 6 }, (_, index) => (
          <div key={index} className="space-y-2 px-2 py-3">
            <Skeleton className="h-4 w-28" />
            <Skeleton className="h-3 w-40" />
          </div>
        ))}
      </div>
    )
  }

  return (
    <div className="divide-y divide-border-subtle">
      {summaries.map((summary) => {
        const selected = summary.definition.id === selectedSuiteId
        return (
          <button
            key={summary.definition.id}
            type="button"
            aria-pressed={selected}
            onClick={() => onSelect(summary.definition.id)}
            className={cn(
              "flex w-full min-w-0 items-center gap-3 rounded-md border border-transparent px-2.5 py-3 text-left transition-colors outline-none",
              "focus-visible:border-slate-300/60 focus-visible:ring-2 focus-visible:ring-slate-300/15",
              selected
                ? "border-border-strong bg-surface-elevated text-slate-50"
                : "text-slate-300 hover:bg-surface-inset hover:text-slate-100",
            )}
          >
            <BookOpenText
              className={cn("h-4 w-4 shrink-0", selected ? "text-accent-300" : "text-slate-500")}
              strokeWidth={1.8}
            />
            <span className="min-w-0 flex-1">
              <span className="block truncate text-sm font-semibold">
                {summary.definition.title}
              </span>
              <span className="mt-0.5 block truncate text-xs text-slate-400">
                {summary.queryNames.length} queries / {summary.coveredCombinations}/
                {summary.totalCombinations} covered
              </span>
            </span>
          </button>
        )
      })}
    </div>
  )
}
