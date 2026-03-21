import { useMemo } from "react"

import type { SelectionState } from "../../hooks/useSelectionState"
import { buildQueryComparisonRows } from "../../lib/chartTransforms"
import type { SuiteConfig } from "../../lib/suiteConfig"
import type { QuerySummary } from "../../lib/types"
import { PanelCard, PanelHeader } from "../layout/Panel"
import { QueryHeatmapGrid } from "../QueryHeatmapGrid"
import { Skeleton } from "../Skeleton"
import { BodyText, SectionTitle } from "../Typography"

interface QueryHeatmapPanelProps {
  suiteConfig: SuiteConfig
  querySummaries: QuerySummary[]
  includedDatabases: string[]
  databaseColors: Record<string, string>
  selection: SelectionState
  isLoading: boolean
}

export function QueryHeatmapPanel({
  suiteConfig,
  querySummaries,
  includedDatabases,
  databaseColors,
  selection,
  isLoading,
}: QueryHeatmapPanelProps) {
  const queryRows = useMemo(
    () => buildQueryComparisonRows(querySummaries, includedDatabases, suiteConfig),
    [querySummaries, includedDatabases, suiteConfig],
  )

  if (!isLoading && queryRows.length === 0) return null

  return (
    <PanelCard>
      <PanelHeader>
        <div>
          <SectionTitle as="h3">Query latency heatmap</SectionTitle>
          <BodyText className="mt-1">
            Each cell shows median query time. Color encodes ratio vs fastest database per query.
          </BodyText>
        </div>
      </PanelHeader>

      <div className="mt-3 rounded-lg bg-surface-inset p-4">
        {isLoading ? (
          <HeatmapSkeleton />
        ) : (
          <QueryHeatmapGrid
            rows={queryRows}
            databases={includedDatabases}
            databaseColors={databaseColors}
            selection={selection}
          />
        )}
      </div>
    </PanelCard>
  )
}

function HeatmapSkeleton() {
  return (
    <div className="space-y-2">
      <div className="flex gap-2">
        <Skeleton className="h-4 w-24" />
        <Skeleton className="h-4 w-16" />
        <Skeleton className="h-4 w-20" />
        <Skeleton className="h-4 w-16" />
      </div>
      {Array.from({ length: 6 }, (_, i) => (
        <div key={i} className="flex gap-1">
          <Skeleton className="h-7 w-36" />
          <Skeleton className="h-7 w-16" />
          <Skeleton className="h-7 w-16" />
          <Skeleton className="h-7 w-16" />
          <Skeleton className="h-7 w-16" />
        </div>
      ))}
    </div>
  )
}
