import { useDeferredValue, useEffect, useMemo, useState } from "react"
import { useContainerWidth } from "react-grid-layout"

import type { SelectionState } from "../../hooks/useSelectionState"
import { buildQueryComparisonRows, computeMaxDuration } from "../../lib/chartTransforms"
import { cn } from "../../lib/cn"
import { getDatabaseColors } from "../../lib/databaseColors"
import type { DurationScaleMode } from "../../lib/format"
import type { SuiteConfig } from "../../lib/suiteConfig"
import type { BenchmarkOperation, QueriesManifest, QueryStep, QuerySummary } from "../../lib/types"
import { DatabaseLegend } from "../DatabaseLegend"
import { QueryComparisonTable } from "../QueryComparisonTable"
import { QueryDetailPanel } from "../QueryDetailPanel"
import { RunTimeline } from "../RunTimeline"
import { BodyText, Eyebrow, FeatureTitle, MetaLabel, SectionTitle } from "../Typography"
import { InspectorSkeleton, LegendSkeleton, QueryTableSkeleton } from "./ExplorerSkeletons"

const DETAIL_SPLIT_MIN_WIDTH = 1160
const QUERY_TABLE_MIN_HEIGHT_CLASS = "min-h-[24rem]"
const QUERY_TABLE_SPLIT_SCROLL_AREA_CLASS = "max-h-[42rem]"
const QUERY_TABLE_STACKED_SCROLL_AREA_CLASS = "max-h-[34rem]"

interface OperationTabProps {
  operation: BenchmarkOperation
  suiteConfig: SuiteConfig
  querySummaries: QuerySummary[]
  querySteps: QueryStep[]
  databases: string[]
  includedDatabases: string[]
  queriesManifest: QueriesManifest | null
  selection: SelectionState
  isLoading: boolean
}

export function OperationTab({
  operation,
  suiteConfig,
  querySummaries,
  querySteps,
  databases,
  includedDatabases,
  queriesManifest,
  selection,
  isLoading,
}: OperationTabProps) {
  const databaseColors = useMemo(() => getDatabaseColors(databases), [databases])
  const { selectedQuery, setSelectedQuery } = selection
  const [tableScaleMode, setTableScaleMode] = useState<DurationScaleMode>("linear")
  const {
    width: layoutWidth,
    containerRef: layoutRef,
    mounted: layoutMounted,
  } = useContainerWidth({
    initialWidth: 1280,
  })
  const deferredLayoutWidth = useDeferredValue(layoutWidth)

  const queryRows = useMemo(
    () => buildQueryComparisonRows(querySummaries, includedDatabases, suiteConfig),
    [querySummaries, includedDatabases, suiteConfig],
  )
  const maxDuration = useMemo(() => computeMaxDuration(queryRows), [queryRows])

  const selectedRow = selectedQuery
    ? (queryRows.find((r) => r.query_name === selectedQuery) ?? null)
    : null

  const selectedSql = queriesManifest?.[suiteConfig.id]?.[selectedQuery ?? ""] ?? null
  const showTimeline = isLoading || querySteps.length > 0
  const isSplitLayout = !layoutMounted || deferredLayoutWidth >= DETAIL_SPLIT_MIN_WIDTH

  const timelineTitle = operation === "mutate" ? "Mutate timeline" : "Select timeline"
  const timelineDescription =
    operation === "mutate"
      ? "Insert, upsert, and delete mutation execution timeline per database. Each segment represents one mutation iteration."
      : "Full select query execution timeline per database. Each segment represents one query iteration. Click to inspect."

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSelectedQuery(null)
      }
    }

    window.addEventListener("keydown", handleKeyDown)

    return () => {
      window.removeEventListener("keydown", handleKeyDown)
    }
  }, [setSelectedQuery])

  return (
    <div className="flex h-full min-h-0 flex-col gap-4">
      {showTimeline ? (
        <RunTimeline
          querySteps={querySteps}
          databases={includedDatabases}
          databaseColors={databaseColors}
          onSelectQuery={(queryName) => setSelectedQuery(queryName)}
          selectedQuery={selectedQuery}
          title={timelineTitle}
          description={timelineDescription}
          loading={isLoading}
        />
      ) : null}

      <div
        ref={layoutRef}
        className={cn(
          "grid min-h-0 min-w-0 gap-4",
          isSplitLayout
            ? "flex-1 grid-cols-[minmax(0,1.35fr)_minmax(22rem,0.92fr)] items-stretch"
            : "grid-cols-1",
        )}
      >
        <section
          className={cn(
            "flex min-w-0 flex-col overflow-hidden rounded-2xl bg-surface-raised",
            isSplitLayout ? "h-full min-h-0" : "min-h-[32rem]",
          )}
        >
          <div className="flex min-h-[5.5rem] shrink-0 flex-col items-start justify-between gap-3 border-b border-border-default px-5 py-4 sm:flex-row sm:gap-4">
            <div>
              <SectionTitle as="h3">
                {operation === "mutate" ? "Mutation" : "Select"} latency comparison
              </SectionTitle>
              <BodyText className="mt-1">
                Click a row to inspect its latency spread{operation === "select" ? " and SQL" : ""}.
              </BodyText>
            </div>
            {isLoading ? (
              <LegendSkeleton />
            ) : (
              <div className="flex w-full min-w-0 flex-col gap-2 sm:w-auto sm:min-w-[13rem] sm:items-end">
                <DatabaseLegend databases={includedDatabases} databaseColors={databaseColors} />
              </div>
            )}
          </div>

          <div className="min-h-0 min-w-0 flex-1 overflow-hidden p-5 pt-4">
            {isLoading ? (
              <QueryTableSkeleton />
            ) : (
              <QueryComparisonTable
                rows={queryRows}
                databases={includedDatabases}
                databaseColors={databaseColors}
                selection={selection}
                maxDuration={maxDuration}
                scaleMode={tableScaleMode}
                onScaleModeChange={setTableScaleMode}
                containerClassName={cn(
                  "min-h-0 min-w-0",
                  !isSplitLayout && QUERY_TABLE_MIN_HEIGHT_CLASS,
                )}
                scrollAreaClassName={
                  isSplitLayout
                    ? QUERY_TABLE_SPLIT_SCROLL_AREA_CLASS
                    : QUERY_TABLE_STACKED_SCROLL_AREA_CLASS
                }
              />
            )}
          </div>
        </section>

        <section className={cn("min-w-0", isSplitLayout ? "h-full min-h-0" : "min-h-[28rem]")}>
          {isLoading ? (
            <InspectorSkeleton />
          ) : selectedRow ? (
            <QueryDetailPanel
              row={selectedRow}
              databases={includedDatabases}
              databaseColors={databaseColors}
              sql={selectedSql}
              onClose={() => selection.setSelectedQuery(null)}
            />
          ) : (
            <InspectorPlaceholder
              rowCount={queryRows.length}
              databaseCount={includedDatabases.length}
              operation={operation}
            />
          )}
        </section>
      </div>
    </div>
  )
}

interface InspectorPlaceholderProps {
  rowCount: number
  databaseCount: number
  operation: BenchmarkOperation
}

function InspectorPlaceholder({ rowCount, databaseCount, operation }: InspectorPlaceholderProps) {
  const label = operation === "mutate" ? "mutation" : "query"
  return (
    <div className="flex h-full min-h-0 flex-col justify-between rounded-2xl bg-surface-raised p-5">
      <div>
        <Eyebrow>Inspector</Eyebrow>
        <FeatureTitle as="h3" className="mt-3">
          Pick a {label} row
        </FeatureTitle>
        <BodyText className="mt-3 max-w-md leading-6">
          The detail pane stays pinned on the right. Select any {label} to inspect latency by
          database{operation === "select" ? " and compare the SQL variants" : ""} for only the
          databases currently included.
        </BodyText>
      </div>

      <div className="grid gap-3">
        <div className="rounded-xl border border-border-default bg-surface-inset px-4 py-3">
          <MetaLabel>Rows available</MetaLabel>
          <p className="mt-2 text-lg font-semibold text-slate-100">{rowCount}</p>
        </div>
        <div className="rounded-xl border border-border-default bg-surface-inset px-4 py-3">
          <MetaLabel>Active databases</MetaLabel>
          <p className="mt-2 text-lg font-semibold text-slate-100">{databaseCount}</p>
        </div>
      </div>
    </div>
  )
}
