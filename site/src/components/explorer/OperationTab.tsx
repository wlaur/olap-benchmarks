import { useEffect, useMemo, useState } from "react"

import { useSelectionState } from "../../hooks/useSelectionState"
import { buildQueryComparisonRows, computeMaxDuration } from "../../lib/chartTransforms"
import { getDatabaseColors } from "../../lib/databaseColors"
import type { DurationScaleMode } from "../../lib/format"
import type { SuiteConfig } from "../../lib/suiteConfig"
import type {
  BenchmarkOperation,
  MetricSample,
  QueriesManifest,
  QueryStep,
  QuerySummary,
} from "../../lib/types"
import { DatabaseLegend } from "../DatabaseLegend"
import { QueryComparisonTable } from "../QueryComparisonTable"
import { QueryDetailPanel } from "../QueryDetailPanel"
import {
  getTraceEligibleDatabases,
  ResourceMetricsDrawer,
  RESOURCE_DRAWER_HEIGHT_PX,
  shouldShowResourceDrawer,
} from "../ResourceMetricsDrawer"
import { RunTimeline } from "../RunTimeline"
import { BodyText, Eyebrow, FeatureTitle, MetaLabel, SectionTitle } from "../Typography"
import { InspectorSkeleton, LegendSkeleton, QueryTableSkeleton } from "./ExplorerSkeletons"

const BOTTOM_GRID_CLASS =
  "grid min-w-0 gap-4 xl:min-h-[44rem] xl:flex-1 xl:grid-cols-[minmax(0,1.25fr)_minmax(24rem,0.95fr)]"
const QUERY_SECTION_CLASS =
  "flex min-h-[26rem] min-w-0 flex-col rounded-2xl bg-surface-raised xl:max-h-[44rem] xl:min-h-0"
const QUERY_TABLE_WRAPPER_CLASS =
  "min-h-[24rem] min-w-0 flex-1 overflow-hidden p-5 pt-4 md:min-h-[30rem] xl:min-h-0"
const QUERY_TABLE_CONTAINER_CLASS =
  "h-[24rem] min-h-[24rem] min-w-0 md:h-[30rem] md:min-h-[30rem] xl:h-full xl:max-h-full xl:min-h-0"
const DETAIL_SECTION_CLASS = "min-h-[28rem] xl:min-h-0"

interface OperationTabProps {
  operation: BenchmarkOperation
  suiteConfig: SuiteConfig
  querySummaries: QuerySummary[]
  querySteps: QueryStep[]
  metricSamples: MetricSample[]
  databases: string[]
  includedDatabases: string[]
  queriesManifest: QueriesManifest | null
  isLoading: boolean
}

export function OperationTab({
  operation,
  suiteConfig,
  querySummaries,
  querySteps,
  metricSamples,
  databases,
  includedDatabases,
  queriesManifest,
  isLoading,
}: OperationTabProps) {
  const databaseColors = getDatabaseColors(databases)
  const selection = useSelectionState()
  const { selectedQuery, setSelectedQuery } = selection
  const [tableScaleMode, setTableScaleMode] = useState<DurationScaleMode>("linear")
  const [resourceDrawerOpen, setResourceDrawerOpen] = useState(false)

  const queryRows = useMemo(
    () => buildQueryComparisonRows(querySummaries, includedDatabases, suiteConfig),
    [querySummaries, includedDatabases, suiteConfig],
  )
  const maxDuration = computeMaxDuration(queryRows)

  const selectedRow = selectedQuery
    ? (queryRows.find((r) => r.query_name === selectedQuery) ?? null)
    : null

  const traceEligibleDatabases = useMemo(
    () => getTraceEligibleDatabases(querySteps, selectedQuery, includedDatabases),
    [querySteps, selectedQuery, includedDatabases],
  )
  const hasTraceDrawer = traceEligibleDatabases.length > 0

  const selectedSql = queriesManifest?.[suiteConfig.id]?.[selectedQuery ?? ""] ?? null

  const timelineTitle = operation === "mutate" ? "Mutate timeline" : "Select timeline"
  const timelineDescription =
    operation === "mutate"
      ? "Insert, upsert, and delete mutation execution timeline per database. Each segment represents one mutation iteration."
      : "Full select query execution timeline per database. Each segment represents one query iteration. Click to inspect."

  useEffect(() => {
    if (selectedQuery === null) {
      setResourceDrawerOpen(false)
      return
    }

    if (!shouldShowResourceDrawer(querySteps, selectedQuery, includedDatabases)) {
      setResourceDrawerOpen(false)
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSelectedQuery(null)
      }
    }

    window.addEventListener("keydown", handleKeyDown)

    return () => {
      window.removeEventListener("keydown", handleKeyDown)
    }
  }, [selectedQuery, setSelectedQuery, querySteps, includedDatabases])

  return (
    <div className="flex flex-col gap-4">
      {!isLoading && querySteps.length > 0 ? (
        <RunTimeline
          querySteps={querySteps}
          databases={includedDatabases}
          databaseColors={databaseColors}
          onSelectQuery={(queryName) => setSelectedQuery(queryName)}
          selectedQuery={selectedQuery}
          title={timelineTitle}
          description={timelineDescription}
        />
      ) : null}

      <div className={BOTTOM_GRID_CLASS}>
        <section className={QUERY_SECTION_CLASS}>
          <div className="flex min-h-[5.5rem] shrink-0 items-start justify-between gap-4 border-b border-border-default px-5 py-4">
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
              <div className="flex min-w-[13rem] flex-col items-end gap-2">
                <div className="flex h-9 items-center">
                  {operation === "select" &&
                  selectedQuery &&
                  hasTraceDrawer &&
                  !resourceDrawerOpen ? (
                    <button
                      type="button"
                      onClick={() => setResourceDrawerOpen(true)}
                      className="hover:text-accent-100 inline-flex items-center gap-2 rounded-full border border-border-default bg-surface-inset px-3 py-1.5 text-sm font-medium text-slate-200 transition-colors hover:border-accent-400/40 hover:bg-accent-400/10"
                    >
                      Resource traces
                      <span className="text-xs text-slate-400">
                        {traceEligibleDatabases.length}{" "}
                        {traceEligibleDatabases.length === 1 ? "database" : "databases"}
                      </span>
                    </button>
                  ) : null}
                </div>
                <DatabaseLegend databases={includedDatabases} databaseColors={databaseColors} />
              </div>
            )}
          </div>

          <div className={QUERY_TABLE_WRAPPER_CLASS}>
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
                containerClassName={QUERY_TABLE_CONTAINER_CLASS}
              />
            )}
          </div>
        </section>

        <section className={DETAIL_SECTION_CLASS}>
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

      {operation === "select" ? (
        <>
          <ResourceMetricsDrawer
            isOpen={resourceDrawerOpen}
            onClose={() => setResourceDrawerOpen(false)}
            selectedQuery={selectedQuery}
            querySteps={querySteps}
            metricSamples={metricSamples}
            databases={traceEligibleDatabases}
            databaseColors={databaseColors}
          />
          {resourceDrawerOpen ? (
            <div className="shrink-0" style={{ height: RESOURCE_DRAWER_HEIGHT_PX }} />
          ) : null}
        </>
      ) : null}
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
