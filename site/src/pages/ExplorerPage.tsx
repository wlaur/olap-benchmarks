import { lazy, Suspense, useEffect, useMemo, useState } from "react"
import { useSearchParams } from "react-router-dom"

import { CycleSelect } from "../components/controls/CycleSelect"
import { CrossSystemComparisonPanel } from "../components/explorer/CrossSystemComparisonPanel"
import { ExplorerSection } from "../components/explorer/ExplorerSection"
import { FilterChipsSkeleton } from "../components/explorer/ExplorerSkeletons"
import { OperationTabs } from "../components/explorer/OperationTabs"
import { OverviewPanel } from "../components/explorer/OverviewPanel"
import { RunMethodologyPanel } from "../components/explorer/RunMethodologyPanel"
import { SuiteScoreCards } from "../components/explorer/SuiteScoreCards"

const QueryHeatmapPanel = lazy(() =>
  import("../components/explorer/QueryHeatmapPanel").then((m) => ({
    default: m.QueryHeatmapPanel,
  })),
)
const ResourceTrendPanel = lazy(() =>
  import("../components/explorer/ResourceTrendPanel").then((m) => ({
    default: m.ResourceTrendPanel,
  })),
)
const FlameGraphPanel = lazy(() =>
  import("../components/explorer/FlameGraphPanel").then((m) => ({ default: m.FlameGraphPanel })),
)
import { DatabaseMultiSelect } from "../components/filters/DatabaseMultiSelect"
import {
  ScaleFactorSelector,
  ScaleFactorSelectorSkeleton,
} from "../components/filters/ScaleFactorSelector"
import { InsertPerformancePanel } from "../components/InsertPerformancePanel"
import { useSelectionState } from "../hooks/useSelectionState"
import { useSuiteData } from "../hooks/useSuiteData"
import type { BenchmarkDefinition, BenchmarkSuiteId } from "../lib/benchmarks"
import { cn } from "../lib/cn"
import { getDatabaseColors } from "../lib/databaseColors"
import { getSuiteConfig } from "../lib/suiteConfig"
import type { QueryAnalysisOperation } from "../lib/types"

interface ExplorerPageProps {
  system: string | null
  suiteId: BenchmarkSuiteId
  suiteDefinition: BenchmarkDefinition
  isSystemLoading?: boolean
}

const QUERY_GROUP_OPTIONS: { value: QueryAnalysisOperation; label: string }[] = [
  { value: "select", label: "Select queries" },
  { value: "mutate", label: "Mutate queries" },
  { value: "concurrent", label: "Concurrent workload" },
]

export function ExplorerPage({
  system,
  suiteId,
  suiteDefinition,
  isSystemLoading = false,
}: ExplorerPageProps) {
  const [searchParams] = useSearchParams()
  const preferredScaleFactor = parseScaleFactor(searchParams.get("scale"))
  const suiteConfig = useMemo(() => getSuiteConfig(suiteDefinition), [suiteDefinition])
  const queryGroupOptions = useMemo(
    () =>
      QUERY_GROUP_OPTIONS.filter(
        (option) => option.value === "select" || suiteConfig.operations.includes(option.value),
      ),
    [suiteConfig.operations],
  )
  const [selectedOperation, setSelectedOperation] = useState<QueryAnalysisOperation>("select")
  const selection = useSelectionState()
  const { resetSelection } = selection

  const {
    state,
    databases,
    includedDatabases,
    scaleFactors,
    selectedScaleFactor,
    setSelectedScaleFactor,
    setSelectedDatabases,
    filteredQuerySummaries,
    filteredQueryCoverage,
    filteredMutateSummaries,
    filteredConcurrentSummaries,
    filteredOperationSummaries,
    isLoading,
    toggleDatabase,
  } = useSuiteData(system, suiteId, suiteConfig, isSystemLoading, preferredScaleFactor)

  const databaseColors = useMemo(() => getDatabaseColors(databases), [databases])
  const queryNames = useMemo(() => {
    if (state.queriesManifest === null) return []
    const suiteQueries = state.queriesManifest[suiteConfig.queriesKey]
    if (!suiteQueries) {
      throw new Error(`queries.json is missing suite ${suiteConfig.queriesKey}`)
    }
    return Object.keys(suiteQueries)
  }, [state.queriesManifest, suiteConfig.queriesKey])
  const deferredLoading = isLoading || state.deferredLoading
  const showInsertPerformancePanel = deferredLoading || state.insertSteps.length > 0
  const showFlameGraphPanel = deferredLoading || system !== null
  const showResourceTrendPanel = deferredLoading || state.metricSamples.length > 0

  const selectedOperationSummaries =
    selectedOperation === "mutate"
      ? filteredMutateSummaries
      : selectedOperation === "concurrent"
        ? filteredConcurrentSummaries
        : filteredQuerySummaries

  useEffect(() => {
    if (!queryGroupOptions.some((option) => option.value === selectedOperation)) {
      setSelectedOperation("select")
    }
  }, [queryGroupOptions, selectedOperation])

  useEffect(() => {
    resetSelection()
  }, [resetSelection, suiteId])

  if (!isLoading && state.error) {
    return (
      <section className="flex h-full min-h-0 w-full flex-1 items-center justify-center">
        <div className="rounded-2xl border border-red-500/30 bg-red-950/20 px-6 py-5 text-sm text-red-300">
          Failed to load {suiteConfig.label} data: {state.error}
        </div>
      </section>
    )
  }

  if (
    !isLoading &&
    state.runSummaries.length === 0 &&
    state.queryCoverage.length === 0 &&
    state.operationSummaries.length === 0
  ) {
    return (
      <section className="flex h-full min-h-0 w-full flex-1 items-center justify-center">
        <div className="rounded-2xl bg-surface-raised px-6 py-5 text-sm text-slate-300">
          No completed {suiteConfig.label} runs were found for {system ?? "the selected system"}.
        </div>
      </section>
    )
  }

  return (
    <div className="flex min-h-full w-full flex-col gap-6 pb-6">
      <div className="flex flex-wrap items-center gap-x-5 gap-y-3">
        <div className="flex flex-wrap items-center gap-3">
          <span className="shrink-0 text-[0.65rem] font-semibold tracking-widest text-slate-300 uppercase">
            Scale
          </span>
          {isLoading && scaleFactors.length === 0 ? (
            <ScaleFactorSelectorSkeleton />
          ) : (
            <ScaleFactorSelector
              scaleFactors={scaleFactors}
              selected={selectedScaleFactor}
              onChange={setSelectedScaleFactor}
              disabled={isLoading}
            />
          )}
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <span className="shrink-0 text-[0.65rem] font-semibold tracking-widest text-slate-300 uppercase">
            Databases
          </span>
          {isLoading ? (
            <FilterChipsSkeleton />
          ) : (
            <DatabaseMultiSelect
              databases={databases}
              selectedDatabases={includedDatabases}
              databaseColors={databaseColors}
              onSelectAll={() => setSelectedDatabases(databases)}
              onToggleDatabase={toggleDatabase}
            />
          )}
        </div>
      </div>

      <ExplorerSection
        title="Summary"
        description="Suite ranking and aggregate timing across phases."
      >
        <SuiteScoreCards
          querySummaries={filteredQuerySummaries}
          queryCoverage={filteredQueryCoverage}
          queryNames={queryNames}
          databaseColors={databaseColors}
          isLoading={isLoading}
        />

        <CrossSystemComparisonPanel
          suite={suiteId}
          suiteScaleFactor={selectedScaleFactor}
          queryNames={queryNames}
          currentSystem={system}
          isLoading={isLoading}
        />

        <div
          className={cn(
            "grid items-stretch gap-4",
            showInsertPerformancePanel
              ? "xl:grid-cols-[minmax(22rem,0.9fr)_minmax(0,1.5fr)]"
              : "grid-cols-1",
          )}
        >
          <OverviewPanel
            suiteConfig={suiteConfig}
            databases={databases}
            includedDatabases={includedDatabases}
            operationSummaries={filteredOperationSummaries}
            databaseColors={databaseColors}
            isLoading={isLoading}
          />
          {showInsertPerformancePanel ? (
            <InsertPerformancePanel
              insertSteps={state.insertSteps}
              metricSamples={state.metricSamples}
              databases={includedDatabases}
              databaseColors={databaseColors}
              isLoading={deferredLoading}
            />
          ) : null}
        </div>

        <RunMethodologyPanel
          operationSummaries={filteredOperationSummaries}
          includedDatabases={includedDatabases}
          databaseColors={databaseColors}
          isLoading={isLoading}
        />
      </ExplorerSection>

      <ExplorerSection
        title="Query analysis"
        description="Per-query latency, timeline, and detail inspector."
        trailing={
          queryGroupOptions.length > 1 ? (
            <CycleSelect
              label="Query Group"
              value={selectedOperation}
              options={queryGroupOptions}
              onChange={setSelectedOperation}
            />
          ) : null
        }
      >
        <div className="grid min-h-0 min-w-0 items-start gap-4 2xl:grid-cols-[minmax(0,1.4fr)_minmax(36rem,0.6fr)]">
          <OperationTabs
            activeOperation={selectedOperation}
            suiteConfig={suiteConfig}
            querySummaries={filteredQuerySummaries}
            mutateSummaries={filteredMutateSummaries}
            concurrentSummaries={filteredConcurrentSummaries}
            querySteps={state.querySteps}
            mutateSteps={state.mutateSteps}
            concurrentSteps={state.concurrentSteps}
            databases={databases}
            includedDatabases={includedDatabases}
            queriesManifest={state.queriesManifest}
            selection={selection}
            isLoading={isLoading}
            isTimelineLoading={deferredLoading}
          />

          <Suspense>
            <QueryHeatmapPanel
              suiteConfig={suiteConfig}
              querySummaries={selectedOperationSummaries}
              includedDatabases={includedDatabases}
              databaseColors={databaseColors}
              selection={selection}
              queriesManifest={state.queriesManifest}
              isLoading={isLoading}
            />
          </Suspense>
        </div>
      </ExplorerSection>

      {showResourceTrendPanel || showFlameGraphPanel ? (
        <ExplorerSection
          title="Runtime profile"
          description="CPU, memory, and execution-span breakdowns."
        >
          <Suspense>
            <div
              className={cn(
                "grid items-stretch gap-4",
                showResourceTrendPanel && showFlameGraphPanel
                  ? "xl:grid-cols-[minmax(0,1.08fr)_minmax(22rem,0.92fr)]"
                  : "grid-cols-1",
              )}
            >
              {showResourceTrendPanel ? (
                <ResourceTrendPanel
                  selectedOperation={selectedOperation}
                  suiteConfig={suiteConfig}
                  metricSamples={state.metricSamples}
                  insertSteps={state.insertSteps}
                  querySteps={state.querySteps}
                  mutateSteps={state.mutateSteps}
                  concurrentSteps={state.concurrentSteps}
                  databases={includedDatabases}
                  isLoading={deferredLoading}
                />
              ) : null}

              {showFlameGraphPanel ? (
                <FlameGraphPanel
                  system={system}
                  suite={suiteId}
                  suiteScaleFactor={selectedScaleFactor}
                  databases={includedDatabases}
                  metricSamples={state.metricSamples}
                  isLoading={deferredLoading}
                />
              ) : null}
            </div>
          </Suspense>
        </ExplorerSection>
      ) : null}
    </div>
  )
}

function parseScaleFactor(value: string | null): number | null {
  if (value === null) return null
  const scaleFactor = Number.parseInt(value, 10)
  return Number.isFinite(scaleFactor) && scaleFactor > 0 ? scaleFactor : null
}
