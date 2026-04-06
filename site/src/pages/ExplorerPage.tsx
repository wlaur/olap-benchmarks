import { lazy, Suspense, useEffect, useMemo, useState } from "react"

import { FilterChipsSkeleton } from "../components/explorer/ExplorerSkeletons"
import { OperationSelector } from "../components/explorer/OperationSelector"
import { OperationTabs } from "../components/explorer/OperationTabs"
import { OverviewPanel } from "../components/explorer/OverviewPanel"

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
import { InsertPerformancePanel } from "../components/InsertPerformancePanel"
import { useSelectionState } from "../hooks/useSelectionState"
import { useSuiteData } from "../hooks/useSuiteData"
import type { BenchmarkSuiteId } from "../lib/benchmarks"
import { cn } from "../lib/cn"
import { getDatabaseColors } from "../lib/databaseColors"
import { getSuiteConfig } from "../lib/suiteConfig"

interface ExplorerPageProps {
  system: string | null
  suiteId: BenchmarkSuiteId
  isSystemLoading?: boolean
}

export function ExplorerPage({ system, suiteId, isSystemLoading = false }: ExplorerPageProps) {
  const suiteConfig = useMemo(() => getSuiteConfig(suiteId), [suiteId])
  const hasMutateOperation = suiteConfig.operations.includes("mutate")
  const [selectedOperation, setSelectedOperation] = useState<"select" | "mutate">("select")
  const selection = useSelectionState()
  const { resetSelection } = selection

  const {
    state,
    databases,
    includedDatabases,
    setSelectedDatabases,
    filteredQuerySummaries,
    filteredMutateSummaries,
    filteredOperationSummaries,
    filteredQuerySteps,
    filteredMutateSteps,
    isLoading,
    toggleDatabase,
  } = useSuiteData(system, suiteId, suiteConfig, isSystemLoading)

  const databaseColors = useMemo(() => getDatabaseColors(databases), [databases])
  const deferredLoading = isLoading || state.deferredLoading
  const showInsertPerformancePanel = deferredLoading || state.insertSteps.length > 0
  const showFlameGraphPanel = deferredLoading || system !== null
  const showResourceTrendPanel = deferredLoading || state.metricSamples.length > 0

  useEffect(() => {
    if (!hasMutateOperation) {
      setSelectedOperation("select")
    }
  }, [hasMutateOperation])

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

  if (!isLoading && state.runSummaries.length === 0) {
    return (
      <section className="flex h-full min-h-0 w-full flex-1 items-center justify-center">
        <div className="rounded-2xl bg-surface-raised px-6 py-5 text-sm text-slate-400">
          No completed {suiteConfig.label} runs were found for {system ?? "the selected system"}.
        </div>
      </section>
    )
  }

  return (
    <section className="min-h-full w-full pb-4">
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <span className="shrink-0 text-[0.65rem] font-semibold tracking-widest text-slate-400 uppercase">
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

      <div className="space-y-4">
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

        <div className="space-y-4">
          {hasMutateOperation ? (
            <div className="flex flex-wrap items-center gap-3">
              <span className="shrink-0 text-[0.65rem] font-semibold tracking-widest text-slate-400 uppercase">
                Query Analysis
              </span>
              <OperationSelector operation={selectedOperation} onChange={setSelectedOperation} />
            </div>
          ) : null}

          <div className="grid min-h-0 min-w-0 items-start gap-4 2xl:grid-cols-[minmax(0,1.4fr)_minmax(36rem,0.6fr)]">
            <OperationTabs
              activeOperation={selectedOperation}
              suiteConfig={suiteConfig}
              querySummaries={filteredQuerySummaries}
              mutateSummaries={filteredMutateSummaries}
              querySteps={filteredQuerySteps}
              mutateSteps={filteredMutateSteps}
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
                querySummaries={
                  selectedOperation === "mutate" ? filteredMutateSummaries : filteredQuerySummaries
                }
                includedDatabases={includedDatabases}
                databaseColors={databaseColors}
                selection={selection}
                isLoading={isLoading}
              />
            </Suspense>
          </div>

          {showResourceTrendPanel || showFlameGraphPanel ? (
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
                    querySteps={filteredQuerySteps}
                    mutateSteps={filteredMutateSteps}
                    databases={includedDatabases}
                    isLoading={deferredLoading}
                  />
                ) : null}

                {showFlameGraphPanel ? (
                  <FlameGraphPanel
                    system={system}
                    suite={suiteId}
                    databases={includedDatabases}
                    metricSamples={state.metricSamples}
                    isLoading={deferredLoading}
                  />
                ) : null}
              </div>
            </Suspense>
          ) : null}
        </div>
      </div>
    </section>
  )
}
