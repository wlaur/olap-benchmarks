import { useEffect, useMemo, useState } from "react"

import { FilterChipsSkeleton } from "../components/explorer/ExplorerSkeletons"
import { FlameGraphPanel } from "../components/explorer/FlameGraphPanel"
import { OperationSelector } from "../components/explorer/OperationSelector"
import { OperationTabs } from "../components/explorer/OperationTabs"
import { OverviewPanel } from "../components/explorer/OverviewPanel"
import { ResourceTrendPanel } from "../components/explorer/ResourceTrendPanel"
import { DatabaseMultiSelect } from "../components/filters/DatabaseMultiSelect"
import { InsertPerformancePanel } from "../components/InsertPerformancePanel"
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

  const databaseColors = getDatabaseColors(databases)
  const showInsertPerformancePanel = isLoading || state.insertSteps.length > 0
  const showFlameGraphPanel = isLoading || system !== null
  const showResourceTrendPanel = isLoading || state.metricSamples.length > 0

  useEffect(() => {
    if (!hasMutateOperation) {
      setSelectedOperation("select")
    }
  }, [hasMutateOperation])

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
              isLoading={isLoading}
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
            isLoading={isLoading}
          />

          {showResourceTrendPanel || showFlameGraphPanel ? (
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
                  isLoading={isLoading}
                />
              ) : null}

              {showFlameGraphPanel ? (
                <FlameGraphPanel
                  system={system}
                  suite={suiteId}
                  databases={includedDatabases}
                  metricSamples={state.metricSamples}
                  isLoading={isLoading}
                />
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </section>
  )
}
