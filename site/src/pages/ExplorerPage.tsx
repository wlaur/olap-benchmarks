import { useMemo } from "react"

import { OperationTabs } from "../components/explorer/OperationTabs"
import { OverviewPanel } from "../components/explorer/OverviewPanel"
import { ResourceTrendPanel } from "../components/explorer/ResourceTrendPanel"
import { InsertPerformancePanel } from "../components/InsertPerformancePanel"
import { useSuiteData } from "../hooks/useSuiteData"
import type { BenchmarkSuiteId } from "../lib/benchmarks"
import { getDatabaseColors } from "../lib/databaseColors"
import { getSuiteConfig } from "../lib/suiteConfig"

interface ExplorerPageProps {
  system: string | null
  suiteId: BenchmarkSuiteId
  isSystemLoading?: boolean
}

export function ExplorerPage({ system, suiteId, isSystemLoading = false }: ExplorerPageProps) {
  const suiteConfig = useMemo(() => getSuiteConfig(suiteId), [suiteId])

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
    <section className="flex min-h-full w-full flex-col gap-4 pb-4">
      <OverviewPanel
        suiteConfig={suiteConfig}
        databases={databases}
        includedDatabases={includedDatabases}
        operationSummaries={filteredOperationSummaries}
        isLoading={isLoading}
        onSelectAll={() => setSelectedDatabases(databases)}
        onToggleDatabase={toggleDatabase}
      />

      {!isLoading && state.insertSteps.length > 0 ? (
        <InsertPerformancePanel
          insertSteps={state.insertSteps}
          metricSamples={state.metricSamples}
          databases={includedDatabases}
          databaseColors={databaseColors}
        />
      ) : null}

      <OperationTabs
        suiteConfig={suiteConfig}
        querySummaries={filteredQuerySummaries}
        mutateSummaries={filteredMutateSummaries}
        querySteps={filteredQuerySteps}
        mutateSteps={filteredMutateSteps}
        metricSamples={state.metricSamples}
        databases={databases}
        includedDatabases={includedDatabases}
        queriesManifest={state.queriesManifest}
        isLoading={isLoading}
      />

      {!isLoading && state.metricSamples.length > 0 ? (
        <ResourceTrendPanel
          suiteConfig={suiteConfig}
          metricSamples={state.metricSamples}
          insertSteps={state.insertSteps}
          querySteps={filteredQuerySteps}
          mutateSteps={filteredMutateSteps}
          databases={includedDatabases}
          stepMetricAvailability={state.stepMetricAvailability}
        />
      ) : null}
    </section>
  )
}
