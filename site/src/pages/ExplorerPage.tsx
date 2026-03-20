import { useMemo } from "react"

import { FlameGraphPanel } from "../components/explorer/FlameGraphPanel"
import { OperationTabs } from "../components/explorer/OperationTabs"
import { OverviewPanel } from "../components/explorer/OverviewPanel"
import { ResourceTrendPanel } from "../components/explorer/ResourceTrendPanel"
import { InsertPerformancePanel } from "../components/InsertPerformancePanel"
import { DashboardGrid, type WidgetConfig } from "../components/layout/DashboardGrid"
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
  const showInsertPerformancePanel = isLoading || state.insertSteps.length > 0
  const showFlameGraphPanel = isLoading || system !== null
  const showResourceTrendPanel = isLoading || state.metricSamples.length > 0

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

  const widgets: WidgetConfig[] = [
    {
      id: "overview",
      visible: isLoading || state.operationSummaries.length > 0,
      content: (
        <OverviewPanel
          suiteConfig={suiteConfig}
          databases={databases}
          includedDatabases={includedDatabases}
          operationSummaries={filteredOperationSummaries}
          isLoading={isLoading}
          onSelectAll={() => setSelectedDatabases(databases)}
          onToggleDatabase={toggleDatabase}
        />
      ),
    },
    {
      id: "insert-performance",
      visible: showInsertPerformancePanel,
      content: (
        <InsertPerformancePanel
          insertSteps={state.insertSteps}
          metricSamples={state.metricSamples}
          databases={includedDatabases}
          databaseColors={databaseColors}
          isLoading={isLoading}
        />
      ),
    },
    {
      id: "operations",
      visible: true,
      content: (
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
      ),
    },
    {
      id: "flame-graph",
      visible: showFlameGraphPanel,
      content: (
        <FlameGraphPanel
          system={system}
          suite={suiteId}
          databases={includedDatabases}
          metricSamples={state.metricSamples}
          isLoading={isLoading}
        />
      ),
    },
    {
      id: "resource-trends",
      visible: showResourceTrendPanel,
      content: (
        <ResourceTrendPanel
          suiteConfig={suiteConfig}
          metricSamples={state.metricSamples}
          insertSteps={state.insertSteps}
          querySteps={filteredQuerySteps}
          mutateSteps={filteredMutateSteps}
          databases={includedDatabases}
          isLoading={isLoading}
        />
      ),
    },
  ]

  return (
    <section className="min-h-full w-full pb-4">
      <DashboardGrid widgets={widgets} />
    </section>
  )
}
