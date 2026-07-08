import type { SelectionState } from "../../hooks/useSelectionState"
import type { SuiteConfig } from "../../lib/suiteConfig"
import type {
  QueriesManifest,
  QueryAnalysisOperation,
  QueryStep,
  QuerySummary,
} from "../../lib/types"
import { OperationTab } from "./OperationTab"

interface OperationTabsProps {
  activeOperation: QueryAnalysisOperation
  suiteConfig: SuiteConfig
  querySummaries: QuerySummary[]
  mutateSummaries: QuerySummary[]
  concurrentSummaries: QuerySummary[]
  querySteps: QueryStep[]
  mutateSteps: QueryStep[]
  concurrentSteps: QueryStep[]
  databases: string[]
  includedDatabases: string[]
  queriesManifest: QueriesManifest | null
  selection: SelectionState
  isLoading: boolean
  isTimelineLoading: boolean
}

export function OperationTabs({
  activeOperation,
  suiteConfig,
  querySummaries,
  mutateSummaries,
  concurrentSummaries,
  querySteps,
  mutateSteps,
  concurrentSteps,
  databases,
  includedDatabases,
  queriesManifest,
  selection,
  isLoading,
  isTimelineLoading,
}: OperationTabsProps) {
  const resolvedTab: QueryAnalysisOperation =
    activeOperation !== "select" && suiteConfig.operations.includes(activeOperation)
      ? activeOperation
      : "select"
  const resolvedSummaries =
    resolvedTab === "mutate"
      ? mutateSummaries
      : resolvedTab === "concurrent"
        ? concurrentSummaries
        : querySummaries
  const resolvedSteps =
    resolvedTab === "mutate"
      ? mutateSteps
      : resolvedTab === "concurrent"
        ? concurrentSteps
        : querySteps

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="min-h-0 flex-1">
        <OperationTab
          operation={resolvedTab}
          suiteConfig={suiteConfig}
          querySummaries={resolvedSummaries}
          querySteps={resolvedSteps}
          databases={databases}
          includedDatabases={includedDatabases}
          queriesManifest={queriesManifest}
          selection={selection}
          isLoading={isLoading}
          isTimelineLoading={isTimelineLoading}
        />
      </div>
    </div>
  )
}
