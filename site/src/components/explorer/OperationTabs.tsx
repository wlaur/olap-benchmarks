import type { SuiteConfig } from "../../lib/suiteConfig"
import type { QueriesManifest, QueryStep, QuerySummary } from "../../lib/types"
import { OperationTab } from "./OperationTab"

interface OperationTabsProps {
  activeOperation: "select" | "mutate"
  suiteConfig: SuiteConfig
  querySummaries: QuerySummary[]
  mutateSummaries: QuerySummary[]
  querySteps: QueryStep[]
  mutateSteps: QueryStep[]
  databases: string[]
  includedDatabases: string[]
  queriesManifest: QueriesManifest | null
  isLoading: boolean
}

type TabOperation = "select" | "mutate"

export function OperationTabs({
  activeOperation,
  suiteConfig,
  querySummaries,
  mutateSummaries,
  querySteps,
  mutateSteps,
  databases,
  includedDatabases,
  queriesManifest,
  isLoading,
}: OperationTabsProps) {
  const resolvedTab: TabOperation =
    activeOperation === "mutate" && suiteConfig.operations.includes("mutate") ? "mutate" : "select"

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="min-h-0 flex-1">
        {resolvedTab === "select" ? (
          <OperationTab
            operation="select"
            suiteConfig={suiteConfig}
            querySummaries={querySummaries}
            querySteps={querySteps}
            databases={databases}
            includedDatabases={includedDatabases}
            queriesManifest={queriesManifest}
            isLoading={isLoading}
          />
        ) : (
          <OperationTab
            operation="mutate"
            suiteConfig={suiteConfig}
            querySummaries={mutateSummaries}
            querySteps={mutateSteps}
            databases={databases}
            includedDatabases={includedDatabases}
            queriesManifest={queriesManifest}
            isLoading={isLoading}
          />
        )}
      </div>
    </div>
  )
}
