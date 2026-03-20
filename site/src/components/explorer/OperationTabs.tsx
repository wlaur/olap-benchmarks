import { useState } from "react"

import type { SuiteConfig } from "../../lib/suiteConfig"
import type { QueriesManifest, QueryStep, QuerySummary } from "../../lib/types"
import { OperationTab } from "./OperationTab"

interface OperationTabsProps {
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
  const hasMutate = suiteConfig.operations.includes("mutate")
  const tabs: TabOperation[] = hasMutate ? ["mutate", "select"] : ["select"]
  const [activeTab, setActiveTab] = useState<TabOperation>("select")

  const resolvedTab = tabs.includes(activeTab) ? activeTab : "select"

  return (
    <div className="flex h-full min-h-0 flex-col">
      {tabs.length > 1 ? (
        <div className="mb-2 grid shrink-0 grid-cols-2 gap-0 rounded-lg border border-border-default bg-surface-inset p-0.5">
          {tabs.map((tab) => (
            <button
              key={tab}
              type="button"
              onClick={() => setActiveTab(tab)}
              className={
                resolvedTab === tab
                  ? "rounded-md bg-accent-500/15 py-2 text-sm font-semibold text-accent-200 shadow-[inset_0_0_0_1px_rgba(90,151,255,0.25)]"
                  : "rounded-md py-2 text-sm font-medium text-slate-400 transition-colors hover:text-slate-200"
              }
            >
              {tab === "select" ? "Select queries" : "Mutate queries"}
            </button>
          ))}
        </div>
      ) : null}

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
