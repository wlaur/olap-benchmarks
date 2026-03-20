import { useState } from "react"

import type { SuiteConfig } from "../../lib/suiteConfig"
import type { MetricSample, QueriesManifest, QueryStep, QuerySummary } from "../../lib/types"
import { OperationTab } from "./OperationTab"

interface OperationTabsProps {
  suiteConfig: SuiteConfig
  querySummaries: QuerySummary[]
  mutateSummaries: QuerySummary[]
  querySteps: QueryStep[]
  mutateSteps: QueryStep[]
  metricSamples: MetricSample[]
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
  metricSamples,
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
    <div className="flex flex-col gap-4">
      {tabs.length > 1 ? (
        <div className="inline-flex self-start rounded-full border border-border-default bg-surface-raised p-1">
          {tabs.map((tab) => (
            <button
              key={tab}
              type="button"
              onClick={() => setActiveTab(tab)}
              className={
                resolvedTab === tab
                  ? "rounded-full bg-sky-500/10 px-4 py-2 text-sm font-medium text-sky-100 shadow-[inset_0_0_0_1px_rgba(96,165,250,0.28)]"
                  : "rounded-full px-4 py-2 text-sm font-medium text-slate-400 transition-colors hover:text-slate-200"
              }
            >
              {tab === "select" ? "Select" : "Mutate"}
            </button>
          ))}
        </div>
      ) : null}

      {resolvedTab === "select" ? (
        <OperationTab
          operation="select"
          suiteConfig={suiteConfig}
          querySummaries={querySummaries}
          querySteps={querySteps}
          metricSamples={metricSamples}
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
          metricSamples={metricSamples}
          databases={databases}
          includedDatabases={includedDatabases}
          queriesManifest={queriesManifest}
          isLoading={isLoading}
        />
      )}
    </div>
  )
}
