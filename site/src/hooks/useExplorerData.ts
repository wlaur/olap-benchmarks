import { useEffect, useState } from "react"

import type { BenchmarkSuiteId } from "../lib/benchmarks"
import { fetchExplorerQueryMetrics, fetchQueriesManifest } from "../lib/queries"
import type { ExplorerQueryMetric, QueriesManifest } from "../lib/types"

interface ExplorerDataState {
  metrics: ExplorerQueryMetric[]
  queriesManifest: QueriesManifest
  loading: boolean
  error: string | null
}

export function useExplorerData(suite: BenchmarkSuiteId): ExplorerDataState {
  const [state, setState] = useState<ExplorerDataState>({
    metrics: [],
    queriesManifest: {},
    loading: true,
    error: null,
  })

  useEffect(() => {
    let cancelled = false
    setState({ metrics: [], queriesManifest: {}, loading: true, error: null })

    void Promise.all([fetchExplorerQueryMetrics(suite), fetchQueriesManifest()])
      .then(([metrics, queriesManifest]) => {
        if (!cancelled) {
          setState({ metrics, queriesManifest, loading: false, error: null })
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setState({ metrics: [], queriesManifest: {}, loading: false, error: String(error) })
        }
      })

    return () => {
      cancelled = true
    }
  }, [suite])

  return state
}
