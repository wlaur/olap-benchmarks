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

const EMPTY_STATE: ExplorerDataState = {
  metrics: [],
  queriesManifest: {},
  loading: true,
  error: null,
}

export function useExplorerData(suite: BenchmarkSuiteId): ExplorerDataState {
  const [state, setState] = useState<{
    suite: BenchmarkSuiteId
    data: ExplorerDataState
  } | null>(null)

  useEffect(() => {
    let cancelled = false

    void Promise.all([fetchExplorerQueryMetrics(suite), fetchQueriesManifest()])
      .then(([metrics, queriesManifest]) => {
        if (!cancelled) {
          setState({ suite, data: { metrics, queriesManifest, loading: false, error: null } })
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setState({ suite, data: { ...EMPTY_STATE, loading: false, error: String(error) } })
        }
      })

    return () => {
      cancelled = true
    }
  }, [suite])

  return state?.suite === suite ? state.data : EMPTY_STATE
}
