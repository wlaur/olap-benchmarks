import { useEffect, useState } from "react"

import type { AnalyticalRun } from "../lib/analytical"
import { fetchAnalyticalRuns } from "../lib/analyticalReader"

export function useAnalyticalData() {
  const [state, setState] = useState<{
    runs: AnalyticalRun[]
    loading: boolean
    error: string | null
  }>({ runs: [], loading: true, error: null })
  useEffect(() => {
    let cancelled = false
    void fetchAnalyticalRuns().then(
      (runs) => {
        if (!cancelled) setState({ runs, loading: false, error: null })
      },
      (error: unknown) => {
        if (!cancelled) setState({ runs: [], loading: false, error: String(error) })
      },
    )
    return () => {
      cancelled = true
    }
  }, [])
  return state
}
