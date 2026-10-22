import { useEffect, useState } from "react"

import { fetchComparisonData, type ComparisonData } from "../lib/observationReader"

export function useComparisonData() {
  const [state, setState] = useState<{
    data: ComparisonData | null
    loading: boolean
    error: string | null
  }>({ data: null, loading: true, error: null })
  useEffect(() => {
    let cancelled = false
    void fetchComparisonData().then(
      (data) => {
        if (!cancelled) setState({ data, loading: false, error: null })
      },
      (error: unknown) => {
        if (!cancelled) setState({ data: null, loading: false, error: String(error) })
      },
    )
    return () => {
      cancelled = true
    }
  }, [])
  return state
}
