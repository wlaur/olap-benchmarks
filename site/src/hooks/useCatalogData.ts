import { useEffect, useState } from "react"

import { fetchCatalogRunDimensions, fetchQueriesManifest } from "../lib/queries"
import type { CatalogRunDimension, QueriesManifest } from "../lib/types"

interface CatalogDataState {
  dimensions: CatalogRunDimension[]
  queriesManifest: QueriesManifest
  loading: boolean
  error: string | null
}

const EMPTY_QUERIES_MANIFEST: QueriesManifest = {}

export function useCatalogData(): CatalogDataState {
  const [state, setState] = useState<CatalogDataState>({
    dimensions: [],
    queriesManifest: EMPTY_QUERIES_MANIFEST,
    loading: true,
    error: null,
  })

  useEffect(() => {
    let cancelled = false

    void Promise.all([fetchCatalogRunDimensions(), fetchQueriesManifest()])
      .then(([dimensions, queriesManifest]) => {
        if (!cancelled) {
          setState({ dimensions, queriesManifest, loading: false, error: null })
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setState((current) => ({ ...current, loading: false, error: String(error) }))
        }
      })

    return () => {
      cancelled = true
    }
  }, [])

  return state
}
