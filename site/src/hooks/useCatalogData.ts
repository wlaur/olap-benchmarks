import { useEffect, useState } from "react"

import { fetchQueriesManifest } from "../lib/queries"
import type { QueriesManifest } from "../lib/types"
import { useComparisonData } from "./useComparisonData"

export function useCatalogData() {
  const comparison = useComparisonData()
  const [queries, setQueries] = useState<{
    manifest: QueriesManifest
    error: string | null
  } | null>(null)
  useEffect(() => {
    let cancelled = false
    void fetchQueriesManifest().then(
      (manifest) => {
        if (!cancelled) setQueries({ manifest, error: null })
      },
      (error: unknown) => {
        if (!cancelled) setQueries({ manifest: {}, error: String(error) })
      },
    )
    return () => {
      cancelled = true
    }
  }, [])
  return {
    data: comparison.data,
    dimensions:
      comparison.data?.runs.map((run) => ({
        system: run.system,
        suite: run.suite,
        suite_scale_factor: run.scale,
        db: run.db,
        db_version: run.version,
        db_driver: run.driver,
      })) ?? [],
    queriesManifest: queries?.manifest ?? {},
    loading: comparison.loading || queries === null,
    error: comparison.error ?? queries?.error ?? null,
  }
}
