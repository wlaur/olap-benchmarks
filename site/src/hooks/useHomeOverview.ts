import { useMemo } from "react"

import { compareFamily } from "../lib/observations"
import { aggregateOverview } from "../lib/overall"
import { useComparisonData } from "./useComparisonData"

export function useHomeOverview(system: string | null) {
  const { data, loading, error } = useComparisonData()
  const families = useMemo(
    () =>
      data && system
        ? data.manifest.families.map((family) => ({
            family,
            scales: family.scale_factors.map((scale) => ({
              scale,
              rows: compareFamily(family, scale, system, data.runs, data.manifest),
            })),
          }))
        : [],
    [data, system],
  )
  const aggregation = useMemo(() => {
    try {
      return { overall: aggregateOverview(families, data?.manifest ?? { version: 1 }), error: null }
    } catch (cause: unknown) {
      return { overall: aggregateOverview([], { version: 1 }), error: String(cause) }
    }
  }, [families, data])
  return {
    families,
    overall: aggregation.overall,
    overallError: aggregation.error,
    loading,
    error,
    retrospective: data?.manifest.retrospective ?? null,
    snapshotCount: new Set(
      data?.runs
        .filter((run) => run.system === system && run.snapshotId !== null)
        .map((run) => run.snapshotId),
    ).size,
  }
}
