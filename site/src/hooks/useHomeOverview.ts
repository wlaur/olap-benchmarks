import { useMemo } from "react"

import { compareFamily } from "../lib/observations"
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
  return {
    families,
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
