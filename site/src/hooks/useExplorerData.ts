import { useEffect, useState } from "react"

import type { BenchmarkSuiteId } from "../lib/benchmarks"
import { median } from "../lib/observations"
import { fetchQueriesManifest } from "../lib/queries"
import type { ExplorerQueryMetric, QueriesManifest } from "../lib/types"
import { useComparisonData } from "./useComparisonData"

export function useExplorerData(suite: BenchmarkSuiteId) {
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
  const runs = comparison.data?.runs.filter((run) => run.suite === suite) ?? []
  const metrics: ExplorerQueryMetric[] = runs.flatMap((run) =>
    [...run.queries].flatMap(([query_name, steps]) => {
      const duration = median(
        steps.flatMap((step) =>
          step.role === "measured" &&
          step.resultStatus === "ok" &&
          step.status === "completed" &&
          step.durationS !== null &&
          Number.isFinite(step.durationS) &&
          step.durationS > 0
            ? [step.durationS]
            : [],
        ),
      )
      return duration === null
        ? []
        : [
            {
              run_id: run.runId,
              system: run.system,
              suite_scale_factor: run.scale,
              db: run.db,
              db_version: run.version,
              db_driver: run.driver,
              system_snapshot_id: run.snapshotId,
              finished_at: run.finishedAt,
              query_name,
              median_duration_s: duration,
            },
          ]
    }),
  )
  return {
    metrics,
    runs,
    comparison: comparison.data?.manifest ?? null,
    queriesManifest: queries?.manifest ?? {},
    loading: comparison.loading || queries === null,
    error: comparison.error ?? queries?.error ?? null,
  }
}
