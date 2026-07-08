import { useEffect, useMemo, useState } from "react"

import type { BenchmarkSuiteId } from "../../lib/benchmarks"
import { cn } from "../../lib/cn"
import { getDatabaseColors } from "../../lib/databaseColors"
import { fetchCrossSystemQueryCoverage, fetchCrossSystemQuerySummaries } from "../../lib/queries"
import { computeSystemDatabaseScores, formatScore } from "../../lib/score"
import type { CrossSystemQueryCoverage, CrossSystemQuerySummary } from "../../lib/types"
import { PanelCard, PanelHeader } from "../layout/Panel"
import { Skeleton } from "../Skeleton"
import { MetaLabel, SectionTitle } from "../Typography"

interface CrossSystemComparisonPanelProps {
  suite: BenchmarkSuiteId
  suiteScaleFactor: number | null
  queryNames: string[]
  currentSystem: string | null
  isLoading: boolean
}

interface CrossSystemState {
  loading: boolean
  error: string | null
  querySummaries: CrossSystemQuerySummary[]
  queryCoverage: CrossSystemQueryCoverage[]
}

const INITIAL_STATE: CrossSystemState = {
  loading: false,
  error: null,
  querySummaries: [],
  queryCoverage: [],
}

export function CrossSystemComparisonPanel({
  suite,
  suiteScaleFactor,
  queryNames,
  currentSystem,
  isLoading,
}: CrossSystemComparisonPanelProps) {
  const [state, setState] = useState<CrossSystemState>(INITIAL_STATE)

  useEffect(() => {
    if (suiteScaleFactor === null || queryNames.length === 0) {
      setState(INITIAL_STATE)
      return
    }

    let cancelled = false
    setState({ ...INITIAL_STATE, loading: true })

    Promise.all([
      fetchCrossSystemQuerySummaries(suite, suiteScaleFactor),
      fetchCrossSystemQueryCoverage(suite, suiteScaleFactor),
    ])
      .then(([querySummaries, queryCoverage]) => {
        if (cancelled) return
        setState({
          loading: false,
          error: null,
          querySummaries,
          queryCoverage,
        })
      })
      .catch((error) => {
        if (cancelled) return
        setState({
          ...INITIAL_STATE,
          loading: false,
          error: String(error),
        })
      })

    return () => {
      cancelled = true
    }
  }, [queryNames.length, suite, suiteScaleFactor])

  const scores = useMemo(
    () => computeSystemDatabaseScores(state.querySummaries, state.queryCoverage, queryNames),
    [queryNames, state.queryCoverage, state.querySummaries],
  )
  const databaseColors = useMemo(() => getDatabaseColors(scores.map((score) => score.db)), [scores])
  const systemCount = useMemo(() => new Set(scores.map((score) => score.system)).size, [scores])
  const loading = isLoading || state.loading

  return (
    <PanelCard className="p-4">
      <PanelHeader>
        <div>
          <SectionTitle as="h3">Cross-system comparison</SectionTitle>
          <p className="mt-1 text-sm text-slate-400">
            Latest select run per system and database for this suite and scale.
          </p>
        </div>
        <MetaLabel className="shrink-0 tracking-normal text-slate-400 normal-case">
          {loading ? "Loading" : `${systemCount} ${systemCount === 1 ? "system" : "systems"}`}
        </MetaLabel>
      </PanelHeader>

      <div className="mt-4 overflow-x-auto rounded-lg bg-surface-inset">
        <table className="min-w-full table-fixed text-left text-sm">
          <colgroup>
            <col className="w-16" />
            <col className="w-44" />
            <col className="w-40" />
            <col className="w-28" />
            <col className="w-36" />
            <col className="w-36" />
          </colgroup>
          <thead className="border-b border-border-default text-[11px] font-semibold tracking-wider text-slate-400 uppercase">
            <tr>
              <th className="px-3 py-2">Rank</th>
              <th className="px-3 py-2">System</th>
              <th className="px-3 py-2">Database</th>
              <th className="px-3 py-2 text-right">Score</th>
              <th className="px-3 py-2 text-right">Coverage</th>
              <th className="px-3 py-2 text-right">Finished</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-default/70">
            {loading ? <LoadingRows /> : null}
            {!loading && state.error ? (
              <tr>
                <td colSpan={6} className="px-3 py-6 text-center text-sm text-red-300">
                  Failed to load cross-system results: {state.error}
                </td>
              </tr>
            ) : null}
            {!loading && !state.error && scores.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-3 py-6 text-center text-sm text-slate-400">
                  No select runs are available at this scale factor.
                </td>
              </tr>
            ) : null}
            {!loading && !state.error
              ? scores.map((score, index) => (
                  <tr
                    key={score.dbKey}
                    className={cn(
                      "text-slate-300",
                      score.system === currentSystem ? "bg-surface-raised/50" : null,
                    )}
                  >
                    <td className="px-3 py-2 font-mono text-xs text-slate-400">#{index + 1}</td>
                    <td className="px-3 py-2">
                      <span className="block truncate text-slate-100">{score.system}</span>
                    </td>
                    <td className="px-3 py-2">
                      <span className="flex min-w-0 items-center gap-2">
                        <span
                          aria-hidden
                          className="size-2 shrink-0 rounded-full"
                          style={{ backgroundColor: databaseColors[score.db] ?? "#94a3b8" }}
                        />
                        <span className="min-w-0 truncate">{score.db}</span>
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right font-mono font-semibold text-slate-50 tabular-nums">
                      {formatScore(score.score)}
                    </td>
                    <td className="px-3 py-2 text-right text-xs text-slate-400">
                      {formatCoverage(score.queryCount, score.missing, score.failed)}
                    </td>
                    <td className="px-3 py-2 text-right text-xs whitespace-nowrap text-slate-400">
                      {formatTimestamp(score.finishedAt)}
                    </td>
                  </tr>
                ))
              : null}
          </tbody>
        </table>
      </div>
    </PanelCard>
  )
}

function LoadingRows() {
  return Array.from({ length: 4 }, (_, index) => (
    <tr key={index}>
      <td colSpan={6} className="px-3 py-2">
        <Skeleton className="h-8 rounded-md" />
      </td>
    </tr>
  ))
}

function formatCoverage(completed: number, missing: number, failed: number): string {
  const total = completed + missing
  const failureSuffix = failed > 0 ? `, ${failed} failed` : ""
  return `${completed}/${total}${failureSuffix}`
}

function formatTimestamp(value: string): string {
  return value.slice(0, 16)
}
