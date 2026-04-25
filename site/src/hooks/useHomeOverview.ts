import { useEffect, useState } from "react"

import { benchmarkDefinitions, type BenchmarkSuiteId } from "../lib/benchmarks"
import { fetchQuerySummaries } from "../lib/queries"
import { computeDatabaseScores, type DatabaseScore } from "../lib/score"

export interface SuiteOverview {
  suiteId: BenchmarkSuiteId
  scores: DatabaseScore[]
}

export interface HomeOverview {
  loading: boolean
  error: string | null
  databases: string[]
  suites: SuiteOverview[]
  scoresByDbAndSuite: Map<string, Map<BenchmarkSuiteId, DatabaseScore>>
}

const EMPTY_OVERVIEW: HomeOverview = {
  loading: true,
  error: null,
  databases: [],
  suites: [],
  scoresByDbAndSuite: new Map(),
}

export function useHomeOverview(system: string | null): HomeOverview {
  const [overview, setOverview] = useState<HomeOverview>(EMPTY_OVERVIEW)

  useEffect(() => {
    if (system === null) {
      setOverview(EMPTY_OVERVIEW)
      return
    }

    let cancelled = false
    setOverview({ ...EMPTY_OVERVIEW, loading: true })

    Promise.all(
      benchmarkDefinitions.map(async (definition) => {
        const summaries = await fetchQuerySummaries(system, definition.id)
        return { suiteId: definition.id, scores: computeDatabaseScores(summaries) }
      }),
    )
      .then((suites) => {
        if (cancelled) return

        const scoresByDbAndSuite = new Map<string, Map<BenchmarkSuiteId, DatabaseScore>>()
        const dbSet = new Set<string>()
        for (const suite of suites) {
          for (const entry of suite.scores) {
            dbSet.add(entry.db)
            const inner = scoresByDbAndSuite.get(entry.db) ?? new Map()
            inner.set(suite.suiteId, entry)
            scoresByDbAndSuite.set(entry.db, inner)
          }
        }

        setOverview({
          loading: false,
          error: null,
          databases: Array.from(dbSet).sort(),
          suites,
          scoresByDbAndSuite,
        })
      })
      .catch((error) => {
        if (cancelled) return
        setOverview({ ...EMPTY_OVERVIEW, loading: false, error: String(error) })
      })

    return () => {
      cancelled = true
    }
  }, [system])

  return overview
}
