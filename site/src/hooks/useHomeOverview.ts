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
        const summaries = await fetchQuerySummaries(
          system,
          definition.id,
          definition.defaultScaleFactor,
        )
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

        const worstBySuite = new Map<BenchmarkSuiteId, number>()
        for (const suite of suites) {
          const finite = suite.scores.map((s) => s.score).filter(Number.isFinite)
          if (finite.length > 0) worstBySuite.set(suite.suiteId, Math.max(...finite))
        }

        const overallScores = new Map<string, number>()
        for (const db of dbSet) {
          let logSum = 0
          let count = 0
          for (const [suiteId, worst] of worstBySuite) {
            const entry = scoresByDbAndSuite.get(db)?.get(suiteId)
            const score = entry && Number.isFinite(entry.score) ? entry.score : worst
            logSum += Math.log(score)
            count += 1
          }
          overallScores.set(db, count > 0 ? Math.exp(logSum / count) : Number.POSITIVE_INFINITY)
        }

        setOverview({
          loading: false,
          error: null,
          databases: Array.from(dbSet).sort((a, b) => {
            const diff = (overallScores.get(a) ?? Infinity) - (overallScores.get(b) ?? Infinity)
            return diff !== 0 ? diff : a.localeCompare(b)
          }),
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
