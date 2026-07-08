import { useEffect, useState } from "react"

import { benchmarkDefinitions, type BenchmarkSuiteId } from "../lib/benchmarks"
import { fetchQueriesManifest, fetchQueryCoverage, fetchQuerySummaries } from "../lib/queries"
import { computeDatabaseScores, type DatabaseScore } from "../lib/score"
import { getSuiteConfig } from "../lib/suiteConfig"

export interface HomeDatabase {
  key: string
  label: string
  dbName: string
  dbVersion: string
}

export interface SuiteOverview {
  suiteId: BenchmarkSuiteId
  scores: DatabaseScore[]
}

export interface HomeOverview {
  loading: boolean
  error: string | null
  databases: HomeDatabase[]
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

    Promise.all([
      fetchQueriesManifest(),
      Promise.all(
        benchmarkDefinitions.map(async (definition) => {
          const [summaries, coverage] = await Promise.all([
            fetchQuerySummaries(system, definition.id, definition.defaultScaleFactor),
            fetchQueryCoverage(system, definition.id, definition.defaultScaleFactor),
          ])
          return { definition, summaries, coverage }
        }),
      ),
    ])
      .then(([queriesManifest, suiteRows]) =>
        suiteRows.map(({ definition, summaries, coverage }) => {
          const suiteConfig = getSuiteConfig(definition.id)
          const suiteQueries = queriesManifest[suiteConfig.queriesKey]
          if (!suiteQueries) {
            throw new Error(`queries.json is missing suite ${suiteConfig.queriesKey}`)
          }
          return {
            suiteId: definition.id,
            scores: computeDatabaseScores(summaries, coverage, Object.keys(suiteQueries)),
          }
        }),
      )
      .then((suites) => {
        if (cancelled) return

        const scoresByDbAndSuite = new Map<string, Map<BenchmarkSuiteId, DatabaseScore>>()
        const databasesByKey = new Map<string, { key: string; dbName: string; dbVersion: string }>()
        for (const suite of suites) {
          for (const entry of suite.scores) {
            databasesByKey.set(entry.dbKey, {
              key: entry.dbKey,
              dbName: entry.dbName,
              dbVersion: entry.dbVersion,
            })
            const inner = scoresByDbAndSuite.get(entry.dbKey) ?? new Map()
            inner.set(suite.suiteId, entry)
            scoresByDbAndSuite.set(entry.dbKey, inner)
          }
        }

        const worstBySuite = new Map<BenchmarkSuiteId, number>()
        for (const suite of suites) {
          const finite = suite.scores.map((s) => s.score).filter(Number.isFinite)
          if (finite.length > 0) worstBySuite.set(suite.suiteId, Math.max(...finite))
        }

        const overallScores = new Map<string, number>()
        for (const dbKey of databasesByKey.keys()) {
          let logSum = 0
          let count = 0
          for (const [suiteId, worst] of worstBySuite) {
            const entry = scoresByDbAndSuite.get(dbKey)?.get(suiteId)
            const score = entry && Number.isFinite(entry.score) ? entry.score : worst
            logSum += Math.log(score)
            count += 1
          }
          overallScores.set(dbKey, count > 0 ? Math.exp(logSum / count) : Number.POSITIVE_INFINITY)
        }

        const versionCountsByDb = new Map<string, Set<string>>()
        for (const database of databasesByKey.values()) {
          const versions = versionCountsByDb.get(database.dbName) ?? new Set<string>()
          versions.add(database.dbVersion)
          versionCountsByDb.set(database.dbName, versions)
        }

        setOverview({
          loading: false,
          error: null,
          databases: Array.from(databasesByKey.values())
            .map((database) => ({
              ...database,
              label:
                (versionCountsByDb.get(database.dbName)?.size ?? 0) > 1
                  ? `${database.dbName} ${database.dbVersion}`
                  : database.dbName,
            }))
            .sort((a, b) => {
              const diff =
                (overallScores.get(a.key) ?? Infinity) - (overallScores.get(b.key) ?? Infinity)
              return diff !== 0 ? diff : a.label.localeCompare(b.label)
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
