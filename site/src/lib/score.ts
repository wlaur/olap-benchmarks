import type { QueryCoverage, QuerySummary } from "./types"

export interface DatabaseScore {
  dbKey: string
  db: string
  dbName: string
  dbVersion: string
  score: number
  queryCount: number
  wins: number
  missing: number
  failed: number
  neverCompleted: number
  latestSelectFailed: boolean
}

export interface RelativeScoreSeries {
  key: string
  queryValues: ReadonlyMap<string, number>
}

export interface RelativeSeriesScore {
  key: string
  score: number
  queryCount: number
  wins: number
  missing: number
}

const SMOOTHING_SECONDS = 0.01
const MISSING_QUERY_MIN_RATIO = 10
const MISSING_QUERY_WORST_MULTIPLIER = 2
const DURATION_EQUALITY_TOLERANCE = 1e-9

export function databaseVariantKey(dbName: string, dbVersion: string): string {
  return JSON.stringify([dbName, dbVersion])
}

export function computeDatabaseScores(
  querySummaries: QuerySummary[],
  queryCoverage: QueryCoverage[],
  queryNames: string[],
): DatabaseScore[] {
  if (querySummaries.length === 0 && queryCoverage.length === 0) return []

  const queryGroups = new Map<string, Map<string, number>>()
  const databases = new Map<string, { db: string; dbName: string; dbVersion: string }>()
  const coverageByDb = new Map(
    queryCoverage.map((row) => [databaseVariantKey(row.db_name, row.db_version), row]),
  )
  const scoredQueryNames = new Set(queryNames)

  for (const row of querySummaries) {
    const dbKey = databaseVariantKey(row.db_name, row.db_version)
    databases.set(dbKey, { db: row.db, dbName: row.db_name, dbVersion: row.db_version })
    scoredQueryNames.add(row.query_name)
    const group = queryGroups.get(row.query_name) ?? new Map<string, number>()
    group.set(dbKey, row.median_duration_s)
    queryGroups.set(row.query_name, group)
  }
  for (const row of queryCoverage) {
    databases.set(databaseVariantKey(row.db_name, row.db_version), {
      db: row.db,
      dbName: row.db_name,
      dbVersion: row.db_version,
    })
  }

  const dbStats = calculateRelativeScores(
    [...databases.keys()],
    [...scoredQueryNames],
    queryGroups,
    SMOOTHING_SECONDS,
  )

  return Array.from(databases)
    .map(([dbKey, database]) => {
      const stats = dbStats.get(dbKey)!
      const coverage = coverageByDb.get(dbKey)
      if (!coverage) {
        throw new Error(`query coverage is missing for ${database.db}`)
      }
      return {
        dbKey,
        db: database.db,
        dbName: database.dbName,
        dbVersion: database.dbVersion,
        score: stats.score,
        queryCount: stats.queryCount,
        wins: stats.wins,
        missing: stats.missing,
        failed: coverage.failed_query_count,
        neverCompleted: Math.max(0, stats.missing - coverage.failed_query_count),
        latestSelectFailed: coverage.latest_status === "failed",
      }
    })
    .sort((a, b) => {
      const leftFinite = Number.isFinite(a.score)
      const rightFinite = Number.isFinite(b.score)
      if (leftFinite && !rightFinite) return -1
      if (!leftFinite && rightFinite) return 1

      const scoreDelta = a.score - b.score
      if (scoreDelta !== 0 && Number.isFinite(scoreDelta)) return scoreDelta
      return a.db.localeCompare(b.db)
    })
}

export function computeRelativeSeriesScores(
  series: readonly RelativeScoreSeries[],
  queryNames: readonly string[],
  smoothingDuration: number,
): RelativeSeriesScore[] {
  const scoredQueryNames = new Set(queryNames)
  const queryGroups = new Map<string, Map<string, number>>()
  for (const entry of series) {
    for (const [queryName, duration] of entry.queryValues) {
      scoredQueryNames.add(queryName)
      const group = queryGroups.get(queryName) ?? new Map<string, number>()
      group.set(entry.key, duration)
      queryGroups.set(queryName, group)
    }
  }

  const scores = calculateRelativeScores(
    series.map((entry) => entry.key),
    [...scoredQueryNames],
    queryGroups,
    smoothingDuration,
  )
  return series.map((entry) => scores.get(entry.key)!)
}

function calculateRelativeScores(
  seriesKeys: readonly string[],
  queryNames: readonly string[],
  queryGroups: ReadonlyMap<string, ReadonlyMap<string, number>>,
  smoothingDuration: number,
): Map<string, RelativeSeriesScore> {
  const statsBySeries = new Map<
    string,
    { logSum: number; scoredCount: number; queryCount: number; wins: number }
  >()
  for (const key of seriesKeys) {
    statsBySeries.set(key, { logSum: 0, scoredCount: 0, queryCount: 0, wins: 0 })
  }

  for (const queryName of queryNames) {
    const group = queryGroups.get(queryName) ?? new Map<string, number>()
    const times = [...group.values()]
    const ratios = new Map<string, number>()
    const minTime = times.length > 0 ? Math.min(...times) : null
    let missingRatio = MISSING_QUERY_MIN_RATIO

    if (minTime !== null) {
      const minSmoothed = minTime + smoothingDuration
      for (const [key, time] of group) {
        ratios.set(key, (time + smoothingDuration) / minSmoothed)
      }
      missingRatio = Math.max(
        MISSING_QUERY_MIN_RATIO,
        MISSING_QUERY_WORST_MULTIPLIER * Math.max(...ratios.values()),
      )
    }

    for (const key of seriesKeys) {
      const stats = statsBySeries.get(key)!
      const time = group.get(key)
      stats.logSum += Math.log(ratios.get(key) ?? missingRatio)
      stats.scoredCount += 1
      if (time === undefined) continue
      stats.queryCount += 1
      if (minTime !== null && durationsAreEqual(time, minTime)) stats.wins += 1
    }
  }

  return new Map(
    seriesKeys.map((key) => {
      const stats = statsBySeries.get(key)!
      return [
        key,
        {
          key,
          score:
            stats.scoredCount > 0
              ? Math.exp(stats.logSum / stats.scoredCount)
              : Number.POSITIVE_INFINITY,
          queryCount: stats.queryCount,
          wins: stats.wins,
          missing: queryNames.length - stats.queryCount,
        },
      ]
    }),
  )
}

function durationsAreEqual(left: number, right: number): boolean {
  return Math.abs(left - right) <= DURATION_EQUALITY_TOLERANCE
}

export function formatScore(score: number): string {
  if (!Number.isFinite(score)) return "—"
  if (score < 100) return `${score.toFixed(1)}×`
  return `${Math.round(score)}×`
}
