import type {
  CrossSystemQueryCoverage,
  CrossSystemQuerySummary,
  QueryCoverage,
  QuerySummary,
} from "./types"

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

export interface SystemDatabaseScore extends DatabaseScore {
  system: string
  startedAt: string
  finishedAt: string
}

const SMOOTHING_SECONDS = 0.01
const MISSING_QUERY_MIN_RATIO = 10
const MISSING_QUERY_WORST_MULTIPLIER = 2
const DURATION_EQUALITY_TOLERANCE = 1e-9

export function databaseVariantKey(dbName: string, dbVersion: string): string {
  return JSON.stringify([dbName, dbVersion])
}

export function systemDatabaseVariantKey(
  system: string,
  dbName: string,
  dbVersion: string,
): string {
  return JSON.stringify([system, dbName, dbVersion])
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

  const dbStats = new Map<
    string,
    { logSum: number; scoredCount: number; queryCount: number; wins: number }
  >()
  for (const dbKey of databases.keys()) {
    dbStats.set(dbKey, { logSum: 0, scoredCount: 0, queryCount: 0, wins: 0 })
  }

  for (const queryName of scoredQueryNames) {
    const group = queryGroups.get(queryName) ?? new Map<string, number>()
    const times = Array.from(group.values())
    const ratios = new Map<string, number>()
    const minTime = times.length > 0 ? Math.min(...times) : null
    let missingRatio = MISSING_QUERY_MIN_RATIO

    if (minTime !== null) {
      const minSmoothed = minTime + SMOOTHING_SECONDS

      for (const [dbKey, time] of group) {
        ratios.set(dbKey, (time + SMOOTHING_SECONDS) / minSmoothed)
      }

      missingRatio = Math.max(
        MISSING_QUERY_MIN_RATIO,
        MISSING_QUERY_WORST_MULTIPLIER * Math.max(...ratios.values()),
      )
    }

    for (const dbKey of databases.keys()) {
      const stats = dbStats.get(dbKey)!
      const time = group.get(dbKey)
      const ratio = ratios.get(dbKey) ?? missingRatio
      stats.logSum += Math.log(ratio)
      stats.scoredCount += 1
      if (time === undefined) continue
      stats.queryCount += 1
      if (minTime !== null && durationsAreEqual(time, minTime)) stats.wins += 1
    }
  }

  const totalQueries = scoredQueryNames.size

  return Array.from(databases)
    .map(([dbKey, database]) => {
      const stats = dbStats.get(dbKey)!
      const coverage = coverageByDb.get(dbKey)
      if (!coverage) {
        throw new Error(`query coverage is missing for ${database.db}`)
      }
      const score =
        stats.scoredCount > 0
          ? Math.exp(stats.logSum / stats.scoredCount)
          : Number.POSITIVE_INFINITY
      return {
        dbKey,
        db: database.db,
        dbName: database.dbName,
        dbVersion: database.dbVersion,
        score,
        queryCount: stats.queryCount,
        wins: stats.wins,
        missing: totalQueries - stats.queryCount,
        failed: coverage.failed_query_count,
        neverCompleted: Math.max(0, totalQueries - stats.queryCount - coverage.failed_query_count),
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

export function computeSystemDatabaseScores(
  querySummaries: CrossSystemQuerySummary[],
  queryCoverage: CrossSystemQueryCoverage[],
  queryNames: string[],
): SystemDatabaseScore[] {
  if (querySummaries.length === 0 && queryCoverage.length === 0) return []

  const queryGroups = new Map<string, Map<string, number>>()
  const databases = new Map<
    string,
    { db: string; dbName: string; dbVersion: string; system: string }
  >()
  const coverageByDb = new Map(
    queryCoverage.map((row) => [
      systemDatabaseVariantKey(row.system, row.db_name, row.db_version),
      row,
    ]),
  )
  const scoredQueryNames = new Set(queryNames)

  for (const row of querySummaries) {
    const dbKey = systemDatabaseVariantKey(row.system, row.db_name, row.db_version)
    databases.set(dbKey, {
      db: row.db,
      dbName: row.db_name,
      dbVersion: row.db_version,
      system: row.system,
    })
    scoredQueryNames.add(row.query_name)
    const group = queryGroups.get(row.query_name) ?? new Map<string, number>()
    group.set(dbKey, row.median_duration_s)
    queryGroups.set(row.query_name, group)
  }
  for (const row of queryCoverage) {
    databases.set(systemDatabaseVariantKey(row.system, row.db_name, row.db_version), {
      db: row.db,
      dbName: row.db_name,
      dbVersion: row.db_version,
      system: row.system,
    })
  }

  const dbStats = new Map<
    string,
    { logSum: number; scoredCount: number; queryCount: number; wins: number }
  >()
  for (const dbKey of databases.keys()) {
    dbStats.set(dbKey, { logSum: 0, scoredCount: 0, queryCount: 0, wins: 0 })
  }

  for (const queryName of scoredQueryNames) {
    const group = queryGroups.get(queryName) ?? new Map<string, number>()
    const times = Array.from(group.values())
    const ratios = new Map<string, number>()
    const minTime = times.length > 0 ? Math.min(...times) : null
    let missingRatio = MISSING_QUERY_MIN_RATIO

    if (minTime !== null) {
      const minSmoothed = minTime + SMOOTHING_SECONDS

      for (const [dbKey, time] of group) {
        ratios.set(dbKey, (time + SMOOTHING_SECONDS) / minSmoothed)
      }

      missingRatio = Math.max(
        MISSING_QUERY_MIN_RATIO,
        MISSING_QUERY_WORST_MULTIPLIER * Math.max(...ratios.values()),
      )
    }

    for (const dbKey of databases.keys()) {
      const stats = dbStats.get(dbKey)!
      const time = group.get(dbKey)
      const ratio = ratios.get(dbKey) ?? missingRatio
      stats.logSum += Math.log(ratio)
      stats.scoredCount += 1
      if (time === undefined) continue
      stats.queryCount += 1
      if (minTime !== null && durationsAreEqual(time, minTime)) stats.wins += 1
    }
  }

  const totalQueries = scoredQueryNames.size

  return Array.from(databases)
    .map(([dbKey, database]) => {
      const stats = dbStats.get(dbKey)!
      const coverage = coverageByDb.get(dbKey)
      if (!coverage) {
        throw new Error(`query coverage is missing for ${database.system} ${database.db}`)
      }
      const score =
        stats.scoredCount > 0
          ? Math.exp(stats.logSum / stats.scoredCount)
          : Number.POSITIVE_INFINITY
      return {
        dbKey,
        db: database.db,
        dbName: database.dbName,
        dbVersion: database.dbVersion,
        system: database.system,
        startedAt: coverage.started_at,
        finishedAt: coverage.finished_at,
        score,
        queryCount: stats.queryCount,
        wins: stats.wins,
        missing: totalQueries - stats.queryCount,
        failed: coverage.failed_query_count,
        neverCompleted: Math.max(0, totalQueries - stats.queryCount - coverage.failed_query_count),
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
      const systemDelta = a.system.localeCompare(b.system)
      if (systemDelta !== 0) return systemDelta
      return a.db.localeCompare(b.db)
    })
}

function durationsAreEqual(left: number, right: number): boolean {
  return Math.abs(left - right) <= DURATION_EQUALITY_TOLERANCE
}

export function formatScore(score: number): string {
  if (!Number.isFinite(score)) return "—"
  if (score < 1.005) return "1.00×"
  if (score < 10) return `${score.toFixed(2)}×`
  if (score < 100) return `${score.toFixed(1)}×`
  return `${Math.round(score)}×`
}

export const SCORE_EXPLAINER = {
  title: "How the score is calculated",
  body: [
    "For each query in the suite, we find the fastest database and compare every other database's warm median time to it as a ratio (a smoothing constant of 10ms is added to both sides to avoid blow-ups on sub-millisecond queries).",
    "Query medians use warm or steady-state iterations when available. Queries with only one recorded iteration fall back to the all-iteration median.",
    "The score shown is the geometric mean of these ratios across the suite's query manifest. Missing or unsupported queries are scored as the larger of 10× or 2× the slowest observed ratio for that query.",
    "When the latest attempted select run recorded failed query steps, those failures are shown separately from queries that were never completed or not recorded.",
    "1.00× means the database was the fastest on every query; 2.50× means it was on average 2.5× slower than the fastest per query after any missing-query penalties.",
    "This is the same shape of metric used by the official ClickBench rankings, just normalised to per-query so suites with very different query counts stay comparable.",
  ],
}
