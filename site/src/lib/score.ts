import type { QueryCoverage, QuerySummary } from "./types"

export interface DatabaseScore {
  db: string
  score: number
  queryCount: number
  wins: number
  missing: number
  failed: number
  neverCompleted: number
  latestSelectFailed: boolean
}

const SMOOTHING_SECONDS = 0.01
const MISSING_QUERY_MIN_RATIO = 10
const MISSING_QUERY_WORST_MULTIPLIER = 2

export function computeDatabaseScores(
  querySummaries: QuerySummary[],
  queryCoverage: QueryCoverage[] = [],
): DatabaseScore[] {
  if (querySummaries.length === 0 && queryCoverage.length === 0) return []

  const queryGroups = new Map<string, Map<string, number>>()
  const databases = new Set<string>()
  const coverageByDb = new Map(queryCoverage.map((row) => [row.db, row]))

  for (const row of querySummaries) {
    databases.add(row.db)
    const group = queryGroups.get(row.query_name) ?? new Map<string, number>()
    group.set(row.db, row.median_duration_s)
    queryGroups.set(row.query_name, group)
  }
  for (const row of queryCoverage) {
    databases.add(row.db)
  }

  const dbStats = new Map<
    string,
    { logSum: number; scoredCount: number; queryCount: number; wins: number }
  >()
  for (const db of databases) {
    dbStats.set(db, { logSum: 0, scoredCount: 0, queryCount: 0, wins: 0 })
  }

  for (const group of queryGroups.values()) {
    const times = Array.from(group.values())
    if (times.length === 0) continue
    const minTime = Math.min(...times)
    const minSmoothed = minTime + SMOOTHING_SECONDS
    const ratios = new Map<string, number>()

    for (const [db, time] of group) {
      ratios.set(db, (time + SMOOTHING_SECONDS) / minSmoothed)
    }

    const worstObservedRatio = Math.max(...ratios.values())
    const missingRatio = Math.max(
      MISSING_QUERY_MIN_RATIO,
      MISSING_QUERY_WORST_MULTIPLIER * worstObservedRatio,
    )

    for (const db of databases) {
      const stats = dbStats.get(db)!
      const time = group.get(db)
      const ratio = ratios.get(db) ?? missingRatio
      stats.logSum += Math.log(ratio)
      stats.scoredCount += 1
      if (time === undefined) continue
      stats.queryCount += 1
      if (time === minTime) stats.wins += 1
    }
  }

  const totalQueries = queryGroups.size

  return Array.from(databases)
    .map((db) => {
      const stats = dbStats.get(db)!
      const score =
        stats.scoredCount > 0
          ? Math.exp(stats.logSum / stats.scoredCount)
          : Number.POSITIVE_INFINITY
      return {
        db,
        score,
        queryCount: stats.queryCount,
        wins: stats.wins,
        missing: totalQueries - stats.queryCount,
        failed: coverageByDb.get(db)?.failed_query_count ?? 0,
        neverCompleted: Math.max(
          0,
          totalQueries - stats.queryCount - (coverageByDb.get(db)?.failed_query_count ?? 0),
        ),
        latestSelectFailed: coverageByDb.get(db)?.latest_status === "failed",
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
    "For each query in the suite, we find the fastest database and compare every other database's median time to it as a ratio (a smoothing constant of 10ms is added to both sides to avoid blow-ups on sub-millisecond queries).",
    "Query medians include every recorded iteration from the latest completed run, including the first iteration.",
    "The score shown is the geometric mean of these ratios across all observed queries. Missing or unsupported queries are scored as the larger of 10× or 2× the slowest observed ratio for that query.",
    "When the latest attempted select run recorded failed query steps, those failures are shown separately from queries that were never completed or not recorded.",
    "1.00× means the database was the fastest on every query; 2.50× means it was on average 2.5× slower than the fastest per query after any missing-query penalties.",
    "This is the same shape of metric used by the official ClickBench rankings, just normalised to per-query so suites with very different query counts stay comparable.",
  ],
}
