import type { QuerySummary } from "./types"

export interface DatabaseScore {
  db: string
  score: number
  queryCount: number
  wins: number
  missing: number
}

const SMOOTHING_SECONDS = 0.01

export function computeDatabaseScores(querySummaries: QuerySummary[]): DatabaseScore[] {
  if (querySummaries.length === 0) return []

  const queryGroups = new Map<string, Map<string, number>>()
  const databases = new Set<string>()

  for (const row of querySummaries) {
    databases.add(row.db)
    const group = queryGroups.get(row.query_name) ?? new Map<string, number>()
    group.set(row.db, row.median_duration_s)
    queryGroups.set(row.query_name, group)
  }

  const dbStats = new Map<string, { logSum: number; count: number; wins: number }>()
  for (const db of databases) {
    dbStats.set(db, { logSum: 0, count: 0, wins: 0 })
  }

  for (const group of queryGroups.values()) {
    const times = Array.from(group.values())
    if (times.length === 0) continue
    const minTime = Math.min(...times)
    const minSmoothed = minTime + SMOOTHING_SECONDS

    for (const [db, time] of group) {
      const stats = dbStats.get(db)!
      const ratio = (time + SMOOTHING_SECONDS) / minSmoothed
      stats.logSum += Math.log(ratio)
      stats.count += 1
      if (time === minTime) stats.wins += 1
    }
  }

  const totalQueries = queryGroups.size

  return Array.from(databases)
    .map((db) => {
      const stats = dbStats.get(db)!
      const score =
        stats.count > 0 ? Math.exp(stats.logSum / stats.count) : Number.POSITIVE_INFINITY
      return {
        db,
        score,
        queryCount: stats.count,
        wins: stats.wins,
        missing: totalQueries - stats.count,
      }
    })
    .sort((a, b) => a.score - b.score)
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
    "The score shown is the geometric mean of these ratios across all queries the database completed. 1.00× means the database was the fastest on every query; 2.50× means it was on average 2.5× slower than the fastest per query.",
    "This is the same shape of metric used by the official ClickBench rankings, just normalised to per-query so suites with very different query counts stay comparable.",
  ],
}
