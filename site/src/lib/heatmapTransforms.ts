import type { QueryComparisonRow } from "../components/QueryComparisonTable"
import { clamp } from "./format"

export const APPROX_CHAR_WIDTH = 7.5

const COLOR_STOPS: readonly [number, number, number][] = [
  [52, 211, 153],
  [250, 204, 21],
  [249, 115, 22],
  [239, 68, 68],
  [153, 27, 27],
]

const MISSING_COLOR = "rgba(148, 163, 184, 0.08)"

export function interpolateColor(t: number): string {
  const clamped = clamp(t, 0, 1)
  const segmentCount = COLOR_STOPS.length - 1
  const segment = Math.min(Math.floor(clamped * segmentCount), segmentCount - 1)
  const segmentT = clamped * segmentCount - segment
  const from = COLOR_STOPS[segment]!
  const to = COLOR_STOPS[segment + 1]!
  const r = Math.round(from[0] + (to[0] - from[0]) * segmentT)
  const g = Math.round(from[1] + (to[1] - from[1]) * segmentT)
  const b = Math.round(from[2] + (to[2] - from[2]) * segmentT)
  const alpha = 0.45 + clamped * 0.3
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

export function ratioToT(ratio: number, maxRatio: number): number {
  if (ratio <= 1) return 0
  const logMax = Math.log(Math.max(maxRatio, 1.01))
  return Math.min(1, Math.log(ratio) / logMax)
}

export function ratioColor(ratio: number | null, maxRatio: number): string {
  if (ratio === null) return MISSING_COLOR
  return interpolateColor(ratioToT(ratio, maxRatio))
}

export interface HeatmapCellData {
  duration: number | null
  ratio: number | null
}

export interface HeatmapGridData {
  cells: HeatmapCellData[][]
  maxRatio: number
  summaryRow: HeatmapCellData[]
}

export function buildHeatmapGridData(
  rows: QueryComparisonRow[],
  databases: string[],
): HeatmapGridData {
  let maxRatio = 1

  // Per-database: sum of all median durations (total query time per db)
  const dbTotals = databases.map((db) => {
    let total = 0
    let hasAny = false
    for (const row of rows) {
      const d = row.by_database[db]
      if (d !== null && d !== undefined) {
        total += d
        hasAny = true
      }
    }
    return hasAny ? total : null
  })

  const validTotals = dbTotals.filter((d): d is number => d !== null)
  const fastestTotal = validTotals.length > 0 ? Math.min(...validTotals) : null
  const summaryRow: HeatmapCellData[] = dbTotals.map((total) => {
    const ratio =
      total !== null && fastestTotal !== null && fastestTotal > 0 ? total / fastestTotal : null
    if (ratio !== null && ratio > maxRatio) maxRatio = ratio
    return { duration: total, ratio }
  })

  const cells = rows.map((row) => {
    const durations = databases.map((db) => row.by_database[db] ?? null)
    const validDurations = durations.filter((d): d is number => d !== null)
    const fastest = validDurations.length > 0 ? Math.min(...validDurations) : null

    return durations.map((duration) => {
      const ratio = duration !== null && fastest !== null && fastest > 0 ? duration / fastest : null
      if (ratio !== null && ratio > maxRatio) maxRatio = ratio
      return { duration, ratio }
    })
  })

  return { cells, maxRatio, summaryRow }
}

export function computeCellWidth(databases: string[]): number {
  const longestName = Math.max(0, ...databases.map((db) => db.length))
  return Math.max(64, longestName * APPROX_CHAR_WIDTH + 20)
}

export function buildLegendTickRatios(maxRatio: number): number[] {
  const ticks: number[] = [1]
  const candidates = [1.5, 2, 3, 5, 10, 20, 50, 100, 500, 1000]
  for (const c of candidates) {
    if (c <= maxRatio) ticks.push(c)
  }
  if (maxRatio > 1 && !ticks.includes(Math.round(maxRatio))) {
    ticks.push(Math.round(maxRatio))
  }
  if (ticks.length > 6) {
    const step = Math.ceil(ticks.length / 5)
    const filtered = [ticks[0]!]
    for (let i = step; i < ticks.length - 1; i += step) filtered.push(ticks[i]!)
    filtered.push(ticks[ticks.length - 1]!)
    return filtered
  }
  return ticks
}

export function formatCellDuration(seconds: number): string {
  if (seconds <= 0) return "0s"
  if (seconds < 0.001) return "<1ms"
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
  if (seconds < 10) return `${seconds.toFixed(1)}s`
  if (seconds < 60) return `${Math.round(seconds)}s`
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  return s > 0 ? `${m}m${s}s` : `${m}m`
}

export function clipLabel(text: string, maxChars: number): string {
  if (text.length <= maxChars) return text
  return `${text.slice(0, maxChars - 1)}…`
}
