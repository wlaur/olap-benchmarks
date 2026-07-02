import { toTitleCase } from "./format"
import type { QueryStep } from "./types"

export interface TimelineSegment {
  db: string
  query_name: string
  iteration: number
  start_s: number
  end_s: number
  duration_s: number
}

export interface TimelineData {
  segmentsByDb: Map<string, TimelineSegment[]>
  maxElapsed: number
  queryNames: string[]
}

const QUERY_COLORS = [
  ["rgba(96, 165, 250, 0.58)", "rgba(147, 197, 253, 0.9)"],
  ["rgba(251, 146, 60, 0.58)", "rgba(253, 186, 116, 0.9)"],
  ["rgba(52, 211, 153, 0.58)", "rgba(110, 231, 183, 0.9)"],
  ["rgba(250, 204, 21, 0.56)", "rgba(253, 224, 71, 0.9)"],
  ["rgba(244, 114, 182, 0.56)", "rgba(251, 182, 206, 0.9)"],
  ["rgba(168, 85, 247, 0.62)", "rgba(216, 180, 254, 0.92)"],
  ["rgba(251, 113, 133, 0.56)", "rgba(253, 164, 175, 0.9)"],
  ["rgba(45, 212, 191, 0.58)", "rgba(153, 246, 228, 0.9)"],
  ["rgba(129, 140, 248, 0.6)", "rgba(199, 210, 254, 0.92)"],
  ["rgba(245, 158, 11, 0.58)", "rgba(252, 211, 77, 0.9)"],
  ["rgba(74, 222, 128, 0.58)", "rgba(134, 239, 172, 0.9)"],
  ["rgba(192, 132, 252, 0.6)", "rgba(233, 213, 255, 0.92)"],
] as const

export function buildTimelineData(
  steps: QueryStep[],
  databases: string[],
  compareQueryNames?: (left: string, right: string) => number,
): TimelineData {
  const rawByDb = new Map<string, TimelineSegment[]>()
  const queryNameSet = new Set<string>()

  for (const step of steps) {
    queryNameSet.add(step.query_name)

    const dbSegments = rawByDb.get(step.db) ?? []
    dbSegments.push({
      db: step.db,
      query_name: step.query_name,
      iteration: step.iteration,
      start_s: step.elapsed_start_s,
      end_s: step.elapsed_end_s,
      duration_s: step.duration_s,
    })
    rawByDb.set(step.db, dbSegments)
  }

  const segmentsByDb = new Map<string, TimelineSegment[]>()
  let maxElapsed = 0

  for (const [db, segs] of rawByDb) {
    segs.sort((a, b) => a.start_s - b.start_s)
    let cursor = 0
    const packed = segs.map((seg) => {
      const packedSeg = { ...seg, start_s: cursor, end_s: cursor + seg.duration_s }
      cursor = packedSeg.end_s
      return packedSeg
    })
    if (cursor > maxElapsed) maxElapsed = cursor
    segmentsByDb.set(db, packed)
  }

  for (const db of databases) {
    if (!segmentsByDb.has(db)) segmentsByDb.set(db, [])
  }

  const queryNames = Array.from(queryNameSet).sort(compareQueryNames)
  maxElapsed = Math.max(1, Math.ceil(maxElapsed))

  return { segmentsByDb, maxElapsed, queryNames }
}

export function buildQueryColorMap(queryNames: string[]): Record<string, [string, string]> {
  const map: Record<string, [string, string]> = {}
  for (const [i, name] of queryNames.entries()) {
    map[name] = QUERY_COLORS[i % QUERY_COLORS.length]! as [string, string]
  }
  return map
}

export function shortenQueryName(name: string): string {
  const match = /^([a-z]+)_(\d+)/.exec(name)
  if (!match) return name
  return `${match[1]}_${match[2]}`
}

export function formatQueryLabel(name: string): string {
  return toTitleCase(name.replace(/_/g, " "))
}
