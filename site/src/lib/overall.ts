import { record, type ComparisonFamily } from "./comparison"
import {
  runPerformanceProblems,
  type FamilyEngineResult,
  type QueryObservation,
} from "./observations"

export const OVERALL_POLICY = "bounded-log-utility-v1"
export const SLOWDOWN_CAP = 100

export interface FamilyOverview {
  family: ComparisonFamily
  scales: { scale: number; rows: FamilyEngineResult[] }[]
}

export interface OverallFamilyScore {
  id: string
  title: string
  weight: number
  score: number
  coverage: number
  scoredQueries: number
  cappedQueries: number
  recordedCoverage: number
  totalQueries: number
  gaps: Record<string, number>
  runIds: number[]
  recordedRunIds: number[]
}

export interface OverallEngineResult {
  db: string
  versions: string[]
  score: number
  coverage: number
  recordedGapCoverage: number
  unmeasuredCoverage: number
  scoredQueries: number
  totalQueries: number
  workloadsWithData: number
  rank: number | null
  rankRange: [number, number] | null
  unrankedOmissions: number
  families: OverallFamilyScore[]
}

export interface OverallRanking {
  policy: string
  familyCount: number
  queryCount: number
  rows: OverallEngineResult[]
}

function queryProblem(query: QueryObservation | undefined): string {
  if (!query) return "Not measured"
  if (query.evidence === "conflict") return "Answer conflict"
  if (query.evidence === "excluded") return "Comparison excluded"
  for (const status of ["wrong_result", "timeout", "error", "unsupported", "skipped"]) {
    if (query.steps.some((step) => step.resultStatus === status)) return status.replaceAll("_", " ")
  }
  return "Incomplete measurements"
}

function bounded(value: number): number {
  return Math.max(0, Math.min(100, value))
}

function rankOf(score: number, scores: readonly number[]): number {
  const shown = Math.round(score * 10)
  return 1 + scores.filter((other) => Math.round(other * 10) > shown).length
}

export function aggregateOverview(overview: readonly FamilyOverview[]): OverallRanking {
  const included = overview.filter(({ family }) => family.weight > 0 && family.track === "raw")
  const engines = [
    ...new Set(
      overview.flatMap((entry) => entry.scales.flatMap((cell) => cell.rows.map((row) => row.db))),
    ),
  ].sort()
  const totalWeight = included.reduce((sum, { family }) => sum + family.weight, 0)
  const familyScores = new Map<string, OverallFamilyScore[]>()
  for (const db of engines) familyScores.set(db, [])

  for (const { family, scales } of included) {
    const accumulators = new Map(
      engines.map((db) => [
        db,
        {
          id: family.id,
          title: family.title,
          weight: family.weight,
          score: 0,
          coverage: 0,
          scoredQueries: 0,
          cappedQueries: 0,
          recordedCoverage: 0,
          totalQueries: family.query_names.length * family.scale_factors.length,
          gaps: {} as Record<string, number>,
          runIds: [] as number[],
          recordedRunIds: [] as number[],
        },
      ]),
    )
    const queryWeight = 100 / family.scale_factors.length / family.query_names.length
    for (const scale of family.scale_factors) {
      const rows = scales.find((cell) => cell.scale === scale)?.rows ?? []
      const selected = new Map<string, FamilyEngineResult>()
      const problems = new Map<string, string>()
      for (const db of engines) {
        const variants = rows.filter((row) => row.db === db)
        const row = variants[0]
        if (variants.length > 1)
          throw new Error(`Multiple publication variants: ${db}, ${family.id}, SF ${scale}`)
        else if (!row || !row.run)
          problems.set(
            db,
            row?.registration === "unimplemented" ? "Adapter unimplemented" : "Not measured",
          )
        else if (row.registration !== "registered")
          throw new Error(`Run exists for an unregistered scale: ${db}, ${family.id}, SF ${scale}`)
        else {
          selected.set(db, row)
          accumulators.get(db)!.recordedRunIds.push(row.run.runId)
          const blockers = runPerformanceProblems(row.run, row.annotations)
          if (blockers.length > 0) problems.set(db, blockers.join("; "))
        }
      }
      const inputs = new Set(
        [...selected]
          .filter(([db]) => !problems.has(db))
          .map(([, row]) => record(row.run!.metadata.input).manifest_sha256),
      )
      if (inputs.size > 1) throw new Error(`Input fingerprints differ: ${family.id}, SF ${scale}`)
      if ([...selected.keys()].filter((db) => !problems.has(db)).length < 2)
        throw new Error(`At least two scorable runs required: ${family.id}, SF ${scale}`)
      for (const [db, row] of selected) {
        const conflict = row.queries.find((query) => query.evidence === "conflict")
        if (!problems.has(db) && conflict)
          throw new Error(
            `Unresolved answer conflict: ${db}, ${family.id}, SF ${scale}, ${conflict.queryName}`,
          )
      }

      for (const name of family.query_names) {
        const usable = new Map<string, QueryObservation>()
        for (const [db, row] of selected) {
          const query = row.queries.find((entry) => entry.queryName === name)
          if (
            !problems.has(db) &&
            query?.complete &&
            query.warmup === 1 &&
            query.evidence !== "excluded" &&
            query.medianS !== null &&
            Number.isFinite(query.medianS) &&
            query.medianS > 0
          )
            usable.set(db, query)
        }
        if (usable.size < 2)
          throw new Error(
            `At least two usable query results required: ${family.id}, SF ${scale}, ${name}`,
          )
        const reference = Math.min(...[...usable.values()].map((query) => query.medianS!))
        for (const db of engines) {
          const accumulator = accumulators.get(db)!
          const query = usable.get(db)
          if (selected.has(db)) accumulator.recordedCoverage += queryWeight
          if (query) {
            const ratio = query.medianS! / reference
            const credit = Math.max(0, 1 - Math.log(ratio) / Math.log(SLOWDOWN_CAP))
            accumulator.score += queryWeight * credit
            accumulator.coverage += queryWeight
            accumulator.scoredQueries += 1
            if (ratio >= SLOWDOWN_CAP) accumulator.cappedQueries += 1
            const runId = selected.get(db)!.run!.runId
            if (!accumulator.runIds.includes(runId)) accumulator.runIds.push(runId)
          } else {
            const reason =
              problems.get(db) ??
              queryProblem(selected.get(db)?.queries.find((entry) => entry.queryName === name))
            accumulator.gaps[reason] = (accumulator.gaps[reason] ?? 0) + 1
          }
        }
      }
    }
    for (const [db, accumulator] of accumulators) {
      accumulator.score = bounded(accumulator.score)
      accumulator.coverage = bounded(accumulator.coverage)
      accumulator.recordedCoverage = bounded(accumulator.recordedCoverage)
      familyScores.get(db)!.push(accumulator)
    }
  }

  const rows: OverallEngineResult[] = engines.map((db) => {
    const families = familyScores.get(db)!
    const coverage =
      totalWeight > 0
        ? bounded(
            families.reduce((sum, family) => sum + family.weight * family.coverage, 0) /
              totalWeight,
          )
        : 0
    const recorded =
      totalWeight > 0
        ? bounded(
            families.reduce((sum, family) => sum + family.weight * family.recordedCoverage, 0) /
              totalWeight,
          )
        : 0
    return {
      db,
      versions: [
        ...new Set(
          overview.flatMap((entry) =>
            entry.scales.flatMap((cell) =>
              cell.rows.filter((row) => row.db === db && row.run).map((row) => row.run!.version),
            ),
          ),
        ),
      ].sort(),
      score:
        totalWeight > 0
          ? bounded(
              families.reduce((sum, family) => sum + family.weight * family.score, 0) / totalWeight,
            )
          : 0,
      coverage,
      recordedGapCoverage: bounded(recorded - coverage),
      unmeasuredCoverage: bounded(100 - recorded),
      scoredQueries: families.reduce((sum, family) => sum + family.scoredQueries, 0),
      totalQueries: families.reduce((sum, family) => sum + family.totalQueries, 0),
      workloadsWithData: families.filter((family) => family.scoredQueries > 0).length,
      rank: null,
      rankRange: null,
      unrankedOmissions: 0,
      families,
    }
  })
  const scores = rows.filter((row) => row.scoredQueries > 0).map((row) => row.score)
  for (const row of rows) {
    if (row.scoredQueries === 0) continue
    row.rank = rankOf(row.score, scores)
    const ranks = [row.rank]
    for (const { family } of included) {
      const remainingWeight = totalWeight - family.weight
      if (remainingWeight <= 0) continue
      const without = (entry: OverallEngineResult) =>
        Math.max(
          0,
          (entry.score * totalWeight -
            entry.families.find((value) => value.id === family.id)!.score * family.weight) /
            remainingWeight,
        )
      const remaining = (entry: OverallEngineResult) =>
        entry.scoredQueries -
          entry.families.find((value) => value.id === family.id)!.scoredQueries >
        0
      if (remaining(row)) ranks.push(rankOf(without(row), rows.filter(remaining).map(without)))
      else row.unrankedOmissions += 1
    }
    row.rankRange = [Math.min(...ranks), Math.max(...ranks)]
  }
  return {
    policy: OVERALL_POLICY,
    familyCount: included.length,
    queryCount: included.reduce(
      (sum, { family }) => sum + family.query_names.length * family.scale_factors.length,
      0,
    ),
    rows: rows.sort(
      (a, b) => Math.round(b.score * 10) - Math.round(a.score * 10) || a.db.localeCompare(b.db),
    ),
  }
}
