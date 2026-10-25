import type { BenchmarkDefinition } from "./benchmarks"
import type { ComparisonFamily, ComparisonManifest } from "./comparison"
import {
  matchingAnnotations,
  observeQuery,
  runPerformanceProblems,
  type QueryObservation,
  type RunObservation,
} from "./observations"
import type { QuerySqlEntry } from "./types"

export type CatalogRunOutcome =
  | "complete"
  | "partial"
  | "failed"
  | "invalidated"
  | "incomplete"
  | "blocked"

export const CATALOG_OUTCOME_LABELS: Record<CatalogRunOutcome, string> = {
  complete: "Complete execution",
  partial: "Partial execution",
  failed: "Failed run",
  invalidated: "Invalidated run",
  incomplete: "Incomplete run",
  blocked: "Comparison blocked",
}

export interface CatalogRunCoverage {
  runId: number
  system: string
  scale: number
  version: string
  outcome: CatalogRunOutcome
  completedQueries: number
  totalQueries: number
  queryOutcomes: string[]
  comparisonBlockers: string[]
}

export interface CatalogFamilyCoverage {
  family: ComparisonFamily
  runs: CatalogRunCoverage[]
}

export interface CatalogDatabaseCoverage {
  database: string
  registeredScales: number[]
  implemented: boolean
  families: CatalogFamilyCoverage[]
}

export interface CatalogSuiteSummary {
  definition: BenchmarkDefinition
  queryNames: string[]
  scales: number[]
  systems: string[]
  families: ComparisonFamily[]
  databases: CatalogDatabaseCoverage[]
  recordedDatabaseCount: number
}

export function buildCatalogSuiteSummary(
  definition: BenchmarkDefinition,
  runs: readonly RunObservation[],
  comparison: ComparisonManifest,
): CatalogSuiteSummary {
  const suiteRuns = runs.filter((run) => run.suite === definition.id)
  const families = comparison.families.filter((family) => family.suite === definition.id)
  const queryNames = unique(families.flatMap((family) => family.coverage_query_names))
  const scales = unique([
    ...families.flatMap((family) => family.scale_factors),
    ...suiteRuns.map((run) => run.scale),
  ]).sort((a, b) => a - b)
  const databases = unique([
    ...runs.map((run) => run.db),
    ...comparison.registrations.map((entry) => entry.db),
  ])
    .sort()
    .map((database): CatalogDatabaseCoverage => {
      const registrations = comparison.registrations.filter(
        (entry) =>
          entry.db === database &&
          entry.suite === definition.id &&
          entry.operations.includes("select"),
      )
      return {
        database,
        registeredScales: unique(registrations.flatMap((entry) => entry.scale_factors)),
        implemented: registrations.length > 0,
        families: families.map((family) => ({
          family,
          runs: suiteRuns
            .filter((run) => run.db === database && family.scale_factors.includes(run.scale))
            .map((run) => {
              const queries = family.coverage_query_names.map((query) =>
                observeQuery(run, query, [], comparison),
              )
              const completedQueries = queries.filter((query) => query.executionComplete).length
              const comparisonBlockers = runPerformanceProblems(
                run,
                matchingAnnotations(run, database, definition.id, run.scale, comparison),
              )
              const outcome: CatalogRunOutcome =
                run.metadata.invalidation !== null && run.metadata.invalidation !== undefined
                  ? "invalidated"
                  : run.status === "failed" || run.error !== null
                    ? "failed"
                    : run.status !== "completed"
                      ? "incomplete"
                      : comparisonBlockers.length > 0
                        ? "blocked"
                        : queries.length > 0 && completedQueries === queries.length
                          ? "complete"
                          : "partial"
              return {
                runId: run.runId,
                system: run.system,
                scale: run.scale,
                version: run.driver === null ? run.version : `${run.version} · ${run.driver}`,
                outcome,
                completedQueries,
                totalQueries: queries.length,
                queryOutcomes: summarizeQueryOutcomes(queries),
                comparisonBlockers,
              }
            }),
        })),
      }
    })

  return {
    definition,
    queryNames,
    scales,
    systems: unique(suiteRuns.map((run) => run.system)).sort(),
    families,
    databases,
    recordedDatabaseCount: unique(suiteRuns.map((run) => run.db)).length,
  }
}

export function catalogTrackLabel(family: ComparisonFamily): string {
  return `${family.track[0]!.toUpperCase()}${family.track.slice(1)} track`
}

export function catalogRunAvailability(run: CatalogRunCoverage): string {
  return `${CATALOG_OUTCOME_LABELS[run.outcome]} · ${run.completedQueries}/${run.totalQueries} queries`
}

export function catalogMissingAvailability(
  database: CatalogDatabaseCoverage,
  scale: number,
): string {
  if (!database.implemented) return "Adapter unimplemented"
  return database.registeredScales.includes(scale)
    ? "No recorded run · execution unknown"
    : "Scale not registered"
}

export function getSqlDialects(entry: QuerySqlEntry | undefined): string[] {
  if (!entry) return []
  return [...(entry.sql === null ? [] : ["base"]), ...Object.keys(entry.db_overrides).sort()]
}

function summarizeQueryOutcomes(queries: readonly QueryObservation[]): string[] {
  const counts = new Map<string, number>()
  for (const query of queries) {
    if (query.executionComplete) continue
    const statuses = unique(
      query.steps.flatMap((step) =>
        step.resultStatus !== null && step.resultStatus !== "ok"
          ? [step.resultStatus.replaceAll("_", " ")]
          : [],
      ),
    )
    if (statuses.length === 0) {
      statuses.push(query.steps.length === 0 ? "not run" : "incomplete measurements")
    }
    for (const status of statuses) counts.set(status, (counts.get(status) ?? 0) + 1)
  }
  return [...counts].map(([status, count]) => `${count} ${status}`)
}

function unique<T>(values: readonly T[]): T[] {
  return [...new Set(values)]
}
