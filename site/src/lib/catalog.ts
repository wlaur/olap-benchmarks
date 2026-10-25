import type { BenchmarkDefinition } from "./benchmarks"
import type { ComparisonFamily, ComparisonManifest } from "./comparison"
import { observeQuery, type QueryObservation, type RunObservation } from "./observations"
import type { QuerySqlEntry } from "./types"

export type CatalogRunOutcome = "complete" | "partial" | "failed" | "invalidated" | "incomplete"

export const CATALOG_OUTCOME_LABELS: Record<CatalogRunOutcome, string> = {
  complete: "Complete execution",
  partial: "Partial execution",
  failed: "Failed run",
  invalidated: "Invalidated run",
  incomplete: "Incomplete run",
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
}

export interface CatalogDatabaseCoverage {
  database: string
  registeredScales: number[]
  implemented: boolean
  runs: CatalogRunCoverage[]
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
        runs: suiteRuns
          .filter((run) => run.db === database)
          .map((run) => {
            const coverageNames = unique(
              families
                .filter((family) => family.scale_factors.includes(run.scale))
                .flatMap((family) => family.coverage_query_names),
            )
            const queries = coverageNames.map((query) => observeQuery(run, query, [], comparison))
            const completedQueries = queries.filter((query) => query.executionComplete).length
            const outcome: CatalogRunOutcome =
              run.metadata.invalidation !== null && run.metadata.invalidation !== undefined
                ? "invalidated"
                : run.status === "failed" || run.error !== null
                  ? "failed"
                  : run.status !== "completed"
                    ? "incomplete"
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
            }
          }),
      }
    })

  return {
    definition,
    queryNames,
    scales,
    systems: unique(suiteRuns.map((run) => run.system)).sort(),
    families,
    databases,
    recordedDatabaseCount: databases.filter((database) => database.runs.length > 0).length,
  }
}

export function catalogAvailability(database: CatalogDatabaseCoverage): string {
  if (database.runs.length === 0) {
    return database.implemented ? "No recorded run" : "Adapter unimplemented"
  }
  return Object.entries(CATALOG_OUTCOME_LABELS)
    .flatMap(([outcome, label]) => {
      const count = database.runs.filter((run) => run.outcome === outcome).length
      return count === 0 ? [] : [`${label}: ${count}`]
    })
    .join(" · ")
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
