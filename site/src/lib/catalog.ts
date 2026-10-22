import type { BenchmarkDefinition } from "./benchmarks"
import type { ComparisonManifest } from "./comparison"
import type { CatalogRunDimension, QuerySqlEntry } from "./types"

export interface CatalogDatabaseCoverage {
  database: string
  versions: string[]
  registeredScales: number[]
  implemented: boolean
  systemsByScale: Map<number, string[]>
}

export interface CatalogSuiteSummary {
  definition: BenchmarkDefinition
  queryNames: string[]
  scales: number[]
  systems: string[]
  databases: CatalogDatabaseCoverage[]
  coveredCombinations: number
  totalCombinations: number
}

export function buildCatalogSuiteSummary(
  definition: BenchmarkDefinition,
  dimensions: readonly CatalogRunDimension[],
  comparison: ComparisonManifest | null,
): CatalogSuiteSummary {
  const suiteRows = dimensions.filter((row) => row.suite === definition.id)
  const allDatabases = unique([
    ...dimensions.map((row) => row.db),
    ...(comparison?.registrations.map((entry) => entry.db) ?? []),
  ]).sort()
  const scales = unique([
    ...(comparison?.families
      .filter((family) => family.suite === definition.id)
      .flatMap((family) => family.scale_factors) ?? []),
    ...suiteRows.map((row) => row.suite_scale_factor),
  ]).sort((a, b) => a - b)
  const databases = allDatabases.map((database) => {
    const databaseRows = suiteRows.filter((row) => row.db === database)
    const systemsByScale = new Map<number, string[]>()

    for (const scale of scales) {
      systemsByScale.set(
        scale,
        unique(
          databaseRows.filter((row) => row.suite_scale_factor === scale).map((row) => row.system),
        ).sort(),
      )
    }

    return {
      database,
      registeredScales: unique(
        comparison?.registrations
          .filter(
            (entry) =>
              entry.db === database &&
              entry.suite === definition.id &&
              entry.operations.includes("select"),
          )
          .flatMap((entry) => entry.scale_factors) ?? [],
      ),
      implemented:
        comparison?.registrations.some(
          (entry) =>
            entry.db === database &&
            entry.suite === definition.id &&
            entry.operations.includes("select"),
        ) ?? false,
      versions: unique(
        databaseRows.map((row) =>
          row.db_driver === null ? row.db_version : `${row.db_version} · ${row.db_driver}`,
        ),
      ).sort(),
      systemsByScale,
    }
  })
  const coveredCombinations = databases.reduce(
    (total, database) =>
      total +
      scales.filter((scale) => (database.systemsByScale.get(scale)?.length ?? 0) > 0).length,
    0,
  )

  return {
    definition,
    queryNames: comparison
      ? unique(
          comparison.families
            .filter((family) => family.suite === definition.id)
            .flatMap((family) => family.coverage_query_names),
        )
      : [],
    scales,
    systems: unique(suiteRows.map((row) => row.system)).sort(),
    databases,
    coveredCombinations,
    totalCombinations: databases.length * scales.length,
  }
}

export function getSqlDialects(entry: QuerySqlEntry | undefined) {
  if (!entry) return []
  return [...(entry.sql === null ? [] : ["base"]), ...Object.keys(entry.db_overrides).sort()]
}

function unique<T>(values: readonly T[]) {
  return [...new Set(values)]
}
