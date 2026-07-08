import type { BenchmarkOperation } from "./types"

export type BenchmarkSuiteId =
  | "time_series"
  | "rtabench"
  | "clickbench"
  | "jsonbench"
  | "kaggle_airbnb"
  | "tpc_h"
  | "tpc_ds"

export type QueryNameParserId = "generic" | "time_series"
export type BenchmarkPublicRole = "benchmark" | "smoke"

export interface BenchmarkDefinition {
  id: BenchmarkSuiteId
  navLabel: string
  title: string
  defaultScaleFactor: number
  supportedScaleFactors: number[]
  scaleFactorSupported: boolean
  queriesKey: string
  operations: BenchmarkOperation[]
  queryNameParser: QueryNameParserId
  publicRole: BenchmarkPublicRole
}

interface SuiteManifestEntry {
  id: string
  nav_label: string
  title: string
  default_scale_factor: number
  supported_scale_factors: number[]
  scale_factor_supported: boolean
  queries_key: string
  operations: BenchmarkOperation[]
  query_name_parser: QueryNameParserId
  public_role: BenchmarkPublicRole
}

interface SuitesManifest {
  suites: SuiteManifestEntry[]
}

const KNOWN_SUITE_IDS: readonly BenchmarkSuiteId[] = [
  "time_series",
  "rtabench",
  "clickbench",
  "jsonbench",
  "kaggle_airbnb",
  "tpc_h",
  "tpc_ds",
]

let benchmarkDefinitionsCache: BenchmarkDefinition[] | null = null

export const fallbackBenchmarkId: BenchmarkSuiteId = "time_series"

export function isBenchmarkSuiteId(
  value: string | undefined,
  definitions: readonly BenchmarkDefinition[],
): value is BenchmarkSuiteId {
  if (value === undefined) return false
  return definitions.some((definition) => definition.id === value)
}

export function getDefaultBenchmarkId(
  definitions: readonly BenchmarkDefinition[],
): BenchmarkSuiteId {
  return definitions[0]?.id ?? fallbackBenchmarkId
}

export async function fetchBenchmarkDefinitions(): Promise<BenchmarkDefinition[]> {
  if (benchmarkDefinitionsCache) return benchmarkDefinitionsCache

  const response = await fetch(`${import.meta.env.BASE_URL}data/suites.json`)
  if (!response.ok) {
    throw new Error(`Failed to load suites.json: ${response.status}`)
  }

  const manifest = (await response.json()) as SuitesManifest
  benchmarkDefinitionsCache = manifest.suites.map(parseSuiteManifestEntry)
  return benchmarkDefinitionsCache
}

function parseSuiteManifestEntry(entry: SuiteManifestEntry): BenchmarkDefinition {
  if (!KNOWN_SUITE_IDS.includes(entry.id as BenchmarkSuiteId)) {
    throw new Error(`suites.json contains unknown suite: ${entry.id}`)
  }

  return {
    id: entry.id as BenchmarkSuiteId,
    navLabel: entry.nav_label,
    title: entry.title,
    defaultScaleFactor: entry.default_scale_factor,
    supportedScaleFactors: entry.supported_scale_factors,
    scaleFactorSupported: entry.scale_factor_supported,
    queriesKey: entry.queries_key,
    operations: entry.operations,
    queryNameParser: entry.query_name_parser,
    publicRole: entry.public_role,
  }
}
