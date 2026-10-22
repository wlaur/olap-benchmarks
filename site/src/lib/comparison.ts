import { fetchResultsManifest } from "./resultsManifest"

export interface ComparisonFamily {
  id: string
  suite: string
  title: string
  weight: number
  scale_factors: number[]
  query_names: string[]
  coverage_query_names: string[]
  track: "raw" | "prepared" | "smoke"
}

export interface Registration {
  db: string
  suite: string
  scale_factors: number[]
  operations: string[]
}

export interface ComparisonAnnotation {
  db: string
  suite: string
  scale_factor: number | null
  run_id: number | null
  preparation: "incomplete" | "observed" | "unknown"
  tuning: string | null
  summary: string
  evidence_url: string
}

export interface ComparisonManifest {
  version: 1
  results_db_sha256: string
  policy_id: "fixed-workloads-v1"
  retrospective: boolean
  families: ComparisonFamily[]
  registrations: Registration[]
  answer_comparison_exclusions: {
    suite: string
    query_name: string
    db: string | null
    reason: string
  }[]
  annotations: ComparisonAnnotation[]
}

export function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}
}

function strings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string" && item.length > 0)
}

function scales(value: unknown): value is number[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every((item) => Number.isFinite(item) && item > 0) &&
    new Set(value).size === value.length
  )
}

export function parseComparisonManifest(
  input: unknown,
  resultsSha256: unknown,
): ComparisonManifest {
  const manifest = record(input)
  if (
    manifest.version !== 1 ||
    manifest.policy_id !== "fixed-workloads-v1" ||
    typeof manifest.retrospective !== "boolean"
  ) {
    throw new Error("comparison.json has no valid fixed-workloads-v1 policy")
  }
  if (
    typeof manifest.results_db_sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(manifest.results_db_sha256) ||
    manifest.results_db_sha256 !== resultsSha256
  ) {
    throw new Error("comparison.json results_db_sha256 does not match manifest.json")
  }
  if (
    !Array.isArray(manifest.families) ||
    manifest.families.length === 0 ||
    !Array.isArray(manifest.registrations) ||
    !Array.isArray(manifest.annotations) ||
    !Array.isArray(manifest.answer_comparison_exclusions)
  ) {
    throw new Error("comparison.json is missing required publication metadata")
  }
  const ids = new Set<string>()
  for (const value of manifest.families) {
    const family = record(value)
    if (
      typeof family.id !== "string" ||
      ids.has(family.id) ||
      typeof family.suite !== "string" ||
      typeof family.title !== "string" ||
      typeof family.weight !== "number" ||
      !Number.isFinite(family.weight) ||
      family.weight < 0 ||
      !scales(family.scale_factors) ||
      !strings(family.query_names) ||
      family.query_names.length === 0 ||
      !strings(family.coverage_query_names) ||
      new Set(family.query_names).size !== family.query_names.length ||
      new Set(family.coverage_query_names).size !== family.coverage_query_names.length ||
      !family.query_names.every((query) =>
        (family.coverage_query_names as string[]).includes(query),
      ) ||
      !["raw", "prepared", "smoke"].includes(String(family.track))
    ) {
      throw new Error("comparison.json contains an invalid fixed family")
    }
    ids.add(family.id)
  }
  for (const value of manifest.registrations) {
    const registration = record(value)
    if (
      typeof registration.db !== "string" ||
      typeof registration.suite !== "string" ||
      !scales(registration.scale_factors) ||
      !strings(registration.operations)
    )
      throw new Error("comparison.json contains an invalid registration")
  }
  for (const value of manifest.answer_comparison_exclusions) {
    const exclusion = record(value)
    if (
      typeof exclusion.suite !== "string" ||
      typeof exclusion.query_name !== "string" ||
      (exclusion.db !== null && typeof exclusion.db !== "string") ||
      typeof exclusion.reason !== "string"
    )
      throw new Error("comparison.json contains an invalid answer exclusion")
  }
  for (const value of manifest.annotations) {
    const annotation = record(value)
    if (
      typeof annotation.db !== "string" ||
      typeof annotation.suite !== "string" ||
      !["incomplete", "observed", "unknown"].includes(String(annotation.preparation)) ||
      (annotation.scale_factor !== null && typeof annotation.scale_factor !== "number") ||
      (annotation.run_id !== null && typeof annotation.run_id !== "number") ||
      (annotation.tuning !== null && typeof annotation.tuning !== "string") ||
      typeof annotation.summary !== "string" ||
      typeof annotation.evidence_url !== "string" ||
      !/^https?:\/\//.test(annotation.evidence_url)
    )
      throw new Error("comparison.json contains an invalid sourced annotation")
  }
  return manifest as unknown as ComparisonManifest
}

let comparisonPromise: Promise<ComparisonManifest> | null = null

export function fetchComparisonManifest(): Promise<ComparisonManifest> {
  comparisonPromise ??= Promise.all([
    fetchResultsManifest(),
    fetch(`${import.meta.env.BASE_URL}data/comparison.json`).then((response) => {
      if (!response.ok) throw new Error(`Failed to load comparison.json: ${response.status}`)
      return response.json() as Promise<unknown>
    }),
  ]).then(([results, comparison]) => parseComparisonManifest(comparison, results.results_db_sha256))
  return comparisonPromise
}
