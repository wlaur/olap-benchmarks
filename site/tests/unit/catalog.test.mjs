import { describe, expect, test } from "bun:test"

import {
  buildCatalogSuiteSummary,
  catalogMissingAvailability,
  catalogRunAvailability,
} from "../../src/lib/catalog.ts"

const definition = { id: "rtabench" }
const raw = {
  id: "rtabench",
  suite: "rtabench",
  title: "RTABench",
  weight: 1,
  track: "raw",
  scale_factors: [1, 10],
  query_names: ["orders", "customers"],
  coverage_query_names: ["orders", "customers"],
}
const prepared = {
  ...raw,
  id: "rtabench_prepared",
  title: "RTABench prepared aggregates",
  weight: 0,
  track: "prepared",
  scale_factors: [1],
  query_names: ["prepared_orders", "prepared_customers"],
  coverage_query_names: ["prepared_orders", "prepared_customers"],
}
const manifest = {
  families: [raw, prepared],
  registrations: ["duckdb", "missing"].map((db) => ({
    db,
    suite: "rtabench",
    scale_factors: [1, 10],
    operations: ["select"],
  })),
  annotations: [],
  answer_comparison_exclusions: [],
}

function observation(overrides = {}) {
  return {
    runId: 1,
    db: "duckdb",
    version: "2",
    driver: null,
    system: "host",
    suite: "rtabench",
    scale: 1,
    status: "completed",
    finishedAt: "2026-10-01",
    metadata: {
      benchmark_profile: { warmup_iterations: 1, measured_iterations: 3 },
      input: { manifest_sha256: "same-input" },
    },
    error: null,
    queries: new Map(
      ["orders", "customers", "prepared_orders"].map((queryName) => [
        queryName,
        Array.from({ length: 4 }, (_, index) => ({
          id: index + 1,
          queryName,
          iteration: index + 1,
          role: index === 0 ? "warmup" : "measured",
          status: "completed",
          resultStatus: "ok",
          durationS: 1,
          rowCount: 1,
          hash: null,
          hashVersion: null,
          hashMissingReason: null,
          skipReason: null,
          error: null,
        })),
      ]),
    ),
    ...overrides,
  }
}

function database(runs, policy = manifest) {
  return buildCatalogSuiteSummary(definition, runs, policy).databases.find(
    (entry) => entry.database === "duckdb",
  )
}

describe("catalog execution availability", () => {
  test.each(["unsupported", "skipped"])(
    "zero completed prepared queries retain the recorded %s outcome",
    (status) => {
      const run = observation()
      for (const queryName of prepared.query_names)
        run.queries.set(queryName, [
          {
            ...run.queries.get("orders")[0],
            queryName,
            status: "skipped",
            resultStatus: status,
            skipReason: "Recorded selection",
          },
        ])
      const coverage = database([run]).families[1].runs[0]
      expect(catalogRunAvailability(coverage)).toBe(`0/2 completed · 2 ${status}`)
      expect(catalogRunAvailability(coverage)).not.toContain("Partial execution")
    },
  )
  test("separates raw completion from partial prepared execution using each family query set", () => {
    const result = database([observation()])
    expect(result.families[0].runs[0]).toMatchObject({
      outcome: "complete",
      completedQueries: 2,
      totalQueries: 2,
      queryOutcomes: [],
    })
    expect(result.families[1].runs[0]).toMatchObject({
      outcome: "partial",
      completedQueries: 1,
      totalQueries: 2,
      queryOutcomes: ["1 not run"],
    })
    expect(catalogRunAvailability(result.families[0].runs[0])).toBe(
      "Complete execution · 2/2 queries",
    )
  })

  test("retains scale and system scope without inventing a prepared run at an unpublished scale", () => {
    const result = database([
      observation(),
      observation({ runId: 2, scale: 10, system: "second-host" }),
    ])
    expect(result.families[0].runs.map(({ scale, system }) => [scale, system])).toEqual([
      [1, "host"],
      [10, "second-host"],
    ])
    expect(result.families[1].runs.map((run) => run.runId)).toEqual([1])
  })

  test.each([
    [{ status: "failed", error: "Query failed" }, "failed"],
    [{ status: "running" }, "incomplete"],
    [{ error: "Recorded error" }, "failed"],
  ])(
    "retains completed query observations while preserving a blocked run outcome: %j",
    (overrides, outcome) => {
      const result = database([observation(overrides)]).families[0].runs[0]
      expect(result).toMatchObject({ outcome, completedQueries: 2, totalQueries: 2 })
      expect(catalogRunAvailability(result)).not.toContain("Complete execution")
    },
  )

  test("uses run-scoped preparation annotations without treating completion as usable evidence", () => {
    const policy = {
      ...manifest,
      annotations: [
        { db: "duckdb", suite: "rtabench", scale_factor: 10, run_id: 2, preparation: "incomplete" },
      ],
    }
    const result = database([observation(), observation({ runId: 2, scale: 10 })], policy)
    expect(result.families[0].runs[0].outcome).toBe("complete")
    expect(result.families[0].runs[1]).toMatchObject({
      outcome: "blocked",
      completedQueries: 2,
      totalQueries: 2,
      comparisonBlockers: ["documented incomplete preparation"],
    })
    expect(catalogRunAvailability(result.families[0].runs[1])).toBe(
      "Comparison blocked · 2/2 queries",
    )
  })

  test("invalidation overrides complete query execution", () => {
    const run = observation()
    run.metadata.invalidation = { reason: "Invalid inputs" }
    expect(database([run]).families[0].runs[0]).toMatchObject({
      outcome: "invalidated",
      completedQueries: 2,
      totalQueries: 2,
    })
  })

  test("keeps absent runs separate from unimplemented adapters and missing scale registration", () => {
    const missing = buildCatalogSuiteSummary(definition, [], manifest).databases.find(
      (entry) => entry.database === "missing",
    )
    expect(missing.families.every((family) => family.runs.length === 0)).toBe(true)
    expect(catalogMissingAvailability(missing, 1)).toBe("No recorded run · execution unknown")
    expect(catalogMissingAvailability(missing, 100)).toBe("Scale not registered")
    expect(catalogMissingAvailability({ ...missing, implemented: false }, 1)).toBe(
      "Adapter unimplemented",
    )
  })
})
