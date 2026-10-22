import { describe, expect, test } from "bun:test"

import { parseAnalyticalRun, parseAnalyticalSummary } from "../../src/lib/analytical.ts"
import { analyticalMetadata, analyticalSummary } from "../fixtures/analytical.ts"

const canonical = ["Q1", "Q2", "Q28"]
function parse(analytical = analyticalMetadata(), extra = {}) {
  return parseAnalyticalRun(
    {
      id: 1,
      suite: "clickbench",
      suite_scale_factor: 1,
      system: "fixture",
      db: "polars",
      db_version: "1",
      db_driver: null,
      status: "completed",
      finished_at: null,
      error_message: null,
      run_metadata: JSON.stringify({ analytical, ...extra }),
    },
    canonical,
  )
}

describe("qualified analytical summaries", () => {
  test("retains recorded QPS over the actual window and each percentile qualification", () => {
    const run = parse()
    expect(run.elapsedSeconds).toBe(65)
    expect(run.requestedSeconds).toBe(60)
    expect(run.overall.qps).toBe(120 / 65)
    expect(run.overall.percentiles.map((entry) => entry.status)).toEqual([
      "available",
      "available",
      "insufficient_samples",
    ])
    expect(run.overall.percentiles.map((entry) => entry.valueMs)).toEqual([50, 95, null])
    expect(
      parse(analyticalMetadata(1, 4)).overall.percentiles.every((entry) => entry.valueMs === null),
    ).toBe(true)
  })
  test("never derives a missing QPS or uses requested duration as elapsed time", () => {
    const metadata = analyticalMetadata()
    delete metadata.overall.throughput_per_second
    expect(parse(metadata).overall.qps).toBeNull()
    metadata.overall.throughput_per_second = 120 / 60
    expect(parse(metadata).overall.qps).toBeNull()
    metadata.overall.throughput_per_second = 120 / 65
    delete metadata.actual_elapsed_seconds
    expect(parse(metadata).overall.qps).toBeNull()
    expect(parse(metadata).elapsedSeconds).toBeNull()
  })
  test("zero completions is recorded zero throughput only with a valid window", () => {
    expect(parseAnalyticalSummary(analyticalSummary(0), 65).qps).toBe(0)
    expect(parseAnalyticalSummary(analyticalSummary(0), null).qps).toBeNull()
    expect(parseAnalyticalSummary(analyticalSummary(0), 0).qps).toBeNull()
    expect(
      parseAnalyticalSummary({ ...analyticalSummary(1, 1e10), throughput_per_second: 0 }, 1e10).qps,
    ).toBeNull()
    expect(parseAnalyticalSummary(analyticalSummary(1), Number.MIN_VALUE).qps).toBeNull()
  })
  test("contradictory request totals, sample counts and invalid numeric values stay unknown", () => {
    const summary = analyticalSummary()
    summary.offered = 10
    expect(parseAnalyticalSummary(summary, 65)).toMatchObject({
      completed: null,
      qps: null,
      samples: null,
    })
    const mismatched = analyticalSummary()
    mismatched.latency_ms.n = 121
    expect(
      parseAnalyticalSummary(mismatched, 65).percentiles.every((entry) => entry.valueMs === null),
    ).toBe(true)
    for (const value of [-1, Infinity, NaN, "120", null]) {
      expect(
        parseAnalyticalSummary({ ...analyticalSummary(), completed: value }, 65).completed,
      ).toBeNull()
    }
  })
  test("requires explicit status, method, threshold and enough samples without claiming confidence", () => {
    for (const change of [
      { status: "insufficient_samples", value_ms: 1 },
      { status: "available", minimum_n: 1 },
      { status: "available", value_ms: -1 },
      { status: "available", value_ms: "50" },
      { status: "available", minimum_n: 1000 },
    ]) {
      const summary = analyticalSummary()
      Object.assign(summary.latency_ms.p50, change)
      expect(parseAnalyticalSummary(summary, 65).percentiles[0].valueMs).toBeNull()
    }
    const summary = analyticalSummary()
    summary.latency_ms.percentile_method = "interpolated"
    expect(
      parseAnalyticalSummary(summary, 65).percentiles.every((entry) => entry.valueMs === null),
    ).toBe(true)
  })
})

describe("analytical workload and runtime evidence", () => {
  test("accounts for full canonical names and documented skips without erasing per-query counts", () => {
    const run = parse()
    expect(run).toMatchObject({
      manifestStatus: "documented skips",
      canonicalCount: 3,
      selectedCount: 2,
      skipCount: 1,
    })
    expect(run.queries.find((entry) => entry.name === "Q28")).toMatchObject({
      manifestStatus: "documented skip",
      skip: "unsupported: Documented analytical fixture skip",
    })
    expect(run.queries.find((entry) => entry.name === "Q2").summary).toMatchObject({
      completed: 0,
      qps: 0,
    })
    expect(run.budget).toMatchObject({ shared: true, threads: 10 })
  })
  test("a subset, extra query or undocumented skip cannot stand in for the canonical manifest", () => {
    const metadata = analyticalMetadata()
    metadata.excluded_queries = {}
    expect(parse(metadata).manifestStatus).toBe("incomplete")
    expect(parse(metadata).queries.find((entry) => entry.name === "Q28").manifestStatus).toBe(
      "unknown",
    )
    metadata.query_manifest.files.Q28 = "d".repeat(64)
    expect(parse(metadata).manifestStatus).toBe("full")
    metadata.query_manifest.files.Q99 = "e".repeat(64)
    expect(parse(metadata).manifestStatus).toBe("incomplete")
    const conflict = analyticalMetadata()
    conflict.excluded_queries.Q1 = { status: "skipped", reason: "conflicts with selected manifest" }
    expect(parse(conflict).manifestStatus).toBe("incomplete")
  })
  test("missing canonical names or malformed digest is coverage unknown, not full", () => {
    const run = parse()
    expect(parseAnalyticalRun(run.row, null).manifestStatus).toBe("unknown")
    const metadata = analyticalMetadata()
    metadata.query_manifest.digest = "invalid"
    expect(parse(metadata).manifestStatus).toBe("unknown")
    delete metadata.excluded_queries.Q28.reason
    expect(parse(metadata).skipCount).toBeNull()
    const invalidSkip = analyticalMetadata()
    invalidSkip.excluded_queries.Q28.status = "completed"
    expect(parse(invalidSkip).skipCount).toBeNull()
    expect(parse(invalidSkip).manifestStatus).toBe("incomplete")
  })
  test.each([
    null,
    {},
    { ...analyticalMetadata(), version: 2 },
    { ...analyticalMetadata(), track: "other" },
  ])("missing or unsupported metadata never hides a run or invents evidence", (metadata) => {
    const run = parse(metadata)
    expect(run.row.id).toBe(1)
    expect(run.overall.qps).toBeNull()
    expect(run.clients).toBeNull()
    expect(run.limitations.join()).toContain("metadata missing, invalid or unsupported")
    expect(
      parseAnalyticalRun({ ...run.row, run_metadata: "{broken" }, canonical).overall.completed,
    ).toBeNull()
  })
  test("budget scope, worker coverage, cancellation and invalidation retain their limitations", () => {
    const metadata = analyticalMetadata()
    metadata.clients = 2
    metadata.budget.multiplied_by_clients = true
    metadata.cancelled = true
    expect(parse(metadata, { invalidation: { reason: "fixture" } }).limitations.join()).toContain(
      "invalidation",
    )
    expect(parse(metadata).clients).toBeNull()
    expect(parse(metadata).budget.shared).toBe(false)
    expect(parse(metadata).limitations.join()).toContain("cancelled")
    metadata.clients = 4
    delete metadata.worker_runtime["3"]
    expect(parse(metadata).limitations.join()).toContain("Per-worker runtime evidence missing")
    expect(parse(metadata).limitations.join()).toContain(
      "configured limits alone do not establish enforcement",
    )
  })
})
