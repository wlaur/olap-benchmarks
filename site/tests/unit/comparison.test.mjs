import { describe, expect, test } from "bun:test"

import { parseComparisonManifest } from "../../src/lib/comparison.ts"
import { compareFamily, observeQuery } from "../../src/lib/observations.ts"

const family = {
  id: "clickbench",
  suite: "clickbench",
  title: "ClickBench",
  weight: 1,
  track: "raw",
  scale_factors: [1],
  query_names: ["Q1", "Q2"],
  coverage_query_names: ["Q1", "Q2", "Q28"],
}
const manifest = {
  version: 1,
  results_db_sha256: "a".repeat(64),
  policy_id: "fixed-workloads-v1",
  retrospective: true,
  families: [family],
  registrations: ["fast", "slow", "partial", "missing"].map((db) => ({
    db,
    suite: "clickbench",
    scale_factors: [1],
    operations: ["select"],
  })),
  answer_comparison_exclusions: [
    { suite: "clickbench", query_name: "Q28", db: null, reason: "different regex semantics" },
  ],
  annotations: [],
}

function run(db, durations = [1, 2], extra = {}) {
  return {
    runId: db.length,
    db,
    version: "1",
    driver: null,
    system: "host",
    suite: "clickbench",
    scale: 1,
    status: "completed",
    finishedAt: "2026-10-01",
    snapshotId: 1,
    metadata: {
      benchmark_profile: { warmup_iterations: 1, measured_iterations: 3 },
      input: { manifest_sha256: "same-input" },
    },
    snapshot: {},
    error: null,
    queries: new Map(
      durations.map((duration, index) => [
        `Q${index + 1}`,
        Array.from({ length: 4 }, (_, iteration) => ({
          id: index * 4 + iteration,
          queryName: `Q${index + 1}`,
          iteration: iteration + 1,
          role: iteration === 0 ? "warmup" : "measured",
          status: "completed",
          resultStatus: "ok",
          durationS: duration,
          rowCount: 1,
          hash: `answer-${index}`,
          hashVersion: "v1",
          hashMissingReason: null,
          skipReason: null,
          error: null,
        })),
      ]),
    ),
    ...extra,
  }
}

function compare(runs, policy = manifest, workload = family) {
  return compareFamily(workload, 1, "host", runs, policy)
}

describe("fixed publication performance", () => {
  test("uses raw geometric median ratios, no smoothing, penalties or success intersection", () => {
    const result = compare([
      run("fast", [0.00001, 2]),
      run("slow", [0.00004, 8]),
      run("partial", [0.000001]),
    ])
    expect(result.find((row) => row.db === "slow").score).toBeCloseTo(4)
    expect(result.find((row) => row.db === "fast").score).toBe(1)
    expect(result.find((row) => row.db === "partial")).toMatchObject({
      eligible: false,
      score: null,
      completed: 1,
      total: 3,
    })
    expect(result.find((row) => row.db === "missing")).toMatchObject({
      eligible: false,
      registration: "registered",
      run: null,
    })
    expect(result.find((row) => row.db === "missing").reasons.join()).toContain("unknown")
    const visible = result.filter((row) => row.db !== "fast")
    expect(visible.find((row) => row.db === "slow").score).toBeCloseTo(4)
  })
  test("uses measured median, retains coverage-only Q28 and does not block performance on its absence", () => {
    const value = run("fast")
    value.queries.get("Q1")[1].durationS = 1000
    const row = compare([value])[0]
    expect(row.eligible).toBe(true)
    expect(row.queries[0].medianS).toBe(1)
    expect(row.total).toBe(3)
    expect(row.queries[2]).toMatchObject({
      queryName: "Q28",
      evidence: "excluded",
      complete: false,
    })
  })
  test.each(["warmup", "measured"])("a missing %s iteration blocks eligibility", (role) => {
    const value = run("fast")
    value.queries.set(
      "Q1",
      value.queries
        .get("Q1")
        .filter((step, index) => !(step.role === role && (role === "warmup" || index === 1))),
    )
    expect(compare([value])[0].eligible).toBe(false)
  })
  test.each(["error", "timeout", "wrong_result", "skipped", "unsupported"])(
    "recorded %s is not a performance observation",
    (status) => {
      const value = run("fast")
      value.queries.get("Q1")[1].resultStatus = status
      expect(compare([value])[0].eligible).toBe(false)
    },
  )
  test("invalidation, failed run and documented incomplete preparation block a complete workload", () => {
    const value = run("fast")
    expect(compare([{ ...value, status: "failed" }])[0].eligible).toBe(false)
    expect(
      compare([
        { ...value, metadata: { ...value.metadata, invalidation: { reason: "bad input" } } },
      ])[0].eligible,
    ).toBe(false)
    const policy = {
      ...manifest,
      annotations: [
        {
          db: "fast",
          suite: "clickbench",
          scale_factor: 1,
          run_id: value.runId,
          preparation: "incomplete",
          tuning: null,
          summary: "not prepared",
          evidence_url: "https://example.com/evidence",
        },
      ],
    }
    expect(compare([value], policy)[0].reasons).toContain("documented incomplete preparation")
    expect(
      compare([value], { ...policy, annotations: [{ ...policy.annotations[0], run_id: 900 }] })[0]
        .eligible,
    ).toBe(true)
  })
  test("adapter implementation comes from registration, never a missing observation or SQL override", () => {
    const row = compare([run("fast")], {
      ...manifest,
      registrations: [
        ...manifest.registrations,
        { db: "specialist", suite: "other", scale_factors: [1], operations: ["select"] },
      ],
    }).find((entry) => entry.db === "specialist")
    expect(row.registration).toBe("unimplemented")
    expect(row.reasons.join()).toContain("registration snapshot")
  })
  test("family weights, scales and query counts do not create an overall or cross-family ranking", () => {
    const runs = [run("fast"), run("slow", [4, 8])]
    const original = compare(runs).map(({ key, score }) => [key, score])
    const unrelated = {
      ...family,
      id: "other",
      suite: "other",
      weight: 100,
      query_names: Array.from({ length: 100 }, (_, index) => `Q${index}`),
      coverage_query_names: Array.from({ length: 100 }, (_, index) => `Q${index}`),
    }
    expect(
      compare(runs, { ...manifest, families: [{ ...family, weight: 0.01 }, unrelated] }).map(
        ({ key, score }) => [key, score],
      ),
    ).toEqual(original)
    expect(
      compareFamily(family, 10, "host", runs, manifest).every((row) => row.score === null),
    ).toBe(true)
    expect(
      compare(runs, manifest, { ...family, track: "smoke" }).every((row) => row.score === null),
    ).toBe(true)
  })
})

describe("truthful validation evidence", () => {
  test("different or missing input fingerprints cannot produce a performance ranking", () => {
    const first = run("fast")
    const second = run("slow")
    second.metadata.input.manifest_sha256 = "different-input"
    const rows = compare([first, second]).filter((row) => row.run !== null)
    expect(rows.every((row) => row.score === null && !row.eligible)).toBe(true)
    expect(rows.every((row) => row.total === 3)).toBe(true)
    expect(rows[0].reasons.join()).toContain("different input fingerprints")
    delete first.metadata.input.manifest_sha256
    expect(compare([first]).find((row) => row.db === "fast").reasons).toContain(
      "input fingerprint unknown",
    )
  })
  test("an engine-specific semantic exclusion blocks required performance without changing coverage", () => {
    const rows = compare([run("fast"), run("slow")], {
      ...manifest,
      answer_comparison_exclusions: [
        ...manifest.answer_comparison_exclusions,
        { suite: "clickbench", query_name: "Q1", db: "slow", reason: "Different output semantics" },
      ],
    })
    const slow = rows.find((row) => row.db === "slow")
    expect(slow).toMatchObject({ eligible: false, score: null, completed: 2, total: 3 })
    expect(slow.reasons.join()).toContain("answer-comparison exclusion")
    expect(rows.find((row) => row.db === "fast").eligible).toBe(true)
  })
  test("peer agreement requires input fingerprint and matching hash version", () => {
    const value = run("fast")
    expect(observeQuery(value, "Q1", [value], manifest).evidence).toBe("repeat-only")
    expect(observeQuery(value, "Q1", [run("slow")], manifest).evidence).toBe("peer hash")
    expect(
      observeQuery(
        value,
        "Q1",
        [run("slow", [1, 2], { metadata: { input: { manifest_sha256: "different" } } })],
        manifest,
      ).evidence,
    ).toBe("repeat-only")
    const peer = run("slow")
    for (const step of peer.queries.get("Q1")) step.hashVersion = "v2"
    expect(observeQuery(value, "Q1", [peer], manifest).evidence).toBe("repeat-only")
  })
  test("exclusions are per-engine and cannot create peer agreement or conflict", () => {
    const value = run("fast")
    const peer = run("slow")
    for (const step of peer.queries.get("Q1")) step.hash = "different"
    expect(observeQuery(value, "Q1", [peer], manifest).evidence).toBe("conflict")
    const policy = {
      ...manifest,
      answer_comparison_exclusions: [
        { suite: "clickbench", query_name: "Q1", db: "slow", reason: "non-comparable" },
      ],
    }
    expect(observeQuery(value, "Q1", [peer], policy).evidence).toBe("repeat-only")
    expect(observeQuery(peer, "Q1", [value], policy)).toMatchObject({
      evidence: "excluded",
      evidenceReason: "non-comparable",
    })
  })
  test("missing hashes remain unknown and hash limits retain their actual reason", () => {
    const value = run("fast")
    for (const step of value.queries.get("Q1")) {
      step.hash = null
      step.hashVersion = null
    }
    expect(observeQuery(value, "Q1", [], manifest).evidence).toBe("unknown")
    value.queries.get("Q1")[0].hashMissingReason = "above cell limit"
    expect(observeQuery(value, "Q1", [], manifest)).toMatchObject({
      evidence: "hash-limited",
      evidenceReason: "above cell limit",
    })
  })
})

describe("publication contract", () => {
  test("requires exact results identity and explicit canonical metadata", () => {
    expect(parseComparisonManifest(manifest, manifest.results_db_sha256)).toEqual(manifest)
    expect(() => parseComparisonManifest(manifest, "b".repeat(64))).toThrow("does not match")
    expect(() =>
      parseComparisonManifest({ ...manifest, version: 2 }, manifest.results_db_sha256),
    ).toThrow("policy")
    expect(() =>
      parseComparisonManifest(
        { ...manifest, families: [{ ...family, query_names: ["not-in-coverage"] }] },
        manifest.results_db_sha256,
      ),
    ).toThrow("invalid fixed family")
    expect(
      parseComparisonManifest(
        { ...manifest, families: [{ ...family, track: "smoke", weight: 0 }] },
        manifest.results_db_sha256,
      ).families[0].weight,
    ).toBe(0)
  })
})

test("iteration identities and profile facts are required, independently of row counts", () => {
  const value = run("fast")
  value.queries.get("Q1")[3].iteration = 9
  expect(compare([value])[0].eligible).toBe(false)
  value.queries.get("Q1")[3].iteration = 2
  expect(compare([value])[0].eligible).toBe(false)
  value.queries.get("Q1")[3].iteration = 4
  value.metadata = {}
  expect(compare([value])[0].eligible).toBe(false)
})

test("validation conflict blocks performance without removing execution coverage", () => {
  const value = run("fast")
  value.queries.get("Q1")[3].hash = "different-repeat"
  const result = compare([value])[0]
  expect(result.eligible).toBe(false)
  expect(result.completed).toBe(2)
  expect(result.queries[0]).toMatchObject({
    executionComplete: true,
    complete: false,
    evidence: "conflict",
  })
})
