import { describe, expect, test } from "bun:test"

import { compareFamily } from "../../src/lib/observations.ts"
import { aggregateOverview } from "../../src/lib/overall.ts"

function family(id = "a", count = 2, scales = [1], extra = {}) {
  const names = Array.from({ length: count }, (_, i) => `Q${i + 1}`)
  return {
    id,
    suite: id,
    title: id,
    weight: 1,
    track: "raw",
    scale_factors: scales,
    query_names: names,
    coverage_query_names: names,
    ...extra,
  }
}

function run(db, workload, times = workload.query_names.map(() => 1), scale = 1, extra = {}) {
  return {
    runId: db.length * 100 + workload.id.charCodeAt(0) + scale,
    db,
    version: "1",
    driver: null,
    system: "host",
    suite: workload.suite,
    scale,
    status: "completed",
    finishedAt: "2026-10-01",
    snapshotId: 1,
    snapshot: {},
    error: null,
    metadata: {
      benchmark_profile: { warmup_iterations: 1, measured_iterations: 3 },
      input: { manifest_sha256: "same" },
    },
    queries: new Map(
      times.flatMap((time, index) =>
        time === null
          ? []
          : [
              [
                workload.query_names[index],
                Array.from({ length: 4 }, (_, i) => ({
                  id: index * 4 + i,
                  queryName: workload.query_names[index],
                  iteration: i + 1,
                  role: i === 0 ? "warmup" : "measured",
                  status: "completed",
                  resultStatus: "ok",
                  durationS: time,
                  rowCount: 1,
                  hash: `answer-${index}`,
                  hashVersion: "v1",
                  hashMissingReason: null,
                  skipReason: null,
                  error: null,
                })),
              ],
            ],
      ),
    ),
    ...extra,
  }
}

function overview(families, runs, extra = {}) {
  const engines = [...new Set([...runs.map((r) => r.db), "unmeasured"])]
  const manifest = {
    families,
    registrations: engines.flatMap((db) =>
      families.map((f) => ({
        db,
        suite: f.suite,
        operations: ["select"],
        scale_factors: f.scale_factors,
      })),
    ),
    annotations: [],
    answer_comparison_exclusions: [],
    ...extra,
  }
  return families.map((f) => ({
    family: f,
    scales: f.scale_factors.map((scale) => ({
      scale,
      rows: compareFamily(f, scale, "host", runs, manifest),
    })),
  }))
}

function score(families, runs, extra) {
  return aggregateOverview(overview(families, runs, extra))
}

const engine = (result, db) => result.rows.find((row) => row.db === db)

describe("bounded logarithmic overall query utility", () => {
  test.each([
    [1, 100, 0, 1],
    [10, 50, 0, 2],
    [100, 0, 2, 2],
    [1000, 0, 2, 2],
  ])("a %px slowdown earns %p points with usable coverage", (ratio, points, capped, rank) => {
    const f = family(),
      result = score([f], [run("ref", f), run("peer", f, [ratio, ratio])])
    expect(result.policy).toBe("bounded-log-utility-v1")
    expect(engine(result, "ref")).toMatchObject({ score: 100, coverage: 100, rank: 1 })
    expect(engine(result, "peer")).toMatchObject({
      score: points,
      coverage: 100,
      recordedGapCoverage: 0,
      unmeasuredCoverage: 0,
      scoredQueries: 2,
      rank,
      rankRange: [rank, rank],
    })
    expect(engine(result, "peer").families[0].cappedQueries).toBe(capped)
  })

  test("extreme finite durations clamp utility without losing usable coverage", () => {
    const f = family("a", 1),
      result = score([f], [run("ref", f, [1e-300]), run("slow", f, [1e300])])
    expect(engine(result, "ref")).toMatchObject({ score: 100, coverage: 100 })
    expect(engine(result, "slow")).toMatchObject({
      score: 0,
      coverage: 100,
      scoredQueries: 1,
      rank: 2,
    })
    expect(engine(result, "slow").families[0].cappedQueries).toBe(1)
  })

  test("credits individually complete queries without discarding a partial suite", () => {
    const f = family()
    const data = overview(
      [f],
      [run("reference", f), run("slow", f, [10, 10]), run("partial", f, [1, null])],
    )
    expect(data[0].scales[0].rows.find((r) => r.db === "partial").eligible).toBe(false)
    const result = aggregateOverview(data)
    expect(engine(result, "reference")).toMatchObject({ score: 100, coverage: 100 })
    expect(engine(result, "slow")).toMatchObject({ score: 50, coverage: 100 })
    expect(engine(result, "partial")).toMatchObject({
      score: 50,
      coverage: 50,
      recordedGapCoverage: 50,
      unmeasuredCoverage: 0,
      scoredQueries: 1,
      totalQueries: 2,
    })
    expect(engine(result, "unmeasured")).toMatchObject({
      score: 0,
      coverage: 0,
      recordedGapCoverage: 0,
      unmeasuredCoverage: 100,
      rank: null,
      rankRange: null,
    })
  })

  test("equal family votes prevent query counts from dominating; gaps retain their weight", () => {
    const a = family("a", 1),
      b = family("b", 100)
    const result = score(
      [a, b],
      [run("ref", a), run("peer", a), run("specialist", a), run("ref", b), run("peer", b)],
    )
    expect(engine(result, "specialist")).toMatchObject({
      score: 50,
      coverage: 50,
      workloadsWithData: 1,
      rank: 3,
      rankRange: [1, 3],
      unrankedOmissions: 1,
    })
    expect(result.queryCount).toBe(101)
    expect(result.familyCount).toBe(2)
  })

  test("scales share one family vote, including unmeasured declared scales", () => {
    const f = family("a", 1, [1, 10])
    const result = score(
      [f],
      [
        run("ref", f),
        run("peer", f),
        run("only-small", f),
        run("ref", f, [1], 10),
        run("peer", f, [1], 10),
      ],
    )
    expect(engine(result, "only-small")).toMatchObject({ score: 50, coverage: 50 })
    expect(engine(result, "ref").score).toBe(100)
  })

  test("each family divides its vote equally across its scales and fixed queries", () => {
    const a = family("a", 2, [1, 10]),
      b = family("b", 1)
    const result = score(
      [a, b],
      [
        run("ref", a),
        run("peer", a),
        run("partial", a, [1, null]),
        run("ref", a, [1, 1], 10),
        run("peer", a, [1, 1], 10),
        run("partial", a, [10, 100], 10),
        run("ref", b),
        run("peer", b),
        run("partial", b, [10]),
      ],
    )
    const row = engine(result, "partial")
    expect(row).toMatchObject({
      score: 43.75,
      coverage: 87.5,
      recordedGapCoverage: 12.5,
      unmeasuredCoverage: 0,
      scoredQueries: 4,
      totalQueries: 5,
    })
    expect(row.families[0]).toMatchObject({ score: 37.5, coverage: 75, cappedQueries: 1 })
    expect(row.families[1]).toMatchObject({ score: 50, coverage: 100 })
    expect(result.queryCount).toBe(5)
  })

  test("published family weights are normalized once, never around an engine's gaps", () => {
    const a = family("a", 1, [1], { weight: 3 }),
      b = family("b", 1)
    const result = score(
      [a, b],
      [run("ref", a), run("peer", a), run("partial", a), run("ref", b), run("peer", b)],
    )
    expect(engine(result, "partial").score).toBe(75)
    expect(engine(result, "partial")).toMatchObject({
      coverage: 75,
      recordedGapCoverage: 0,
      unmeasuredCoverage: 25,
    })
  })

  test("smoke, prepared and supplementary tracks never add family votes", () => {
    const a = family(),
      b = family("b", 10, [1], { weight: 0 }),
      c = family("c", 1, [1], { track: "prepared" }),
      d = family("d", 1, [1], { track: "smoke" })
    const result = score(
      [a, b, c, d],
      [
        run("ref", a),
        run("peer", a),
        run("specialist", b),
        run("specialist", c),
        run("specialist", d),
      ],
    )
    expect(result.familyCount).toBe(1)
    expect(result.queryCount).toBe(2)
    expect(engine(result, "specialist").score).toBe(0)
  })

  test("arbitrary extra query observations never enter the fixed score", () => {
    const f = family(),
      ref = run("ref", f),
      peer = run("peer", f, [10, 10])
    peer.queries.set("extra", run("x", f, [0.00001]).queries.get("Q1"))
    expect(engine(score([f], [ref, peer]), "peer").score).toBe(50)
  })

  test("a sole scorable run cannot establish a cell reference", () => {
    const f = family()
    expect(() => score([f], [run("only", f)])).toThrow(
      "At least two scorable runs required: a, SF 1",
    )
  })

  test("scorable runs in another scale cannot fill a cell's missing competitor", () => {
    const f = family("a", 1, [1, 10])
    expect(() => score([f], [run("ref", f), run("peer", f), run("ref", f, [1], 10)])).toThrow(
      "At least two scorable runs required: a, SF 10",
    )
  })

  test("withholding the only competitor's query aborts publication", () => {
    const f = family(),
      fast = run("fast", f, [1, 2]),
      slow = run("slow", f, [10, 1])
    const before = score([f], [fast, slow])
    expect(engine(before, "fast").rank).toBe(1)
    expect(() => score([f], [fast, run("slow", f, [null, 1])])).toThrow(
      "At least two usable query results required: a, SF 1, Q1",
    )
  })

  test("two selected runs with disjoint complete queries cannot establish references", () => {
    const f = family()
    expect(() => score([f], [run("first", f, [1, null]), run("second", f, [null, 1])])).toThrow(
      "At least two usable query results required: a, SF 1, Q1",
    )
  })

  test("dropping one's slow query with two peers remaining cannot improve score or rank", () => {
    const f = family(),
      fast = run("fast", f, [1, 10]),
      peer = run("peer", f, [2, 1]),
      before = score([f], [fast, peer, run("withholding", f, [10, 10])]),
      after = score([f], [fast, peer, run("withholding", f, [null, 10])])
    expect(engine(before, "withholding")).toMatchObject({ score: 50, rank: 3 })
    expect(engine(after, "withholding")).toMatchObject({ score: 25, coverage: 50, rank: 3 })
    for (const db of ["fast", "peer"]) expect(engine(after, db)).toEqual(engine(before, db))
  })

  test.each([0, NaN, Infinity, -1])(
    "invalid duration %p is excluded from score and reference",
    (bad) => {
      const f = family(),
        result = score([f], [run("ref", f), run("peer", f, [10, 10]), run("bad", f, [bad, bad])])
      expect(engine(result, "bad")).toMatchObject({
        score: 0,
        coverage: 0,
        recordedGapCoverage: 100,
        unmeasuredCoverage: 0,
        rank: null,
      })
      expect(engine(result, "peer").score).toBe(50)
    },
  )

  test.each(["timeout", "error", "unsupported", "skipped", "wrong_result"])(
    "%s remains a zero-credit position",
    (status) => {
      const f = family(),
        partial = run("partial", f)
      partial.queries.get("Q2")[2].resultStatus = status
      const row = engine(score([f], [run("ref", f), run("peer", f), partial]), "partial")
      expect(row).toMatchObject({ score: 50, coverage: 50, recordedGapCoverage: 50 })
      expect(Object.keys(row.families[0].gaps)).toContain(status.replaceAll("_", " "))
    },
  )

  test.each([
    ["missing warmup", (steps) => steps.shift()],
    ["missing measured iteration", (steps) => steps.pop()],
    ["extra warmup", (steps) => steps.push({ ...steps[0], id: 99, iteration: 5 })],
    ["extra measured iteration", (steps) => steps.push({ ...steps[1], id: 99, iteration: 5 })],
    ["duplicate iteration", (steps) => (steps[2].iteration = steps[1].iteration)],
    ["missing iteration identity", (steps) => (steps[2].iteration = null)],
    ["incorrect iteration role", (steps) => (steps[2].role = "warmup")],
    ["failed warmup", (steps) => (steps[0].status = "failed")],
    ["failed measured iteration", (steps) => (steps[2].status = "failed")],
    ["recorded iteration error", (steps) => (steps[2].error = "execution failed")],
  ])("%s earns zero for that query while retaining the fixed denominator", (_, alter) => {
    const f = family(),
      partial = run("partial", f)
    alter(partial.queries.get("Q2"))
    const row = engine(score([f], [run("ref", f), run("peer", f), partial]), "partial")
    expect(row).toMatchObject({
      score: 50,
      coverage: 50,
      recordedGapCoverage: 50,
      unmeasuredCoverage: 0,
      scoredQueries: 1,
      totalQueries: 2,
    })
  })

  test("a complete two-warmup profile cannot replace the one-warmup publication policy", () => {
    const f = family(),
      alternate = run("alternate", f, [0.001, 0.001])
    alternate.metadata.benchmark_profile.warmup_iterations = 2
    for (const steps of alternate.queries.values()) {
      steps.splice(1, 0, { ...steps[0], id: steps[0].id + 100 })
      steps.forEach((step, index) => {
        step.iteration = index + 1
      })
    }
    const data = overview([f], [run("ref", f), run("peer", f, [10, 10]), alternate]),
      observations = data[0].scales[0].rows.find((row) => row.db === "alternate").queries
    for (const query of observations)
      expect(query).toMatchObject({ complete: true, warmup: 2, measured: 3 })
    const result = aggregateOverview(data)
    expect(engine(result, "alternate")).toMatchObject({
      score: 0,
      coverage: 0,
      recordedGapCoverage: 100,
      unmeasuredCoverage: 0,
      rank: null,
    })
    expect(engine(result, "peer").score).toBe(50)
  })

  test.each(["failed", "running"])("a %s run cannot supply reference or score", (status) => {
    const f = family()
    const result = score(
      [f],
      [run("ref", f), run("peer", f, [10, 10]), run("bad", f, [0.001, 0.001], 1, { status })],
    )
    expect(engine(result, "bad")).toMatchObject({
      score: 0,
      coverage: 0,
      recordedGapCoverage: 100,
      unmeasuredCoverage: 0,
      scoredQueries: 0,
      rank: null,
    })
    expect(engine(result, "peer").score).toBe(50)
  })

  test("invalidation and run errors exclude the entire cell", () => {
    const f = family(),
      bad = run("bad", f, [0.001, 0.001])
    for (const extra of [
      { error: "broken" },
      { metadata: { ...bad.metadata, invalidation: { reason: "wrong data" } } },
    ]) {
      const result = score([f], [run("ref", f), run("peer", f, [10, 10]), { ...bad, ...extra }])
      expect(engine(result, "bad")).toMatchObject({
        score: 0,
        coverage: 0,
        recordedGapCoverage: 100,
        unmeasuredCoverage: 0,
        rank: null,
      })
      expect(engine(result, "peer").score).toBe(50)
    }
  })

  test("documented incomplete preparation blocks otherwise complete queries", () => {
    const f = family(),
      bad = run("bad", f, [0.001, 0.001])
    const result = score([f], [run("ref", f), run("peer", f, [10, 10]), bad], {
      annotations: [
        { db: "bad", suite: "a", scale_factor: 1, run_id: bad.runId, preparation: "incomplete" },
      ],
    })
    expect(engine(result, "bad")).toMatchObject({
      score: 0,
      coverage: 0,
      recordedGapCoverage: 100,
      unmeasuredCoverage: 0,
      rank: null,
    })
    expect(engine(result, "peer").score).toBe(50)
  })

  test.each([undefined, null, {}, { manifest_sha256: "" }, { manifest_sha256: 42 }])(
    "unknown input %p blocks every query in the selected run",
    (input) => {
      const f = family(),
        bad = run("bad", f, [0.001, 0.001])
      bad.metadata.input = input
      const result = score([f], [run("ref", f), run("peer", f, [10, 10]), bad])
      expect(engine(result, "bad")).toMatchObject({
        score: 0,
        coverage: 0,
        recordedGapCoverage: 100,
        unmeasuredCoverage: 0,
        rank: null,
      })
      expect(engine(result, "bad").families[0].gaps).toEqual({ "input fingerprint unknown": 2 })
      expect(engine(result, "peer").score).toBe(50)
    },
  )

  test("a blocked peer does not count toward the two-scorable-run requirement", () => {
    const f = family()
    expect(() =>
      score([f], [run("ref", f), run("bad", f, [1, 1], 1, { status: "failed" })]),
    ).toThrow("At least two scorable runs required: a, SF 1")
  })

  test.each(["repetitions", "peers", "incomplete query"])(
    "unresolved answer disagreement between %s aborts publication",
    (kind) => {
      const f = family(),
        bad = run("bad", f)
      if (kind === "peers") {
        for (const step of bad.queries.get("Q2")) step.hash = "different"
      } else {
        bad.queries.get("Q2")[2].hash = "different"
        if (kind === "incomplete query") bad.queries.get("Q2").pop()
      }
      expect(() => score([f], [run("ref", f), run("peer", f), bad])).toThrow(
        "Unresolved answer conflict: bad, a, SF 1, Q2",
      )
    },
  )

  test("a coverage-only answer conflict in an otherwise scorable run aborts publication", () => {
    const observed = family("a", 3),
      f = { ...observed, query_names: ["Q1", "Q2"] },
      bad = run("bad", observed)
    bad.queries.get("Q3")[2].hash = "different"
    const data = overview([f], [run("ref", observed), run("peer", observed), bad])
    expect(data[0].scales[0].rows.find((row) => row.db === "bad").eligible).toBe(true)
    expect(() => aggregateOverview(data)).toThrow("Unresolved answer conflict: bad, a, SF 1, Q3")
  })

  test("a scoped semantic exclusion zeros only that engine's third query", () => {
    const f = family("a", 3)
    const result = score([f], [run("ref", f), run("peer", f), run("excluded", f)], {
      answer_comparison_exclusions: [
        { suite: "a", query_name: "Q3", db: "excluded", reason: "different semantics" },
      ],
    })
    const row = engine(result, "excluded")
    expect(row.score).toBeCloseTo(200 / 3)
    expect(row.coverage).toBeCloseTo(200 / 3)
    expect(row.recordedGapCoverage).toBeCloseTo(100 / 3)
    expect(row).toMatchObject({ scoredQueries: 2, totalQueries: 3, unmeasuredCoverage: 0 })
    expect(row.families[0].gaps).toEqual({ "Comparison excluded": 1 })
    for (const db of ["ref", "peer"])
      expect(engine(result, db)).toMatchObject({ score: 100, coverage: 100, scoredQueries: 3 })
  })

  test("a scoped exclusion cannot leave a weighted query with only one usable result", () => {
    const f = family()
    expect(() =>
      score([f], [run("ref", f), run("excluded", f)], {
        answer_comparison_exclusions: [
          { suite: "a", query_name: "Q2", db: "excluded", reason: "different semantics" },
        ],
      }),
    ).toThrow("At least two usable query results required: a, SF 1, Q2")
  })

  test("a complete query with documented hash limits retains utility and its actual evidence", () => {
    const f = family(),
      limited = run("limited", f, [10, 10])
    for (const steps of limited.queries.values()) {
      for (const step of steps) {
        step.hash = null
        step.hashVersion = null
        step.hashMissingReason = "result exceeds hash size limit"
      }
    }
    const data = overview([f], [run("ref", f), limited]),
      observations = data[0].scales[0].rows.find((row) => row.db === "limited").queries
    for (const query of observations)
      expect(query).toMatchObject({
        complete: true,
        evidence: "hash-limited",
        evidenceReason: "result exceeds hash size limit",
      })
    expect(engine(aggregateOverview(data), "limited")).toMatchObject({
      score: 50,
      coverage: 100,
      recordedGapCoverage: 0,
      scoredQueries: 2,
    })
  })

  test.each([
    [1, 1],
    [1, null],
  ])("different inputs fail closed even with query durations %p and %p", (first, second) => {
    const f = family(),
      different = run("different", f, [first, second])
    different.metadata.input.manifest_sha256 = "other"
    expect(() => score([f], [run("ref", f), run("peer", f), different])).toThrow(
      "Input fingerprints differ: a, SF 1",
    )
  })

  test.each([{}, { version: "2" }, { driver: "alternate" }])(
    "multiple publication variants %p abort instead of selecting the best timing",
    (variant) => {
      const f = family(),
        slow = run("variant", f, [10, 10]),
        fast = run("variant", f, [0.001, 0.001], 1, { ...variant, runId: 999 })
      expect(() => score([f], [run("ref", f), run("peer", f), slow, fast])).toThrow(
        "Multiple publication variants: variant, a, SF 1",
      )
    },
  )

  test("an existing run at an unregistered scale aborts publication", () => {
    const f = family()
    expect(() =>
      score([f], [run("ref", f), run("peer", f), run("unregistered", f)], {
        registrations: ["ref", "peer", "unregistered"].map((db) => ({
          db,
          suite: "a",
          operations: ["select"],
          scale_factors: db === "unregistered" ? [10] : [1],
        })),
      }),
    ).toThrow("Run exists for an unregistered scale: unregistered, a, SF 1")
  })

  test("unimplemented and registered unmeasured gaps differ from blocked selected runs", () => {
    const f = family(),
      result = score(
        [f],
        [run("ref", f), run("peer", f), run("blocked", f, [1, 1], 1, { status: "failed" })],
        {
          registrations: ["ref", "peer", "blocked", "unmeasured", "unimplemented"].map((db) => ({
            db,
            suite: "a",
            operations: db === "unimplemented" ? [] : ["select"],
            scale_factors: [1],
          })),
        },
      )
    for (const [db, reason] of [
      ["unimplemented", "Adapter unimplemented"],
      ["unmeasured", "Not measured"],
      ["blocked", "run failed"],
    ]) {
      const row = engine(result, db)
      expect(row).toMatchObject({ score: 0, coverage: 0, scoredQueries: 0, rank: null })
      expect(row.families[0].gaps).toEqual({ [reason]: 2 })
      expect(row.recordedGapCoverage).toBe(db === "blocked" ? 100 : 0)
      expect(row.unmeasuredCoverage).toBe(db === "blocked" ? 0 : 100)
    }
  })

  test("usable, recorded gaps and unmeasured coverage partition the full weighted workload", () => {
    const f = family("a", 1, [1, 10, 100]),
      runs = f.scale_factors.flatMap((scale) => [
        run("ref", f, [1], scale),
        run("peer", f, [1], scale),
      ])
    runs.push(run("partial", f, [100]), run("partial", f, [1], 10, { status: "failed" }))
    const result = score([f], runs),
      row = engine(result, "partial")
    expect(row).toMatchObject({ score: 0, scoredQueries: 1, totalQueries: 3, rank: 3 })
    expect(row.coverage).toBeCloseTo(100 / 3)
    expect(row.recordedGapCoverage).toBeCloseTo(100 / 3)
    expect(row.unmeasuredCoverage).toBeCloseTo(100 / 3)
    expect(row.families[0].gaps).toEqual({ "run failed": 1, "Not measured": 1 })
    for (const entry of result.rows)
      expect(entry.coverage + entry.recordedGapCoverage + entry.unmeasuredCoverage).toBeCloseTo(100)
  })

  test("hiding or reordering returned rows preserves absolute scores and ranks", () => {
    const f = family(),
      result = score([f], [run("ref", f), run("peer", f, [10, 10])])
    const before = engine(result, "peer")
    const filtered = result.rows.filter((row) => row.db !== "ref").toReversed()
    expect(filtered.find((row) => row.db === "peer")).toMatchObject({ score: 50, rank: 2 })
    expect(filtered.find((row) => row.db === "peer")).toBe(before)
    expect(before.rankRange).toEqual([2, 2])
  })

  test("ties share competition rank and zero-evidence rows never appear first", () => {
    const f = family(),
      result = score([f], [run("ref", f), run("peer", f), run("slow", f, [10, 10])])
    expect(engine(result, "ref").rank).toBe(1)
    expect(engine(result, "peer").rank).toBe(1)
    expect(engine(result, "slow").rank).toBe(3)
    expect(engine(result, "unmeasured").rank).toBeNull()
  })

  test("display precision determines tied ranks and alphabetical order within a tie", () => {
    const f = family("a", 1),
      result = score(
        [f],
        [
          run("ref", f),
          run("z-faster", f, [100 ** 0.4996]),
          run("a-slower", f, [100 ** 0.4999]),
          run("trailing", f, [100 ** 0.5006]),
        ],
      )
    expect(engine(result, "z-faster").score).toBeCloseTo(50.04)
    expect(engine(result, "a-slower").score).toBeCloseTo(50.01)
    expect(engine(result, "trailing").score).toBeCloseTo(49.94)
    for (const db of ["z-faster", "a-slower"])
      expect(engine(result, db)).toMatchObject({ rank: 2, rankRange: [2, 2] })
    expect(engine(result, "trailing").rank).toBe(4)
    expect(result.rows.map((row) => row.db)).toEqual([
      "ref",
      "a-slower",
      "z-faster",
      "trailing",
      "unmeasured",
    ])
  })

  test("rank sensitivity includes the full rank and every leave-one-family-out rank", () => {
    const a = family("a", 1),
      b = family("b", 1),
      result = score(
        [a, b],
        [
          run("balanced", a, [10]),
          run("balanced", b, [10]),
          run("a-specialist", a),
          run("a-specialist", b, [100 ** 0.9]),
          run("b-specialist", a, [100 ** 0.9]),
          run("b-specialist", b),
        ],
      )
    expect(engine(result, "balanced")).toMatchObject({
      score: 50,
      rank: 3,
      rankRange: [2, 3],
      unrankedOmissions: 0,
    })
    for (const db of ["a-specialist", "b-specialist"])
      expect(engine(result, db)).toMatchObject({ rank: 1, rankRange: [1, 3] })
  })

  test("ties use the displayed toFixed value at a half-decimal boundary", () => {
    const a = family("a", 1, [1], { weight: 23 }),
      b = family("b", 1, [1], { weight: 1977 }),
      result = score(
        [a, b],
        [
          run("ref", a),
          run("peer", a),
          run("ref", b),
          run("peer", b),
          run("boundary", a),
          run("nearby", a, [100 ** (1 - 1.14 / 1.15)]),
        ],
      )
    expect(engine(result, "boundary").score).toBe(1.15)
    expect(engine(result, "boundary").score.toFixed(1)).toBe("1.1")
    expect(engine(result, "nearby").score.toFixed(1)).toBe("1.1")
    expect(engine(result, "boundary").rank).toBe(3)
    expect(engine(result, "nearby").rank).toBe(3)
  })

  test("a ranked zero-credit result sorts before unranked evidence gaps", () => {
    const f = family("a", 1),
      result = score([f], [run("ref", f), run("peer", f), run("z-capped", f, [100])])
    expect(engine(result, "z-capped")).toMatchObject({ score: 0, coverage: 100, rank: 3 })
    expect(result.rows.at(-1).db).toBe("unmeasured")
  })

  test("scored contributions remain bounded by fixed weighted coverage", () => {
    const a = family("a", 3, [1, 10]),
      b = family("b", 2)
    const result = score(
      [a, b],
      [
        run("ref", a),
        run("peer", a, [2, 4, null]),
        run("third", a),
        run("ref", a, [1, 1, 1], 10),
        run("peer", a, [2, 2, 2], 10),
        run("ref", b),
        run("peer", b, [4, 4]),
      ],
    )
    for (const row of result.rows) {
      expect(row.score).toBeGreaterThanOrEqual(0)
      expect(row.coverage).toBeLessThanOrEqual(100)
      expect(row.score).toBeLessThanOrEqual(row.coverage)
      expect(
        row.families.reduce(
          (sum, f) =>
            sum +
            f.scoredQueries +
            Object.values(f.gaps).reduce((gapSum, count) => gapSum + count, 0),
          0,
        ),
      ).toBe(row.totalQueries)
      expect(row.coverage + row.recordedGapCoverage + row.unmeasuredCoverage).toBeCloseTo(100)
    }
  })
})
