import { describe, expect, test } from "bun:test"

import { describeQueryOutcome, queryOutcomeSummary } from "../../src/lib/queryOutcomes.ts"

const completedRun = { status: "completed" }
const observation = (extra = {}) => ({
  steps: [{ resultStatus: "ok", status: "completed" }],
  evidence: "peer hash",
  evidenceReason: "Matching peer hashes",
  complete: true,
  reasons: [],
  ...extra,
})

describe("query presentation preserves evidence outcomes", () => {
  test("summary separates recorded medians from blocked evidence and absent singleton values", () => {
    const measured = describeQueryOutcome(observation(), completedRun, true, [])
    const blocked = describeQueryOutcome(observation(), { status: "failed" }, true, ["run failed"])
    const absent = describeQueryOutcome(undefined, undefined, false, [])
    expect(
      queryOutcomeSummary([
        { valueMs: 40.1, outcome: measured },
        { valueMs: 295, outcome: blocked },
      ]),
    ).toBe("2 recorded medians · 1 usable · 1 run blocked")
    expect(queryOutcomeSummary([{ valueMs: null, outcome: absent }])).toBe(
      "1 selected variant · 0 recorded medians · no comparison",
    )
  })
  test("absent observations do not imply unsupported execution or zero latency", () => {
    expect(describeQueryOutcome(undefined, completedRun, true, [])).toMatchObject({
      kind: "not-run",
      label: "Not run",
    })
    expect(describeQueryOutcome(observation({ steps: [] }), completedRun, true, [])).toMatchObject({
      kind: "not-run",
    })
  })
  test("a campaign exclusion keeps its skipped state without becoming an observed timeout", () => {
    const query = observation({
      steps: [{ resultStatus: "skipped", skipReason: "excluded by campaign query_exclude" }],
      complete: false,
    })
    expect(describeQueryOutcome(query, completedRun, true, [])).toMatchObject({
      kind: "skipped",
      label: "Skipped by campaign",
    })
  })
  test.each([
    ["timeout", "timeout", "Timed out"],
    ["unsupported", "unsupported", "Unsupported"],
    ["error", "error", "Error"],
    ["wrong_result", "conflict", "Wrong result"],
    ["skipped", "skipped", "Skipped"],
  ])("a recorded %s remains explicit even in a failed run", (status, kind, label) => {
    const query = observation({
      steps: [{ resultStatus: status, error: "recorded reason" }],
      complete: false,
    })
    expect(describeQueryOutcome(query, { status: "failed" }, true, ["run failed"])).toEqual({
      kind,
      label,
      detail: "recorded reason",
    })
  })
  test("partial medians, blocked runs and semantic exclusions cannot become measured comparisons", () => {
    expect(
      describeQueryOutcome(
        observation({ complete: false, reasons: ["Missing measured iteration"] }),
        completedRun,
        true,
        [],
      ),
    ).toMatchObject({ kind: "partial" })
    expect(
      describeQueryOutcome(observation(), completedRun, true, ["input fingerprint unknown"]),
    ).toMatchObject({ kind: "blocked" })
    expect(
      describeQueryOutcome(
        observation({ evidence: "excluded" }),
        { status: "invalidated" },
        false,
        ["run invalidated"],
      ),
    ).toMatchObject({ kind: "blocked", detail: "run invalidated" })
    expect(
      describeQueryOutcome(
        observation({ evidence: "excluded", evidenceReason: "Regex semantics differ" }),
        completedRun,
        false,
        [],
      ),
    ).toMatchObject({ kind: "excluded", detail: "Regex semantics differ" })
    expect(
      describeQueryOutcome(
        observation({ evidence: "conflict", evidenceReason: "Answers differ" }),
        completedRun,
        true,
        [],
      ),
    ).toMatchObject({ kind: "conflict" })
    expect(describeQueryOutcome(observation(), completedRun, true, [])).toMatchObject({
      kind: "measured",
    })
  })
})
