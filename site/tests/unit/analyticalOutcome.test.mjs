import { describe, expect, test } from "bun:test"

import { parseAnalyticalRun } from "../../src/lib/analytical.ts"
import {
  cancellationNote,
  percentileText,
  qpsCause,
  readableTime,
  stoppedOutcome,
  stoppedPhase,
  warmupSummary,
} from "../../src/lib/analyticalOutcome.ts"
import { analyticalMetadata } from "../fixtures/analytical.ts"

function failed(analytical, error = {}) {
  return parseAnalyticalRun(
    {
      id: 1,
      suite: "tpc_h",
      suite_scale_factor: 10,
      system: "fixture",
      db: "postgres",
      db_version: "18",
      db_driver: null,
      status: "failed",
      finished_at: null,
      error_type: null,
      error_message: null,
      ...error,
      run_metadata: JSON.stringify({ analytical }),
    },
    null,
  )
}

const unstarted = {
  ...analyticalMetadata(),
  measurement_started_at: null,
  measurement_finished_at: null,
  actual_elapsed_seconds: null,
  overall: null,
}

describe("stopped analytical run outcomes", () => {
  test("names recorded timeouts, cancellations and other failures plainly", () => {
    expect(stoppedOutcome(failed(unstarted, { error_type: "TimeoutError" }))).toBe("Timed out")
    expect(stoppedOutcome(failed(unstarted, { error_type: "KeyboardInterrupt" }))).toBe("Cancelled")
    expect(stoppedOutcome(failed({ ...unstarted, cancelled: true }))).toBe("Cancelled")
    expect(stoppedOutcome(failed(unstarted, { error_type: "OperationalError" }))).toBe("Failed")
  })

  test("labels warmup only while warmup is the last recorded phase", () => {
    const warmup = { ...unstarted, warmup_started_at: "2026-10-22T10:00:00+00:00" }
    expect(stoppedPhase(failed(warmup))).toBe("during warmup")
    expect(
      stoppedPhase(failed({ ...warmup, measurement_started_at: "2026-10-22T10:00:05+00:00" })),
    ).toBe("during measurement")
    expect(
      stoppedPhase(failed({ ...warmup, warmup_finished_at: "2026-10-22T10:00:05+00:00" })),
    ).toBeNull()
    expect(
      stoppedPhase(
        failed(unstarted, {
          error_type: "TimeoutError",
          error_message: "Query client-2:warmup:11 exceeded 45 seconds",
        }),
      ),
    ).toBe("during warmup")
    expect(stoppedPhase(failed(unstarted, { error_type: "KeyboardInterrupt" }))).toBeNull()
    expect(stoppedPhase(failed(analyticalMetadata()))).toBeNull()
  })

  test("explains unknown QPS from the recorded window", () => {
    expect(qpsCause(failed(unstarted))).toBe("QPS unknown: no measurement start recorded")
    expect(qpsCause(failed(analyticalMetadata()))).toBeNull()
  })

  test("keeps warmup counts separate and the cancellation cause unknown", () => {
    const warmup = {
      ...unstarted,
      warmup_summary: { offered: 14, completed: 13, errors: 0, timeouts: 1 },
    }
    const cancelled = failed(warmup, { error_type: "KeyboardInterrupt" })
    expect(warmupSummary(cancelled)).toMatchObject({ offered: 14, completed: 13, timeouts: 1 })
    expect(cancelled.overall.timeouts ?? 0).toBe(0)
    expect(cancellationNote(cancelled)).toBe(
      "Warmup recorded 1 timeout; cancellation cause not recorded",
    )
    expect(cancellationNote(failed(unstarted, { error_type: "KeyboardInterrupt" }))).toBe(
      "Cancellation cause not recorded",
    )
    expect(cancellationNote(failed(warmup, { error_type: "TimeoutError" }))).toBeNull()
    expect(warmupSummary(failed(unstarted))).toBeNull()
  })

  test("formats recorded times without microseconds or timezone guesses", () => {
    expect(readableTime("2026-10-22 13:09:22.351291")).toBe("22 Oct 2026, 13:09:22")
    expect(readableTime("2026-10-22T10:00:05+00:00")).toBe("22 Oct 2026, 10:00:05 UTC")
    expect(readableTime("2026-10-22T10:00:05.5+0200")).toBe("22 Oct 2026, 10:00:05 UTC+02:00")
    expect(readableTime(null)).toBeNull()
  })

  test("explains unavailable percentiles plainly", () => {
    expect(
      percentileText("p50", { valueMs: null, status: "insufficient_samples", minimumN: 5 }),
    ).toBe("p50 needs at least 5 samples")
    expect(percentileText("p99", { valueMs: null, status: "unknown", minimumN: 1000 })).toBe(
      "p99 not recorded",
    )
    expect(percentileText("p95", { valueMs: 2, status: "available", minimumN: 100 })).toBeNull()
  })
})
