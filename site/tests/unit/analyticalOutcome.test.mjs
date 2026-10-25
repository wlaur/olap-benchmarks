import { describe, expect, test } from "bun:test"

import { parseAnalyticalRun } from "../../src/lib/analytical.ts"
import { qpsCause, stoppedOutcome, stoppedPhase } from "../../src/lib/analyticalOutcome.ts"
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
})
