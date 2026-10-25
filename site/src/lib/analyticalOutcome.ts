import type { AnalyticalRun } from "./analytical"

const recordedTime = (value: unknown) => typeof value === "string" && value.trim().length > 0

export function stoppedOutcome(run: AnalyticalRun): string {
  if (run.row.error_type === "TimeoutError") return "Timed out"
  if (run.row.error_type === "KeyboardInterrupt" || run.recorded?.cancelled === true)
    return "Cancelled"
  if (run.row.status === "failed") return "Failed"
  return run.row.status
    ? `${run.row.status.charAt(0).toUpperCase()}${run.row.status.slice(1)}`
    : "Unknown"
}

export function stoppedPhase(run: AnalyticalRun): "during warmup" | "during measurement" | null {
  const recorded = run.recorded
  const measurementStarted = recordedTime(recorded?.measurement_started_at)
  if (measurementStarted)
    return recordedTime(recorded?.measurement_finished_at) ? null : "during measurement"
  if (recordedTime(recorded?.warmup_finished_at)) return null
  if (
    recordedTime(recorded?.warmup_started_at) ||
    /^Query client-\d+:warmup:/.test(run.row.error_message ?? "")
  )
    return "during warmup"
  return null
}

export function qpsCause(run: AnalyticalRun): string | null {
  if (run.overall.qps !== null) return null
  if (run.startedAt === null) return "QPS unknown: no measurement start recorded"
  if (run.elapsedSeconds === null) return "QPS unknown: measurement window not recorded"
  return "QPS unknown: recorded rate, completed count or window inconsistent"
}
