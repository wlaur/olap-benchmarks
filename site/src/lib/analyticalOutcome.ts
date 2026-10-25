import {
  parseAnalyticalSummary,
  type AnalyticalPercentile,
  type AnalyticalRun,
  type AnalyticalSummary,
} from "./analytical"

const recordedTime = (value: unknown) => typeof value === "string" && value.trim().length > 0

const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

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

export function warmupSummary(run: AnalyticalRun): AnalyticalSummary | null {
  const summary = run.recorded?.warmup_summary
  if (summary === undefined) return null
  const parsed = parseAnalyticalSummary(summary, null)
  return parsed.offered === null ? null : parsed
}

export function cancellationNote(run: AnalyticalRun): string | null {
  if (run.row.status === "completed" || stoppedOutcome(run) !== "Cancelled") return null
  const timeouts = warmupSummary(run)?.timeouts ?? 0
  return timeouts > 0
    ? `Warmup recorded ${timeouts} timeout${timeouts === 1 ? "" : "s"}; cancellation cause not recorded`
    : "Cancellation cause not recorded"
}

export function qpsCause(run: AnalyticalRun): string | null {
  if (run.overall.qps !== null) return null
  if (run.startedAt === null) return "QPS unknown: no measurement start recorded"
  if (run.elapsedSeconds === null) return "QPS unknown: measurement window not recorded"
  return "QPS unknown: recorded rate, completed count or window inconsistent"
}

export function readableTime(value: string | null): string | null {
  const match = value
    ?.trim()
    .match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/)
  if (!match) return value
  const [, year, month, day, hour, minute, second, zone] = match
  const offset =
    zone === undefined
      ? ""
      : zone === "Z" || /^[+-]00:?00$/.test(zone)
        ? " UTC"
        : ` UTC${zone.slice(0, 3)}:${zone.slice(-2)}`
  return `${Number(day)} ${months[Number(month) - 1]} ${year}, ${hour}:${minute}:${second}${offset}`
}

export function percentileText(label: string, percentile: AnalyticalPercentile): string | null {
  if (percentile.status === "insufficient_samples")
    return `${label} needs at least ${percentile.minimumN.toLocaleString("en-US")} samples`
  if (percentile.valueMs === null) return `${label} not recorded`
  return null
}
