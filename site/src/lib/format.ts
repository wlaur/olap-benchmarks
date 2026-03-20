export type DurationScaleMode = "linear" | "log"

const MIN_LOG_DURATION_TICK_EXPONENT = -3
const LINEAR_DURATION_TICK_TARGET_COUNT = 6
const DURATION_TICK_STEPS_S = [
  0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900,
  1800, 3600, 7200, 10800, 14400, 21600, 43200, 86400,
] as const
const LOG_DURATION_TICKS_S = [0.001, 0.01, 0.1, 1, 10, 60, 600, 3600, 21600, 43200, 86400] as const

function trimFixed(value: number, fractionDigits: number): string {
  return value
    .toFixed(fractionDigits)
    .replace(/\.0+$/, "")
    .replace(/(\.\d*[1-9])0+$/, "$1")
}

function roundToSignificantDigits(value: number, significantDigits: number): number {
  if (value === 0) return 0
  return Number(value.toPrecision(significantDigits))
}

function formatNumberWithSpaceGrouping(value: number, maximumFractionDigits: number): string {
  const parts = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits,
    useGrouping: true,
  }).formatToParts(value)

  return parts.map((part) => (part.type === "group" ? " " : part.value)).join("")
}

export function formatDurationSeconds(value: number): string {
  if (value <= 0) return "0s"
  if (value < 1) return formatMilliseconds(value * 1000)
  if (value < 60) return formatSecondsOnly(value)
  return formatDurationParts(value)
}

export function formatMultiplier(value: number): string {
  const roundedValue =
    value >= 100 ? roundToSignificantDigits(value, 2) : value >= 10 ? Math.round(value) : value

  return `${formatNumberWithSpaceGrouping(roundedValue, roundedValue >= 10 ? 0 : 2)}x`
}

export function scaleDurationForChart(value: number, mode: DurationScaleMode): number {
  return mode === "log" ? Math.log10(Math.max(0, value) + 1) : value
}

export function unscaleDurationForChart(value: number, mode: DurationScaleMode): number {
  return mode === "log" ? Math.max(0, 10 ** value - 1) : value
}

export function formatDurationAxisTick(value: number, mode: DurationScaleMode): string {
  const rawValue = unscaleDurationForChart(value, mode)
  return formatDurationSeconds(rawValue)
}

export function getDurationAxisDomain(
  maxDurationSeconds: number,
  mode: DurationScaleMode,
): [number, number] {
  const safeMax = Math.max(0, maxDurationSeconds)
  if (mode === "log") {
    const logTicks = getLogDurationTicks(safeMax)
    const upperBound = logTicks[logTicks.length - 1] ?? 1
    return [0, scaleDurationForChart(upperBound, mode)]
  }

  return [0, getLinearDurationAxisUpperBound(safeMax)]
}

export function getDurationAxisTicks(
  maxDurationSeconds: number,
  mode: DurationScaleMode,
): number[] | undefined {
  if (mode === "linear") {
    return getLinearDurationTicks(maxDurationSeconds)
  }

  return getLogDurationTicks(maxDurationSeconds).map((tick) => scaleDurationForChart(tick, mode))
}

function getLogDurationTicks(maxDurationSeconds: number): number[] {
  const safeMax = Math.max(0, maxDurationSeconds)
  const ticks = [0]
  for (const tick of LOG_DURATION_TICKS_S) {
    if (tick <= safeMax) {
      ticks.push(tick)
    }
  }

  if (ticks.length === 1) {
    ticks.push(
      10 **
        Math.max(MIN_LOG_DURATION_TICK_EXPONENT, Math.ceil(Math.log10(Math.max(safeMax, 0.001)))),
    )
    return ticks
  }

  const lastTick = ticks[ticks.length - 1] ?? 0
  if (lastTick < safeMax) {
    const nextTick = LOG_DURATION_TICKS_S.find((tick) => tick > safeMax)
    ticks.push(nextTick ?? Math.ceil(safeMax / 86400) * 86400)
  }

  return ticks
}

function getLinearDurationTicks(maxDurationSeconds: number): number[] {
  const safeMax = Math.max(0, maxDurationSeconds)
  const upperBound = getLinearDurationAxisUpperBound(safeMax)
  const step = getLinearDurationTickStep(upperBound)
  const tickCount = Math.max(1, Math.round(upperBound / step))

  return Array.from({ length: tickCount + 1 }, (_, index) => Number((index * step).toPrecision(12)))
}

function getLinearDurationAxisUpperBound(maxDurationSeconds: number): number {
  const safeMax = Math.max(0, maxDurationSeconds)
  if (safeMax === 0) {
    return 0.001
  }

  const step = getLinearDurationTickStep(safeMax)
  return Number((Math.ceil(safeMax / step) * step).toPrecision(12))
}

function getLinearDurationTickStep(maxDurationSeconds: number): number {
  const safeMax = Math.max(0.001, maxDurationSeconds)
  const rawStep = safeMax / (LINEAR_DURATION_TICK_TARGET_COUNT - 1)

  for (const step of DURATION_TICK_STEPS_S) {
    if (step >= rawStep) return step
  }

  const day = 86400
  return Math.ceil(rawStep / day) * day
}

function formatDurationParts(value: number): string {
  const totalSeconds = Math.round(value)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  const parts: string[] = []

  if (hours > 0) parts.push(`${hours}h`)
  if (minutes > 0) parts.push(`${minutes}m`)
  if (seconds > 0) parts.push(`${seconds}s`)

  if (parts.length === 0) return "0s"

  return parts.join(" ")
}

function formatSecondsOnly(value: number): string {
  const totalMilliseconds = Math.round(value * 1000)
  const seconds = Math.floor(totalMilliseconds / 1000)
  const milliseconds = totalMilliseconds % 1000

  if (milliseconds === 0) return `${seconds}s`
  if (value >= 10) return `${trimFixed(value, 1)}s`
  return `${trimFixed(value, 2)}s`
}

function formatMilliseconds(value: number): string {
  if (value >= 100) return `${trimFixed(value, 0)}ms`
  if (value >= 10) return `${trimFixed(value, 1)}ms`
  return `${trimFixed(value, 2)}ms`
}
