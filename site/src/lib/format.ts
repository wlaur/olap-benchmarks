export type DurationScaleMode = "linear" | "log"

const LOG_DURATION_TICKS_S = [0, 0.001, 0.01, 0.1, 1, 10, 60, 300, 600, 1800, 3600]

export function formatDurationSeconds(value: number): string {
  if (value >= 10) return `${value.toFixed(1)} s`
  if (value >= 1) return `${value.toFixed(2)} s`

  const milliseconds = value * 1000

  if (milliseconds >= 100) return `${milliseconds.toFixed(0)} ms`
  if (milliseconds >= 10) return `${milliseconds.toFixed(1)} ms`
  return `${milliseconds.toFixed(2)} ms`
}

export function formatMultiplier(value: number): string {
  return `${value.toFixed(2)}x`
}

export function scaleDurationForChart(value: number, mode: DurationScaleMode): number {
  return mode === "log" ? Math.log10(Math.max(0, value) + 1) : value
}

export function unscaleDurationForChart(value: number, mode: DurationScaleMode): number {
  return mode === "log" ? Math.max(0, 10 ** value - 1) : value
}

export function formatDurationAxisTick(value: number, mode: DurationScaleMode): string {
  const rawValue = unscaleDurationForChart(value, mode)
  return rawValue === 0 ? "0 s" : formatDurationSeconds(rawValue)
}

export function getDurationAxisDomain(
  maxDurationSeconds: number,
  mode: DurationScaleMode,
): [number, number] {
  const safeMax = Math.max(0, maxDurationSeconds)
  if (mode === "log") {
    return [0, scaleDurationForChart(safeMax, mode)]
  }

  if (safeMax === 0) {
    return [0, 0.001]
  }

  return [0, safeMax * 1.05]
}

export function getDurationAxisTicks(
  maxDurationSeconds: number,
  mode: DurationScaleMode,
): number[] | undefined {
  if (mode === "linear") return undefined

  const safeMax = Math.max(0, maxDurationSeconds)
  const rawTicks = LOG_DURATION_TICKS_S.filter((tick) => tick <= safeMax)

  if (rawTicks.length === 0 || rawTicks[0] !== 0) {
    rawTicks.unshift(0)
  }

  if (safeMax > 0 && rawTicks[rawTicks.length - 1] !== safeMax) {
    rawTicks.push(safeMax)
  }

  return rawTicks.map((tick) => scaleDurationForChart(tick, mode))
}
