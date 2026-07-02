const MB_NICE_STEPS = [1, 2, 5, 10, 20, 50, 100, 200, 500]
const GB_NICE_STEPS_MB = [1024, 2048, 5120, 10240, 20480, 51200, 102400, 204800, 512000, 1048576]

export function formatMegabytes(value: number): string {
  if (value >= 1024) {
    const gb = value / 1024
    return `${gb >= 10 ? gb.toFixed(0) : gb.toFixed(1)} GB`
  }
  return `${value.toFixed(0)} MB`
}

export function formatCpuPercent(value: number): string {
  if (value >= 1000) {
    const thousands = Math.floor(value / 1000)
    const remainder = Math.round(value % 1000)
    return remainder > 0
      ? `${thousands} ${String(remainder).padStart(3, "0")}%`
      : `${thousands} 000%`
  }
  return `${value.toFixed(0)}%`
}

export interface MetricScale {
  domain: [number, number]
  ticks: number[]
  formatter?: (v: number) => string
}

export type MetricScaleBuilder = (maxValue: number) => MetricScale

export function toMetricScale(maxValue: number): { domain: [number, number]; ticks: number[] } {
  const roughStep = maxValue <= 0 ? 1 : maxValue / 4
  const step = getNiceMetricStep(roughStep)
  const roundedMax = maxValue <= 0 ? step : Math.ceil(maxValue / step) * step
  const ticks: number[] = []
  for (let t = 0; t <= roundedMax; t += step) ticks.push(t)
  return { domain: [0, roundedMax], ticks }
}

export function toMemoryScale(maxValue: number): {
  domain: [number, number]
  ticks: number[]
  formatter: (v: number) => string
} {
  const roughStep = maxValue <= 0 ? 1 : maxValue / 4
  const step = getNiceMemoryStep(roughStep)
  const roundedMax = maxValue <= 0 ? step : Math.ceil(maxValue / step) * step
  const ticks: number[] = []
  for (let t = 0; t <= roundedMax; t += step) ticks.push(t)
  const useGb = step >= 1024
  const formatter = useGb ? formatMegabytesGb : formatMegabytes
  return { domain: [0, roundedMax], ticks, formatter }
}

function formatMegabytesGb(value: number): string {
  const gb = value / 1024
  return `${gb >= 10 ? gb.toFixed(0) : gb.toFixed(1)} GB`
}

function getNiceMetricStep(value: number): number {
  const exponent = Math.floor(Math.log10(Math.max(value, 1)))
  const magnitude = 10 ** exponent
  const normalized = value / magnitude
  if (normalized <= 1) return magnitude
  if (normalized <= 2) return 2 * magnitude
  if (normalized <= 5) return 5 * magnitude
  return 10 * magnitude
}

function getNiceMemoryStep(valueMb: number): number {
  if (valueMb >= 1024) {
    const found = GB_NICE_STEPS_MB.find((s) => s >= valueMb)
    return found ?? Math.ceil(valueMb / 1048576) * 1048576
  }
  const found = MB_NICE_STEPS.find((s) => s >= valueMb)
  return found ?? getNiceMetricStep(valueMb)
}
