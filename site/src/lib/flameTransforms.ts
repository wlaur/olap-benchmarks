import type { FlameSpan } from "./types"

const DEPTHS: FlameSpan["depth"][] = ["operation", "step", "query"]

const MIN_VISIBLE_PX = 0.5
const TICK_LABEL_EDGE_PADDING = 6
const TICK_LABEL_EDGE_THRESHOLD = 28

export interface FlameRowData {
  depth: FlameSpan["depth"]
  spans: FlameSpan[]
}

export function buildVisibleRows(
  spans: FlameSpan[],
  viewStart: number,
  viewEnd: number,
): FlameRowData[] {
  const result: FlameRowData[] = []
  for (const depth of DEPTHS) {
    const depthSpans = spans.filter((s) => {
      if (s.depth !== depth) return false
      return s.elapsed_end_s > viewStart && s.elapsed_start_s < viewEnd
    })
    if (depthSpans.length > 0) {
      result.push({ depth, spans: depthSpans })
    }
  }
  return result
}

export function resolveZoomBreadcrumbs(spans: FlameSpan[], zoomPath: string[]): FlameSpan[] {
  return zoomPath
    .map((zoomKey) => spans.find((span) => getFlameSpanKey(span) === zoomKey) ?? null)
    .filter((span): span is FlameSpan => span !== null)
}

export function getSpanRect(
  span: FlameSpan,
  viewStart: number,
  scale: number,
  chartWidth: number,
): { x: number; width: number } {
  const rawX = (span.elapsed_start_s - viewStart) * scale
  const rawEnd = (span.elapsed_end_s - viewStart) * scale
  const x = Math.max(0, rawX)
  const endX = Math.min(chartWidth, rawEnd)
  return { x, width: Math.max(MIN_VISIBLE_PX, endX - x) }
}

export function getTickLabelPlacement(
  x: number,
  chartWidth: number,
): { labelX: number; textAnchor: "start" | "middle" | "end" } {
  if (x <= TICK_LABEL_EDGE_THRESHOLD) {
    return { labelX: TICK_LABEL_EDGE_PADDING, textAnchor: "start" }
  }
  if (x >= chartWidth - TICK_LABEL_EDGE_THRESHOLD) {
    return { labelX: chartWidth - TICK_LABEL_EDGE_PADDING, textAnchor: "end" }
  }
  return { labelX: x, textAnchor: "middle" }
}

export function getSpanLabel(span: FlameSpan): string {
  if (span.depth === "operation") return span.operation
  if (span.depth === "step") {
    const name = span.query_name ?? span.step_name
    if (span.iteration !== null && span.iteration > 1) return `${name} #${span.iteration}`
    return name
  }
  return span.query_sql ? truncateSql(span.query_sql, 80) : (span.query_name ?? span.step_name)
}

export function getSpanBarLabel(span: FlameSpan): string {
  if (span.depth === "step" && span.query_name === null && span.step_name === span.operation) {
    return ""
  }
  return getSpanLabel(span)
}

function truncateSql(sql: string, maxLen: number): string {
  const cleaned = sql.replace(/\s+/g, " ").trim()
  return cleaned.length <= maxLen ? cleaned : `${cleaned.slice(0, maxLen)}...`
}

export function clipText(text: string, maxWidth: number): string {
  const approxCharWidth = 6.5
  const maxChars = Math.floor(maxWidth / approxCharWidth)
  if (text.length <= maxChars) return text
  return maxChars > 3 ? `${text.slice(0, maxChars - 1)}…` : ""
}

function formatElapsedCompact(seconds: number): string {
  if (seconds <= 0) return "0s"
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  return s > 0 ? `${m}m${s}s` : `${m}m`
}

export function formatElapsedTooltip(seconds: number, spanDurationS: number): string {
  if (spanDurationS >= 1) return formatElapsedCompact(seconds)
  if (seconds <= 0) return "0s"
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`
  if (seconds < 60) return `${seconds.toFixed(3)}s`
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60)
    const s = (seconds % 60).toFixed(3).padStart(6, "0")
    return `${m}m ${s}s`
  }
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = (seconds % 60).toFixed(3).padStart(6, "0")
  return `${h}h ${String(m).padStart(2, "0")}m ${s}s`
}

export function formatAxisTick(seconds: number, stepS: number): string {
  if (stepS >= 10) return formatElapsedCompact(seconds)
  if (seconds <= 0) return "0s"
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
  if (seconds < 60) return `${seconds.toFixed(3)}s`
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60)
    const s = (seconds % 60).toFixed(3).padStart(6, "0")
    return `${m}m ${s}s`
  }
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = (seconds % 60).toFixed(3).padStart(6, "0")
  return `${h}h ${String(m).padStart(2, "0")}m ${s}s`
}

export function getFlameSpanKey(span: FlameSpan): string {
  if (span.depth === "operation") {
    return `operation:${span.operation}`
  }

  if (span.depth === "step") {
    return `step:${span.operation}:${span.query_name ?? span.step_name}`
  }

  if (span.query_name !== null) {
    return `query:${span.operation}:${span.query_name}:${span.iteration ?? 0}`
  }

  return `query:${span.operation}:${span.step_name}:${normalizeFlameQuerySql(span.query_sql)}:${span.iteration ?? 0}`
}

function normalizeFlameQuerySql(querySql: string | null): string {
  return querySql?.replace(/\s+/g, " ").trim() ?? ""
}

export function getSpanBreadcrumbLabel(span: FlameSpan): string {
  if (span.depth === "operation") return span.operation
  if (span.depth === "step") return span.query_name ?? span.step_name
  if (span.iteration !== null && span.iteration > 1) {
    return `${span.query_name ?? span.step_name} #${span.iteration}`
  }
  return span.query_name ?? span.step_name
}

export function buildSelectedBreadcrumbs(
  spans: FlameSpan[],
  selectedSpan: FlameSpan | null,
): FlameSpan[] {
  if (!selectedSpan) return []

  const operationSpan =
    spans.find((span) => span.depth === "operation" && span.operation === selectedSpan.operation) ??
    (selectedSpan.depth === "operation" ? selectedSpan : null)
  const stepLabel = selectedSpan.query_name ?? selectedSpan.step_name
  const stepSpan =
    spans.find(
      (span) =>
        span.depth === "step" &&
        span.operation === selectedSpan.operation &&
        (span.query_name ?? span.step_name) === stepLabel,
    ) ?? (selectedSpan.depth === "step" ? selectedSpan : null)

  const breadcrumbs: FlameSpan[] = []
  const seen = new Set<string>()
  const addBreadcrumb = (span: FlameSpan | null) => {
    if (!span) return
    if (seen.has(span.id)) return
    seen.add(span.id)
    breadcrumbs.push(span)
  }

  addBreadcrumb(operationSpan)
  if (selectedSpan.depth !== "operation") addBreadcrumb(stepSpan)
  if (selectedSpan.depth === "query") addBreadcrumb(selectedSpan)
  return breadcrumbs
}

export function buildTimeTicks(startS: number, endS: number): { ticks: number[]; stepS: number } {
  const duration = endS - startS
  if (duration <= 0) return { ticks: [startS], stepS: 1 }
  const steps = [
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600,
    900, 1800, 3600,
  ]
  const targetCount = 6
  const rawStep = duration / targetCount
  const step = steps.find((s) => s >= rawStep) ?? Math.ceil(rawStep / 3600) * 3600
  const firstTick = Math.ceil(startS / step) * step
  const ticks: number[] = []
  for (let t = firstTick; t <= endS; t += step) ticks.push(Number(t.toPrecision(10)))
  return { ticks, stepS: step }
}
