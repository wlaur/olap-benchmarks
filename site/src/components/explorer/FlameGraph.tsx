import { memo, useCallback, useLayoutEffect, useMemo, useRef, useState } from "react"
import { createPortal } from "react-dom"

import { formatDurationSeconds } from "../../lib/format"
import type { BenchmarkOperation, FlameSpan } from "../../lib/types"
import { InlineButton } from "../controls/Control"

interface FlameGraphProps {
  spans: FlameSpan[]
  selectedSpan: FlameSpan | null
  onSelectSpan: (span: FlameSpan | null) => void
  zoomPath: string[]
  onZoomPathChange: (zoomPath: string[]) => void
}

interface TooltipState {
  x: number
  y: number
  span: FlameSpan
}

const ROW_HEIGHT = 26
const ROW_GAP = 1
const TICK_AREA_HEIGHT = 20
const MIN_VISIBLE_PX = 0.5
const TICK_LABEL_EDGE_PADDING = 6
const TICK_LABEL_EDGE_THRESHOLD = 28

const DEPTH_COLORS: Record<
  FlameSpan["depth"],
  { fill: string; fillHover: string; stroke: string }
> = {
  operation: {
    fill: "rgba(99, 102, 241, 0.35)",
    fillHover: "rgba(99, 102, 241, 0.55)",
    stroke: "rgba(99, 102, 241, 0.6)",
  },
  step: {
    fill: "rgba(56, 189, 248, 0.3)",
    fillHover: "rgba(56, 189, 248, 0.5)",
    stroke: "rgba(56, 189, 248, 0.5)",
  },
  query: {
    fill: "rgba(52, 211, 153, 0.25)",
    fillHover: "rgba(52, 211, 153, 0.45)",
    stroke: "rgba(52, 211, 153, 0.45)",
  },
}

const SELECTED_STROKE = "rgba(255, 255, 255, 0.8)"

const OPERATION_COLORS: Partial<
  Record<BenchmarkOperation, { fill: string; fillHover: string; stroke: string }>
> = {
  populate: {
    fill: "rgba(250, 204, 21, 0.3)",
    fillHover: "rgba(250, 204, 21, 0.5)",
    stroke: "rgba(250, 204, 21, 0.5)",
  },
  mutate: {
    fill: "rgba(249, 115, 22, 0.3)",
    fillHover: "rgba(249, 115, 22, 0.5)",
    stroke: "rgba(249, 115, 22, 0.5)",
  },
  select: {
    fill: "rgba(99, 102, 241, 0.35)",
    fillHover: "rgba(99, 102, 241, 0.55)",
    stroke: "rgba(99, 102, 241, 0.6)",
  },
}

const DEPTHS: FlameSpan["depth"][] = ["operation", "step", "query"]

function getSpanColors(span: FlameSpan) {
  if (span.depth === "operation") {
    return OPERATION_COLORS[span.operation] ?? DEPTH_COLORS.operation
  }
  return DEPTH_COLORS[span.depth]
}

function getSpanLabel(span: FlameSpan): string {
  if (span.depth === "operation") return span.operation
  if (span.depth === "step") {
    const name = span.query_name ?? span.step_name
    if (span.iteration !== null && span.iteration > 1) return `${name} #${span.iteration}`
    return name
  }
  return span.query_sql ? truncateSql(span.query_sql, 80) : (span.query_name ?? span.step_name)
}

function getSpanBarLabel(span: FlameSpan): string {
  if (span.depth === "step" && span.query_name === null && span.step_name === span.operation) {
    return ""
  }
  return getSpanLabel(span)
}

function truncateSql(sql: string, maxLen: number): string {
  const cleaned = sql.replace(/\s+/g, " ").trim()
  return cleaned.length <= maxLen ? cleaned : `${cleaned.slice(0, maxLen)}...`
}

export function FlameGraph({
  spans,
  selectedSpan,
  onSelectSpan,
  zoomPath,
  onZoomPathChange,
}: FlameGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [containerWidth, setContainerWidth] = useState(800)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const [hoveredId, setHoveredId] = useState<string | null>(null)

  useLayoutEffect(() => {
    const el = containerRef.current
    if (!el) return

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) setContainerWidth(entry.contentRect.width)
    })
    observer.observe(el)
    return () => observer.disconnect()
  }, [])

  const zoomBreadcrumbs = useMemo(
    () =>
      zoomPath
        .map(
          (zoomKey) => spans.find((span) => getFlameSpanPersistenceKey(span) === zoomKey) ?? null,
        )
        .filter((span): span is FlameSpan => span !== null),
    [zoomPath, spans],
  )
  const zoom = zoomBreadcrumbs[zoomBreadcrumbs.length - 1] ?? null
  const selectedBreadcrumbs = useMemo(
    () => buildSelectedBreadcrumbs(spans, selectedSpan),
    [selectedSpan, spans],
  )
  const isShowingZoomBreadcrumbs = zoomBreadcrumbs.length > 0
  const visibleBreadcrumbs = isShowingZoomBreadcrumbs ? zoomBreadcrumbs : selectedBreadcrumbs

  const globalEnd = useMemo(() => Math.max(0, ...spans.map((s) => s.elapsed_end_s)), [spans])
  const globalStart = 0

  const viewStart = zoom?.elapsed_start_s ?? globalStart
  const viewEnd = zoom?.elapsed_end_s ?? globalEnd
  const viewDuration = viewEnd - viewStart

  // Build visible rows: only show spans overlapping the current view
  const visibleRows = useMemo(() => {
    const result: { depth: FlameSpan["depth"]; spans: FlameSpan[] }[] = []
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
  }, [spans, viewStart, viewEnd])

  const chartWidth = containerWidth
  const scale = viewDuration > 0 ? chartWidth / viewDuration : 1
  const svgHeight = visibleRows.length * (ROW_HEIGHT + ROW_GAP) - ROW_GAP + TICK_AREA_HEIGHT

  const handleMouseMove = useCallback((event: React.MouseEvent, span: FlameSpan) => {
    setTooltip({
      x: event.clientX,
      y: event.clientY,
      span,
    })
    setHoveredId(span.id)
  }, [])

  const handleMouseLeave = useCallback(() => {
    setTooltip(null)
    setHoveredId(null)
  }, [])

  const handleClick = useCallback(
    (span: FlameSpan) => {
      // Select the span for detail view
      onSelectSpan(selectedSpan?.id === span.id ? null : span)

      // Zoom: only zoom into operation or step spans (they have children)
      if (span.depth === "query") return

      const currentZoomPath = zoomBreadcrumbs.map((crumb) => getFlameSpanPersistenceKey(crumb))

      // If clicking the same span we're already zoomed into, do nothing extra
      const lastCrumb = zoomBreadcrumbs[zoomBreadcrumbs.length - 1]
      if (lastCrumb?.id === span.id) return

      // If clicking a parent in the breadcrumb trail, zoom back to it
      const spanKey = getFlameSpanPersistenceKey(span)
      const crumbIndex = currentZoomPath.findIndex((crumbKey) => crumbKey === spanKey)
      if (crumbIndex >= 0) {
        onZoomPathChange(currentZoomPath.slice(0, crumbIndex + 1))
        return
      }

      onZoomPathChange([...currentZoomPath, spanKey])
    },
    [selectedSpan, onSelectSpan, zoomBreadcrumbs, onZoomPathChange],
  )

  const handleResetBreadcrumbs = useCallback(() => {
    if (isShowingZoomBreadcrumbs) {
      onZoomPathChange([])
      return
    }
    onSelectSpan(null)
  }, [isShowingZoomBreadcrumbs, onSelectSpan, onZoomPathChange])

  const handleBreadcrumbClick = useCallback(
    (index: number) => {
      if (index < 0) {
        onZoomPathChange([])
        return
      }
      if (isShowingZoomBreadcrumbs) {
        onZoomPathChange(zoomPath.slice(0, index + 1))
        return
      }

      const span = selectedBreadcrumbs[index]
      if (span) onSelectSpan(span)
    },
    [isShowingZoomBreadcrumbs, onSelectSpan, onZoomPathChange, selectedBreadcrumbs, zoomPath],
  )

  const tickInfo = useMemo(() => buildTimeTicks(viewStart, viewEnd), [viewStart, viewEnd])

  if (spans.length === 0) {
    return (
      <div className="rounded-xl border border-dashed border-border-default bg-surface-inset px-6 py-8 text-sm text-slate-500">
        No execution data found for this database.
      </div>
    )
  }

  return (
    <div ref={containerRef} className="relative select-none">
      {/* Breadcrumb / zoom bar */}
      <div className="mb-2 flex items-center gap-1 text-[11px]">
        <InlineButton
          size="xs"
          onClick={handleResetBreadcrumbs}
          active={visibleBreadcrumbs.length === 0}
        >
          All
        </InlineButton>
        {visibleBreadcrumbs.map((crumb, i) => (
          <span key={crumb.id} className="flex items-center gap-1">
            <span className="text-slate-600">/</span>
            <InlineButton
              size="xs"
              onClick={() => handleBreadcrumbClick(i)}
              active={i === visibleBreadcrumbs.length - 1}
            >
              {getSpanBreadcrumbLabel(crumb)}
            </InlineButton>
          </span>
        ))}
      </div>

      <svg
        width={containerWidth}
        height={svgHeight}
        className="overflow-visible"
        style={{ willChange: "transform" }}
      >
        {/* Time axis ticks */}
        {tickInfo.ticks.map((t) => {
          const x = (t - viewStart) * scale
          const labelX =
            x <= TICK_LABEL_EDGE_THRESHOLD
              ? TICK_LABEL_EDGE_PADDING
              : x >= chartWidth - TICK_LABEL_EDGE_THRESHOLD
                ? chartWidth - TICK_LABEL_EDGE_PADDING
                : x
          const textAnchor =
            x <= TICK_LABEL_EDGE_THRESHOLD
              ? "start"
              : x >= chartWidth - TICK_LABEL_EDGE_THRESHOLD
                ? "end"
                : "middle"
          return (
            <g key={t}>
              <line
                x1={x}
                y1={0}
                x2={x}
                y2={svgHeight - TICK_AREA_HEIGHT}
                stroke="rgba(148, 163, 184, 0.06)"
                strokeDasharray="3 3"
              />
              <text
                x={labelX}
                y={svgHeight - 4}
                textAnchor={textAnchor}
                className="fill-slate-500 text-[10px]"
              >
                {formatAxisTick(t, tickInfo.stepS)}
              </text>
            </g>
          )
        })}

        {/* Flame rows */}
        {visibleRows.map((row, rowIndex) => {
          const y = rowIndex * (ROW_HEIGHT + ROW_GAP)
          return (
            <FlameRow
              key={row.depth}
              y={y}
              spans={row.spans}
              viewStart={viewStart}
              scale={scale}
              chartWidth={chartWidth}
              selectedId={selectedSpan?.id ?? null}
              hoveredId={hoveredId}
              onMouseMove={handleMouseMove}
              onMouseLeave={handleMouseLeave}
              onClick={handleClick}
            />
          )
        })}
      </svg>

      {tooltip && typeof document !== "undefined"
        ? createPortal(
            <div
              className="pointer-events-none fixed z-[120] rounded-lg border border-border-default bg-[#161a23] px-3 py-2 text-xs text-slate-200 shadow-xl"
              style={{
                left: Math.min(tooltip.x + 12, window.innerWidth - 332),
                top: Math.max(8, tooltip.y - 8),
                maxWidth: 320,
              }}
            >
              <div className="mb-1 font-medium">{getSpanLabel(tooltip.span)}</div>
              <div className="space-y-0.5 text-slate-400">
                <div>Duration: {formatDurationSeconds(tooltip.span.duration_s)}</div>
                <div>
                  Start:{" "}
                  {formatElapsedTooltip(tooltip.span.elapsed_start_s, tooltip.span.duration_s)}
                </div>
                {tooltip.span.depth !== "operation" ? (
                  <div className="capitalize">Operation: {tooltip.span.operation}</div>
                ) : null}
                {tooltip.span.depth !== "query" ? (
                  <div className="mt-1 text-slate-500">Click to zoom in</div>
                ) : null}
              </div>
            </div>,
            document.body,
          )
        : null}
    </div>
  )
}

interface FlameRowProps {
  y: number
  spans: FlameSpan[]
  viewStart: number
  scale: number
  chartWidth: number
  selectedId: string | null
  hoveredId: string | null
  onMouseMove: (event: React.MouseEvent, span: FlameSpan) => void
  onMouseLeave: () => void
  onClick: (span: FlameSpan) => void
}

const FlameRow = memo(function FlameRow({
  y,
  spans,
  viewStart,
  scale,
  chartWidth,
  selectedId,
  hoveredId,
  onMouseMove,
  onMouseLeave,
  onClick,
}: FlameRowProps) {
  return (
    <g transform={`translate(0, ${y})`}>
      {spans.map((span) => {
        const rawX = (span.elapsed_start_s - viewStart) * scale
        const rawEnd = (span.elapsed_end_s - viewStart) * scale
        const x = Math.max(0, rawX)
        const endX = Math.min(chartWidth, rawEnd)
        const w = Math.max(MIN_VISIBLE_PX, endX - x)
        const colors = getSpanColors(span)
        const isSelected = selectedId === span.id
        const isHovered = hoveredId === span.id
        const canZoom = span.depth !== "query"

        return (
          <g
            key={span.id}
            onMouseMove={(e) => onMouseMove(e, span)}
            onMouseLeave={onMouseLeave}
            onClick={() => onClick(span)}
            className={canZoom ? "cursor-zoom-in" : "cursor-pointer"}
          >
            <rect
              x={x}
              y={0}
              width={w}
              height={ROW_HEIGHT}
              rx={2}
              fill={isHovered ? colors.fillHover : colors.fill}
              stroke={isSelected ? SELECTED_STROKE : colors.stroke}
              strokeWidth={isSelected ? 1.5 : 0.5}
            />
            {w > 36 ? (
              <text
                x={x + 5}
                y={ROW_HEIGHT / 2}
                dominantBaseline="central"
                className="pointer-events-none fill-slate-200 text-[11px]"
              >
                {clipText(getSpanBarLabel(span), w - 10)}
              </text>
            ) : null}
          </g>
        )
      })}
    </g>
  )
})

function clipText(text: string, maxWidth: number): string {
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

function formatElapsedTooltip(seconds: number, spanDurationS: number): string {
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

function formatAxisTick(seconds: number, stepS: number): string {
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

function getFlameSpanPersistenceKey(span: FlameSpan): string {
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

function getSpanBreadcrumbLabel(span: FlameSpan): string {
  if (span.depth === "operation") return span.operation
  if (span.depth === "step") return span.query_name ?? span.step_name
  if (span.iteration !== null && span.iteration > 1) {
    return `${span.query_name ?? span.step_name} #${span.iteration}`
  }
  return span.query_name ?? span.step_name
}

function buildSelectedBreadcrumbs(spans: FlameSpan[], selectedSpan: FlameSpan | null): FlameSpan[] {
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

function buildTimeTicks(startS: number, endS: number): { ticks: number[]; stepS: number } {
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
