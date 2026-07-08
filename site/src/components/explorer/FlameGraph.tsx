import { memo, useCallback, useLayoutEffect, useMemo, useRef, useState } from "react"

import {
  buildSelectedBreadcrumbs,
  buildTimeTicks,
  buildVisibleRows,
  clipText,
  formatAxisTick,
  formatElapsedTooltip,
  getFlameSpanKey,
  getSpanBarLabel,
  getSpanBreadcrumbLabel,
  getSpanLabel,
  getSpanRect,
  getTickLabelPlacement,
  resolveZoomBreadcrumbs,
} from "../../lib/flameTransforms"
import { formatDurationSeconds } from "../../lib/format"
import type { BenchmarkOperation, FlameSpan } from "../../lib/types"
import { InlineButton } from "../controls/Control"
import { PortalCard } from "../controls/Popover"

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
  concurrent: {
    fill: "rgba(20, 184, 166, 0.3)",
    fillHover: "rgba(45, 212, 191, 0.5)",
    stroke: "rgba(45, 212, 191, 0.5)",
  },
  select: {
    fill: "rgba(99, 102, 241, 0.35)",
    fillHover: "rgba(99, 102, 241, 0.55)",
    stroke: "rgba(99, 102, 241, 0.6)",
  },
}

function getSpanColors(span: FlameSpan) {
  if (span.depth === "operation") {
    return OPERATION_COLORS[span.operation] ?? DEPTH_COLORS.operation
  }
  return DEPTH_COLORS[span.depth]
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

  const zoomBreadcrumbs = useMemo(() => resolveZoomBreadcrumbs(spans, zoomPath), [zoomPath, spans])
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

  const visibleRows = useMemo(
    () => buildVisibleRows(spans, viewStart, viewEnd),
    [spans, viewStart, viewEnd],
  )

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

      const currentZoomPath = zoomBreadcrumbs.map((crumb) => getFlameSpanKey(crumb))

      // If clicking the same span we're already zoomed into, do nothing extra
      const lastCrumb = zoomBreadcrumbs[zoomBreadcrumbs.length - 1]
      if (lastCrumb?.id === span.id) return

      // If clicking a parent in the breadcrumb trail, zoom back to it
      const spanKey = getFlameSpanKey(span)
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
          const { labelX, textAnchor } = getTickLabelPlacement(x, chartWidth)
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

      {tooltip && typeof document !== "undefined" ? (
        <PortalCard
          className="pointer-events-none z-[120] rounded-lg px-3 py-2 text-xs shadow-xl"
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
              Start: {formatElapsedTooltip(tooltip.span.elapsed_start_s, tooltip.span.duration_s)}
            </div>
            {tooltip.span.depth !== "operation" ? (
              <div className="capitalize">Operation: {tooltip.span.operation}</div>
            ) : null}
            {tooltip.span.depth !== "query" ? (
              <div className="mt-1 text-slate-500">Click to zoom in</div>
            ) : null}
          </div>
        </PortalCard>
      ) : null}
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
        const { x, width } = getSpanRect(span, viewStart, scale, chartWidth)
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
              width={width}
              height={ROW_HEIGHT}
              rx={2}
              fill={isHovered ? colors.fillHover : colors.fill}
              stroke={isSelected ? SELECTED_STROKE : colors.stroke}
              strokeWidth={isSelected ? 1.5 : 0.5}
            />
            {width > 36 ? (
              <text
                x={x + 5}
                y={ROW_HEIGHT / 2}
                dominantBaseline="central"
                className="pointer-events-none fill-slate-200 text-[11px]"
              >
                {clipText(getSpanBarLabel(span), width - 10)}
              </text>
            ) : null}
          </g>
        )
      })}
    </g>
  )
})
