import { memo, useCallback, useMemo, useRef, useState } from "react"

import { formatDurationSeconds, formatMultiplier } from "../lib/format"
import type { QueryComparisonRow } from "./QueryComparisonTable"

interface QueryHeatmapGridProps {
  rows: QueryComparisonRow[]
  databases: string[]
  databaseColors: Record<string, string>
  onSelectQuery?: (queryName: string) => void
  selectedQuery?: string | null
}

interface TooltipState {
  x: number
  y: number
  queryLabel: string
  queryName: string
  db: string
  duration: number | null
  ratio: number | null
  isFastest: boolean
}

const CELL_WIDTH = 64
const CELL_HEIGHT = 28
const CELL_GAP = 2
const LABEL_WIDTH = 160
const HEADER_HEIGHT = 80
const CELL_RX = 3
const LEGEND_HEIGHT = 40

const COLOR_STOPS: readonly [number, number, number][] = [
  [52, 211, 153],
  [250, 204, 21],
  [249, 115, 22],
  [239, 68, 68],
  [153, 27, 27],
]

const MISSING_COLOR = "rgba(148, 163, 184, 0.08)"
const HOVERED_STROKE = "rgba(255, 255, 255, 0.6)"

function interpolateColor(t: number): string {
  const clamped = Math.max(0, Math.min(1, t))
  const segmentCount = COLOR_STOPS.length - 1
  const segment = Math.min(Math.floor(clamped * segmentCount), segmentCount - 1)
  const segmentT = clamped * segmentCount - segment
  const from = COLOR_STOPS[segment]!
  const to = COLOR_STOPS[segment + 1]!
  const r = Math.round(from[0] + (to[0] - from[0]) * segmentT)
  const g = Math.round(from[1] + (to[1] - from[1]) * segmentT)
  const b = Math.round(from[2] + (to[2] - from[2]) * segmentT)
  const alpha = 0.45 + clamped * 0.3
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

function ratioToT(ratio: number, maxRatio: number): number {
  if (ratio <= 1) return 0
  const logMax = Math.log(Math.max(maxRatio, 1.01))
  return Math.min(1, Math.log(ratio) / logMax)
}

function ratioColor(ratio: number | null, maxRatio: number): string {
  if (ratio === null) return MISSING_COLOR
  return interpolateColor(ratioToT(ratio, maxRatio))
}

interface CellData {
  duration: number | null
  ratio: number | null
}

interface GridData {
  cells: CellData[][]
  maxRatio: number
}

function buildGridData(rows: QueryComparisonRow[], databases: string[]): GridData {
  let maxRatio = 1
  const cells = rows.map((row) => {
    const durations = databases.map((db) => row.by_database[db] ?? null)
    const validDurations = durations.filter((d): d is number => d !== null)
    const fastest = validDurations.length > 0 ? Math.min(...validDurations) : null

    return durations.map((duration) => {
      const ratio = duration !== null && fastest !== null && fastest > 0 ? duration / fastest : null
      if (ratio !== null && ratio > maxRatio) maxRatio = ratio
      return { duration, ratio }
    })
  })
  return { cells, maxRatio }
}

export function QueryHeatmapGrid({
  rows,
  databases,
  databaseColors,
  onSelectQuery,
  selectedQuery,
}: QueryHeatmapGridProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const [hoveredCell, setHoveredCell] = useState<{ row: number; col: number } | null>(null)

  const gridData = useMemo(() => buildGridData(rows, databases), [rows, databases])
  const { cells, maxRatio } = gridData

  const gridWidth = LABEL_WIDTH + databases.length * (CELL_WIDTH + CELL_GAP) - CELL_GAP
  const gridHeight =
    HEADER_HEIGHT + rows.length * (CELL_HEIGHT + CELL_GAP) - CELL_GAP + LEGEND_HEIGHT

  const handleMouseMove = useCallback(
    (event: React.MouseEvent, rowIdx: number, colIdx: number) => {
      const container = containerRef.current
      if (!container) return
      const rect = container.getBoundingClientRect()
      const row = rows[rowIdx]
      const db = databases[colIdx]
      if (!row || !db) return
      const cell = cells[rowIdx]?.[colIdx]
      setTooltip({
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
        queryLabel: row.query_label,
        queryName: row.query_name,
        db,
        duration: cell?.duration ?? null,
        ratio: cell?.ratio ?? null,
        isFastest: cell?.ratio === 1,
      })
      setHoveredCell({ row: rowIdx, col: colIdx })
    },
    [rows, databases, cells],
  )

  const handleMouseLeave = useCallback(() => {
    setTooltip(null)
    setHoveredCell(null)
  }, [])

  const handleClick = useCallback(
    (rowIdx: number) => {
      const row = rows[rowIdx]
      if (row) onSelectQuery?.(row.query_name)
    },
    [rows, onSelectQuery],
  )

  if (rows.length === 0 || databases.length === 0) {
    return (
      <div className="flex items-center justify-center rounded-lg bg-surface-inset px-4 py-8 text-xs text-slate-500">
        No data available for heatmap.
      </div>
    )
  }

  const legendY = HEADER_HEIGHT + rows.length * (CELL_HEIGHT + CELL_GAP) + 8

  return (
    <div ref={containerRef} className="relative select-none">
      <div className="panel-scrollbar overflow-x-auto overflow-y-hidden">
        <svg
          width={gridWidth}
          height={gridHeight}
          className="overflow-visible"
          style={{ minWidth: gridWidth }}
        >
          <ColumnHeaders
            databases={databases}
            databaseColors={databaseColors}
            hoveredCol={hoveredCell?.col ?? null}
          />

          {rows.map((row, rowIdx) => {
            const y = HEADER_HEIGHT + rowIdx * (CELL_HEIGHT + CELL_GAP)
            const isSelected = selectedQuery === row.query_name
            const isRowHovered = hoveredCell?.row === rowIdx

            return (
              <HeatmapRow
                key={row.query_name}
                row={row}
                rowIdx={rowIdx}
                y={y}
                databases={databases}
                cellData={cells[rowIdx] ?? []}
                maxRatio={maxRatio}
                isSelected={isSelected}
                isRowHovered={isRowHovered}
                hoveredCol={hoveredCell?.row === rowIdx ? (hoveredCell?.col ?? null) : null}
                onMouseMove={handleMouseMove}
                onMouseLeave={handleMouseLeave}
                onClick={handleClick}
              />
            )
          })}

          <ColorLegend y={legendY} maxRatio={maxRatio} gridWidth={gridWidth} />
        </svg>
      </div>

      {tooltip ? <HeatmapTooltip tooltip={tooltip} containerWidth={gridWidth} /> : null}
    </div>
  )
}

const LEGEND_BAR_HEIGHT = 10
const LEGEND_SWATCH_COUNT = 60

function ColorLegend({
  y,
  maxRatio,
  gridWidth,
}: {
  y: number
  maxRatio: number
  gridWidth: number
}) {
  const barWidth = Math.min(280, gridWidth - LABEL_WIDTH - 20)
  const barX = LABEL_WIDTH

  const tickRatios = useMemo(() => {
    const ticks: number[] = [1]
    const candidates = [1.5, 2, 3, 5, 10, 20, 50, 100, 500, 1000]
    for (const c of candidates) {
      if (c <= maxRatio) ticks.push(c)
    }
    if (maxRatio > 1 && !ticks.includes(Math.round(maxRatio))) {
      ticks.push(Math.round(maxRatio))
    }
    // Keep max ~5 ticks
    if (ticks.length > 6) {
      const step = Math.ceil(ticks.length / 5)
      const filtered = [ticks[0]!]
      for (let i = step; i < ticks.length - 1; i += step) filtered.push(ticks[i]!)
      filtered.push(ticks[ticks.length - 1]!)
      return filtered
    }
    return ticks
  }, [maxRatio])

  return (
    <g transform={`translate(0, ${y})`}>
      <text x={barX} y={-2} className="text-[10px]" fill="#64748b">
        vs fastest
      </text>

      {Array.from({ length: LEGEND_SWATCH_COUNT }, (_, i) => {
        const t = i / (LEGEND_SWATCH_COUNT - 1)
        const swatchWidth = barWidth / LEGEND_SWATCH_COUNT + 0.5
        return (
          <rect
            key={i}
            x={barX + (barWidth * i) / LEGEND_SWATCH_COUNT}
            y={0}
            width={swatchWidth}
            height={LEGEND_BAR_HEIGHT}
            fill={interpolateColor(t)}
            rx={i === 0 ? 2 : i === LEGEND_SWATCH_COUNT - 1 ? 2 : 0}
          />
        )
      })}

      {tickRatios.map((ratio) => {
        const t = ratioToT(ratio, maxRatio)
        const x = barX + t * barWidth
        return (
          <g key={ratio}>
            <line
              x1={x}
              y1={LEGEND_BAR_HEIGHT}
              x2={x}
              y2={LEGEND_BAR_HEIGHT + 4}
              stroke="#64748b"
              strokeWidth={1}
            />
            <text
              x={x}
              y={LEGEND_BAR_HEIGHT + 14}
              textAnchor="middle"
              className="text-[9px]"
              fill="#64748b"
            >
              {formatMultiplier(ratio)}
            </text>
          </g>
        )
      })}
    </g>
  )
}

function ColumnHeaders({
  databases,
  databaseColors,
  hoveredCol,
}: {
  databases: string[]
  databaseColors: Record<string, string>
  hoveredCol: number | null
}) {
  return (
    <g>
      {databases.map((db, colIdx) => {
        const x = LABEL_WIDTH + colIdx * (CELL_WIDTH + CELL_GAP) + CELL_WIDTH / 2
        const isHovered = hoveredCol === colIdx
        return (
          <g key={db}>
            <text
              x={x}
              y={HEADER_HEIGHT - 8}
              textAnchor="end"
              dominantBaseline="auto"
              transform={`rotate(-45, ${x}, ${HEADER_HEIGHT - 8})`}
              className="text-[10px]"
              fill={isHovered ? "#e2e8f0" : "#94a3b8"}
              fontWeight={isHovered ? 600 : 400}
            >
              {db}
            </text>
            <line
              x1={x}
              y1={HEADER_HEIGHT - 6}
              x2={x}
              y2={HEADER_HEIGHT - 2}
              stroke={databaseColors[db] ?? "#94a3b8"}
              strokeWidth={2}
              opacity={isHovered ? 1 : 0.5}
            />
          </g>
        )
      })}
    </g>
  )
}

interface HeatmapRowProps {
  row: QueryComparisonRow
  rowIdx: number
  y: number
  databases: string[]
  cellData: CellData[]
  maxRatio: number
  isSelected: boolean
  isRowHovered: boolean
  hoveredCol: number | null
  onMouseMove: (event: React.MouseEvent, rowIdx: number, colIdx: number) => void
  onMouseLeave: () => void
  onClick: (rowIdx: number) => void
}

const HeatmapRow = memo(function HeatmapRow({
  row,
  rowIdx,
  y,
  databases,
  cellData,
  maxRatio,
  isSelected,
  isRowHovered,
  hoveredCol,
  onMouseMove,
  onMouseLeave,
  onClick,
}: HeatmapRowProps) {
  return (
    <g>
      <text
        x={LABEL_WIDTH - 10}
        y={y + CELL_HEIGHT / 2}
        textAnchor="end"
        dominantBaseline="central"
        className="cursor-pointer text-[11px]"
        fill={isSelected ? "#e2e8f0" : isRowHovered ? "#cbd5e1" : "#94a3b8"}
        fontWeight={isSelected ? 600 : 400}
        onClick={() => onClick(rowIdx)}
      >
        {clipLabel(row.query_label, 22)}
      </text>

      {databases.map((_, colIdx) => {
        const cell = cellData[colIdx]
        if (!cell) return null
        const x = LABEL_WIDTH + colIdx * (CELL_WIDTH + CELL_GAP)
        const isCellHovered = isRowHovered && hoveredCol === colIdx
        const fill = ratioColor(cell.ratio, maxRatio)

        return (
          <g
            key={colIdx}
            onMouseMove={(e) => onMouseMove(e, rowIdx, colIdx)}
            onMouseLeave={onMouseLeave}
            onClick={() => onClick(rowIdx)}
            className="cursor-pointer"
          >
            <rect
              x={x}
              y={y}
              width={CELL_WIDTH}
              height={CELL_HEIGHT}
              rx={CELL_RX}
              fill={fill}
              stroke={
                isCellHovered ? HOVERED_STROKE : isSelected ? "rgba(108, 142, 239, 0.5)" : "none"
              }
              strokeWidth={isCellHovered ? 1.5 : isSelected ? 1 : 0}
            />
            {cell.duration !== null ? (
              <text
                x={x + CELL_WIDTH / 2}
                y={y + CELL_HEIGHT / 2}
                textAnchor="middle"
                dominantBaseline="central"
                className="pointer-events-none text-[10px]"
                fill={cell.ratio !== null && cell.ratio <= 1.1 ? "#d1fae5" : "#e2e8f0"}
              >
                {formatCellDuration(cell.duration)}
              </text>
            ) : (
              <text
                x={x + CELL_WIDTH / 2}
                y={y + CELL_HEIGHT / 2}
                textAnchor="middle"
                dominantBaseline="central"
                className="pointer-events-none text-[10px]"
                fill="#475569"
              >
                —
              </text>
            )}
          </g>
        )
      })}
    </g>
  )
})

function HeatmapTooltip({
  tooltip,
  containerWidth,
}: {
  tooltip: TooltipState
  containerWidth: number
}) {
  return (
    <div
      className="pointer-events-none absolute z-50 rounded-lg border border-border-default bg-[#161a23] px-3 py-2 text-xs text-slate-200 shadow-xl"
      style={{
        left: Math.min(tooltip.x + 12, containerWidth - 280),
        top: Math.max(0, tooltip.y - 8),
        maxWidth: 300,
      }}
    >
      <div className="mb-1 font-medium">{tooltip.queryLabel}</div>
      <div className="space-y-0.5 text-slate-400">
        <div>
          Database: <span className="text-slate-200">{tooltip.db}</span>
        </div>
        {tooltip.duration !== null ? (
          <div>
            Duration:{" "}
            <span className="text-slate-200">{formatDurationSeconds(tooltip.duration)}</span>
          </div>
        ) : (
          <div className="text-slate-500">No data</div>
        )}
        {tooltip.ratio !== null ? (
          <div>
            vs fastest:{" "}
            <span
              className={
                tooltip.isFastest
                  ? "font-medium text-emerald-400"
                  : tooltip.ratio <= 2
                    ? "text-emerald-300"
                    : tooltip.ratio <= 5
                      ? "text-amber-300"
                      : "text-red-300"
              }
            >
              {tooltip.isFastest ? "fastest" : formatMultiplier(tooltip.ratio)}
            </span>
          </div>
        ) : null}
      </div>
    </div>
  )
}

function formatCellDuration(seconds: number): string {
  if (seconds <= 0) return "0s"
  if (seconds < 0.001) return "<1ms"
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
  if (seconds < 10) return `${seconds.toFixed(1)}s`
  if (seconds < 60) return `${Math.round(seconds)}s`
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  return s > 0 ? `${m}m${s}s` : `${m}m`
}

function clipLabel(text: string, maxChars: number): string {
  if (text.length <= maxChars) return text
  return `${text.slice(0, maxChars - 1)}…`
}
