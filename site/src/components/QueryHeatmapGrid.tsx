import { memo, useCallback, useMemo, useRef, useState } from "react"

import type { SelectionState } from "../hooks/useSelectionState"
import { clamp, formatDurationSeconds, formatMultiplier } from "../lib/format"
import { highlightSqlTokens } from "../lib/highlightSql"
import type { QuerySqlEntry } from "../lib/types"
import type { QueryComparisonRow } from "./QueryComparisonTable"

interface QueryHeatmapGridProps {
  rows: QueryComparisonRow[]
  databases: string[]
  databaseColors: Record<string, string>
  selection: SelectionState
  sqlBySuite: Record<string, QuerySqlEntry> | null
}

interface TooltipState {
  x: number
  y: number
  queryLabel: string
  queryName: string
  tableFamily: string
  queryId: string
  db: string
  duration: number | null
  ratio: number | null
  isFastest: boolean
}

interface QueryLabelTooltipState {
  x: number
  y: number
  queryLabel: string
  queryName: string
  tableFamily: string
  queryId: string
  sql: string | null
}

const CELL_HEIGHT = 28
const COL_GAP = 3
const ROW_GAP = 3
const CELL_RX = 3
const LABEL_WIDTH = 180
const HEADER_HEIGHT = 34
const LEGEND_HEIGHT = 40
const APPROX_CHAR_WIDTH = 7.5

const COLOR_STOPS: readonly [number, number, number][] = [
  [52, 211, 153],
  [250, 204, 21],
  [249, 115, 22],
  [239, 68, 68],
  [153, 27, 27],
]

const MISSING_COLOR = "rgba(148, 163, 184, 0.08)"
const HOVERED_STROKE = "rgba(255, 255, 255, 0.6)"
const FASTEST_STROKE = "rgba(52, 211, 153, 0.7)"

function interpolateColor(t: number): string {
  const clamped = clamp(t, 0, 1)
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
  summaryRow: CellData[]
}

function buildGridData(rows: QueryComparisonRow[], databases: string[]): GridData {
  let maxRatio = 1

  // Per-database: sum of all median durations (total query time per db)
  const dbTotals = databases.map((db) => {
    let total = 0
    let hasAny = false
    for (const row of rows) {
      const d = row.by_database[db]
      if (d !== null && d !== undefined) {
        total += d
        hasAny = true
      }
    }
    return hasAny ? total : null
  })

  const validTotals = dbTotals.filter((d): d is number => d !== null)
  const fastestTotal = validTotals.length > 0 ? Math.min(...validTotals) : null
  const summaryRow: CellData[] = dbTotals.map((total) => {
    const ratio =
      total !== null && fastestTotal !== null && fastestTotal > 0 ? total / fastestTotal : null
    if (ratio !== null && ratio > maxRatio) maxRatio = ratio
    return { duration: total, ratio }
  })

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

  return { cells, maxRatio, summaryRow }
}

function computeCellWidth(databases: string[]): number {
  const longestName = Math.max(0, ...databases.map((db) => db.length))
  return Math.max(64, longestName * APPROX_CHAR_WIDTH + 20)
}

export function QueryHeatmapGrid({
  rows,
  databases,
  databaseColors,
  selection,
  sqlBySuite,
}: QueryHeatmapGridProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const [queryLabelTooltip, setQueryLabelTooltip] = useState<QueryLabelTooltipState | null>(null)
  const [hoveredCell, setHoveredCell] = useState<{ row: number; col: number } | null>(null)

  const gridData = useMemo(() => buildGridData(rows, databases), [rows, databases])
  const { cells, maxRatio, summaryRow } = gridData
  const cellWidth = useMemo(() => computeCellWidth(databases), [databases])

  const SUMMARY_GAP = 10
  const gridWidth = LABEL_WIDTH + databases.length * (cellWidth + COL_GAP) - COL_GAP
  const queryRowsStartY = HEADER_HEIGHT + CELL_HEIGHT + ROW_GAP + SUMMARY_GAP
  const gridHeight =
    queryRowsStartY + rows.length * (CELL_HEIGHT + ROW_GAP) - ROW_GAP + LEGEND_HEIGHT

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
        tableFamily: row.table_family,
        queryId: row.query_id,
        db,
        duration: cell?.duration ?? null,
        ratio: cell?.ratio ?? null,
        isFastest: cell?.ratio === 1,
      })
      setHoveredCell({ row: rowIdx, col: colIdx })
    },
    [rows, databases, cells],
  )

  const handleSummaryMouseMove = useCallback(
    (event: React.MouseEvent, colIdx: number) => {
      const container = containerRef.current
      if (!container) return
      const rect = container.getBoundingClientRect()
      const db = databases[colIdx]
      if (!db) return
      const cell = summaryRow[colIdx]
      setTooltip({
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
        queryLabel: "Sum of medians",
        queryName: "",
        tableFamily: "",
        queryId: "",
        db,
        duration: cell?.duration ?? null,
        ratio: cell?.ratio ?? null,
        isFastest: cell?.ratio === 1,
      })
      setHoveredCell({ row: -1, col: colIdx })
    },
    [databases, summaryRow],
  )

  const handleMouseLeave = useCallback(() => {
    setTooltip(null)
    setHoveredCell(null)
  }, [])

  const handleClick = useCallback(
    (rowIdx: number) => {
      const row = rows[rowIdx]
      if (row) selection.toggleSelectedQuery(row.query_name)
    },
    [rows, selection],
  )

  const handleQueryLabelEnter = useCallback(
    (event: React.MouseEvent, rowIdx: number) => {
      const container = containerRef.current
      if (!container) return
      const rect = container.getBoundingClientRect()
      const row = rows[rowIdx]
      if (!row) return
      const entry = sqlBySuite?.[row.query_name] ?? null
      const sql = entry?.sql ?? Object.values(entry?.db_overrides ?? {})[0] ?? null
      setQueryLabelTooltip({
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
        queryLabel: row.query_label,
        queryName: row.query_name,
        tableFamily: row.table_family,
        queryId: row.query_id,
        sql,
      })
    },
    [rows, sqlBySuite],
  )

  const handleQueryLabelLeave = useCallback(() => {
    setQueryLabelTooltip(null)
  }, [])

  if (rows.length === 0 || databases.length === 0) {
    return (
      <div className="flex items-center justify-center rounded-lg bg-surface-inset px-4 py-8 text-xs text-slate-500">
        No data available for heatmap.
      </div>
    )
  }

  const summaryY = HEADER_HEIGHT
  const legendY = queryRowsStartY + rows.length * (CELL_HEIGHT + ROW_GAP) + 8
  const hoveredQueryName =
    hoveredCell !== null && hoveredCell.row >= 0
      ? (rows[hoveredCell.row]?.query_name ?? null)
      : null
  const activeQuery = hoveredQueryName ?? selection.selectedQuery
  const isSummaryDimmed = activeQuery !== null

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
            cellWidth={cellWidth}
            hoveredCol={hoveredCell?.col ?? null}
          />

          {/* Summary row — separate section */}
          <g opacity={isSummaryDimmed && hoveredCell?.row !== -1 ? 0.35 : 1}>
            <SummaryRow
              y={summaryY}
              databases={databases}
              cellData={summaryRow}
              cellWidth={cellWidth}
              maxRatio={maxRatio}
              hoveredCol={hoveredCell?.row === -1 ? (hoveredCell?.col ?? null) : null}
              onMouseMove={handleSummaryMouseMove}
              onMouseLeave={handleMouseLeave}
            />
          </g>

          {/* Query rows */}
          {rows.map((row, rowIdx) => {
            const y = queryRowsStartY + rowIdx * (CELL_HEIGHT + ROW_GAP)
            const isSelected = selection.selectedQuery === row.query_name
            const isDimmed = activeQuery !== null && activeQuery !== row.query_name
            const isRowHovered = hoveredCell?.row === rowIdx

            return (
              <HeatmapRow
                key={row.query_name}
                row={row}
                rowIdx={rowIdx}
                y={y}
                databases={databases}
                cellData={cells[rowIdx] ?? []}
                cellWidth={cellWidth}
                maxRatio={maxRatio}
                isSelected={isSelected}
                isDimmed={isDimmed}
                isRowHovered={isRowHovered}
                hoveredCol={hoveredCell?.row === rowIdx ? (hoveredCell?.col ?? null) : null}
                onMouseMove={handleMouseMove}
                onMouseLeave={handleMouseLeave}
                onClick={handleClick}
                onQueryLabelEnter={handleQueryLabelEnter}
                onQueryLabelLeave={handleQueryLabelLeave}
              />
            )
          })}

          <ColorLegend y={legendY} maxRatio={maxRatio} gridWidth={gridWidth} />
        </svg>
      </div>

      {tooltip ? <HeatmapTooltip tooltip={tooltip} containerWidth={gridWidth} /> : null}
      {queryLabelTooltip ? (
        <QueryLabelTooltipPopup tooltip={queryLabelTooltip} containerWidth={gridWidth} />
      ) : null}
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
  cellWidth,
  hoveredCol,
}: {
  databases: string[]
  databaseColors: Record<string, string>
  cellWidth: number
  hoveredCol: number | null
}) {
  return (
    <g>
      {databases.map((db, colIdx) => {
        const x = LABEL_WIDTH + colIdx * (cellWidth + COL_GAP) + cellWidth / 2
        const isHovered = hoveredCol === colIdx
        return (
          <g key={db}>
            <rect
              x={LABEL_WIDTH + colIdx * (cellWidth + COL_GAP)}
              y={4}
              width={cellWidth}
              height={HEADER_HEIGHT - 8}
              rx={CELL_RX}
              fill="transparent"
            />
            <circle
              cx={x - (db.length * APPROX_CHAR_WIDTH * 0.44 + 8)}
              cy={HEADER_HEIGHT / 2}
              r={3.5}
              fill={databaseColors[db] ?? "#94a3b8"}
              opacity={isHovered ? 1 : 0.7}
            />
            <text
              x={x}
              y={HEADER_HEIGHT / 2}
              textAnchor="middle"
              dominantBaseline="central"
              className="text-[11px]"
              fill={isHovered ? "#e2e8f0" : "#94a3b8"}
              fontWeight={isHovered ? 600 : 500}
            >
              {db}
            </text>
          </g>
        )
      })}
    </g>
  )
}

interface SummaryRowProps {
  y: number
  databases: string[]
  cellData: CellData[]
  cellWidth: number
  maxRatio: number
  hoveredCol: number | null
  onMouseMove: (event: React.MouseEvent, colIdx: number) => void
  onMouseLeave: () => void
}

function SummaryRow({
  y,
  databases,
  cellData,
  cellWidth,
  maxRatio,
  hoveredCol,
  onMouseMove,
  onMouseLeave,
}: SummaryRowProps) {
  return (
    <g>
      <text
        x={LABEL_WIDTH - 10}
        y={y + CELL_HEIGHT / 2}
        textAnchor="end"
        dominantBaseline="central"
        className="text-[11px]"
        fill="#94a3b8"
        fontWeight={600}
      >
        Σ Median
      </text>

      {databases.map((_, colIdx) => {
        const cell = cellData[colIdx]
        if (!cell) return null
        const x = LABEL_WIDTH + colIdx * (cellWidth + COL_GAP)
        const isCellHovered = hoveredCol === colIdx
        const isFastest = cell.ratio === 1 && cell.duration !== null
        const fill = ratioColor(cell.ratio, maxRatio)

        const stroke = isCellHovered
          ? HOVERED_STROKE
          : isFastest
            ? FASTEST_STROKE
            : "rgba(148, 163, 184, 0.15)"
        const strokeWidth = isCellHovered ? 1.5 : isFastest ? 1.5 : 0.5

        return (
          <g
            key={colIdx}
            onMouseMove={(e) => onMouseMove(e, colIdx)}
            onMouseLeave={onMouseLeave}
            className="cursor-default"
          >
            <rect
              x={x - COL_GAP / 2}
              y={y - ROW_GAP / 2}
              width={cellWidth + COL_GAP}
              height={CELL_HEIGHT + ROW_GAP}
              fill="transparent"
            />
            <rect
              x={x}
              y={y}
              width={cellWidth}
              height={CELL_HEIGHT}
              rx={CELL_RX}
              fill={fill}
              stroke={stroke}
              strokeWidth={strokeWidth}
            />
            {cell.duration !== null ? (
              <text
                x={x + cellWidth / 2}
                y={y + CELL_HEIGHT / 2}
                textAnchor="middle"
                dominantBaseline="central"
                className="pointer-events-none text-[10px] font-semibold"
                fill={cell.ratio !== null && cell.ratio <= 1.1 ? "#d1fae5" : "#e2e8f0"}
              >
                {formatCellDuration(cell.duration)}
              </text>
            ) : (
              <text
                x={x + cellWidth / 2}
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
}

interface HeatmapRowProps {
  row: QueryComparisonRow
  rowIdx: number
  y: number
  databases: string[]
  cellData: CellData[]
  cellWidth: number
  maxRatio: number
  isSelected: boolean
  isDimmed: boolean
  isRowHovered: boolean
  hoveredCol: number | null
  onMouseMove: (event: React.MouseEvent, rowIdx: number, colIdx: number) => void
  onMouseLeave: () => void
  onClick: (rowIdx: number) => void
  onQueryLabelEnter: (event: React.MouseEvent, rowIdx: number) => void
  onQueryLabelLeave: () => void
}

const HeatmapRow = memo(function HeatmapRow({
  row,
  rowIdx,
  y,
  databases,
  cellData,
  cellWidth,
  maxRatio,
  isSelected,
  isDimmed,
  isRowHovered,
  hoveredCol,
  onMouseMove,
  onMouseLeave,
  onClick,
  onQueryLabelEnter,
  onQueryLabelLeave,
}: HeatmapRowProps) {
  const rowOpacity = isDimmed && !isRowHovered ? 0.35 : 1

  return (
    <g opacity={rowOpacity}>
      <text
        x={LABEL_WIDTH - 10}
        y={y + CELL_HEIGHT / 2}
        textAnchor="end"
        dominantBaseline="central"
        className="cursor-pointer text-[11px]"
        fill={isSelected ? "#e2e8f0" : isRowHovered ? "#cbd5e1" : "#94a3b8"}
        fontWeight={isSelected ? 600 : 400}
        onClick={() => onClick(rowIdx)}
        onMouseEnter={(e) => onQueryLabelEnter(e, rowIdx)}
        onMouseLeave={onQueryLabelLeave}
      >
        {clipLabel(row.query_label, 24)}
      </text>

      {databases.map((_, colIdx) => {
        const cell = cellData[colIdx]
        if (!cell) return null
        const x = LABEL_WIDTH + colIdx * (cellWidth + COL_GAP)
        const isCellHovered = isRowHovered && hoveredCol === colIdx
        const isFastest = cell.ratio === 1 && cell.duration !== null
        const fill = ratioColor(cell.ratio, maxRatio)

        const stroke = isCellHovered
          ? HOVERED_STROKE
          : isSelected
            ? "rgba(108, 142, 239, 0.5)"
            : isFastest
              ? FASTEST_STROKE
              : "none"
        const strokeWidth = isCellHovered ? 1.5 : isSelected ? 1 : isFastest ? 1.5 : 0

        return (
          <g
            key={colIdx}
            onMouseMove={(e) => onMouseMove(e, rowIdx, colIdx)}
            onMouseLeave={onMouseLeave}
            onClick={() => onClick(rowIdx)}
            className="cursor-pointer"
          >
            <rect
              x={x - COL_GAP / 2}
              y={y - ROW_GAP / 2}
              width={cellWidth + COL_GAP}
              height={CELL_HEIGHT + ROW_GAP}
              fill="transparent"
            />
            <rect
              x={x}
              y={y}
              width={cellWidth}
              height={CELL_HEIGHT}
              rx={CELL_RX}
              fill={fill}
              stroke={stroke}
              strokeWidth={strokeWidth}
            />
            {cell.duration !== null ? (
              <text
                x={x + cellWidth / 2}
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
                x={x + cellWidth / 2}
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
      {tooltip.queryId ? (
        <div className="mb-1 text-slate-500">
          {tooltip.tableFamily} · Q{tooltip.queryId}
        </div>
      ) : null}
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

function QueryLabelTooltipPopup({
  tooltip,
  containerWidth,
}: {
  tooltip: QueryLabelTooltipState
  containerWidth: number
}) {
  const popupWidth = tooltip.sql ? 420 : 260
  const tokens = useMemo(
    () => (tooltip.sql ? highlightSqlTokens(tooltip.sql.trim()) : null),
    [tooltip.sql],
  )

  return (
    <div
      className="pointer-events-none absolute z-50 rounded-lg border border-border-default bg-[#161a23] px-3 py-2 text-xs text-slate-200 shadow-xl"
      style={{
        left: Math.min(tooltip.x + 12, containerWidth - popupWidth - 8),
        top: Math.max(0, tooltip.y - 8),
        maxWidth: popupWidth,
      }}
    >
      <div className="font-medium">{tooltip.queryLabel}</div>
      <div className="mt-0.5 text-slate-500">
        {tooltip.tableFamily} · Q{tooltip.queryId}
      </div>
      <div className="mt-0.5 font-mono text-[10px] text-slate-500">{tooltip.queryName}</div>
      {tokens ? (
        <pre className="mt-2 [display:-webkit-box] overflow-hidden font-mono text-[10px] leading-snug whitespace-pre-wrap text-slate-300 [-webkit-box-orient:vertical] [-webkit-line-clamp:10]">
          {tokens.map((token, idx) => (
            <span key={idx} className={token.className || undefined}>
              {token.text}
            </span>
          ))}
        </pre>
      ) : null}
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
