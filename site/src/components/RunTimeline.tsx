import { useCallback, useMemo, useRef, useState } from "react"

import { formatDurationSeconds } from "../lib/format"
import type { TimeSeriesQueryStep } from "../lib/types"
import { DatabaseLegend } from "./DatabaseLegend"
import { PanelCard, PanelHeader } from "./layout/Panel"
import { BodyText, SectionTitle } from "./Typography"

interface RunTimelineProps {
  querySteps: TimeSeriesQueryStep[]
  databases: string[]
  databaseColors: Record<string, string>
  onSelectQuery?: (queryName: string) => void
  selectedQuery?: string | null
}

interface TimelineSegment {
  db: string
  query_name: string
  iteration: number
  start_s: number
  end_s: number
  duration_s: number
}

interface TooltipState {
  x: number
  y: number
  segment: TimelineSegment
}

const QUERY_COLORS = [
  ["rgba(56, 189, 248, 0.5)", "rgba(56, 189, 248, 0.85)"],
  ["rgba(249, 115, 22, 0.5)", "rgba(249, 115, 22, 0.85)"],
  ["rgba(52, 211, 153, 0.5)", "rgba(52, 211, 153, 0.85)"],
  ["rgba(250, 204, 21, 0.5)", "rgba(250, 204, 21, 0.85)"],
  ["rgba(244, 114, 182, 0.5)", "rgba(244, 114, 182, 0.85)"],
  ["rgba(167, 139, 250, 0.5)", "rgba(167, 139, 250, 0.85)"],
  ["rgba(251, 113, 133, 0.5)", "rgba(251, 113, 133, 0.85)"],
  ["rgba(45, 212, 191, 0.5)", "rgba(45, 212, 191, 0.85)"],
  ["rgba(96, 165, 250, 0.5)", "rgba(96, 165, 250, 0.85)"],
  ["rgba(245, 158, 11, 0.5)", "rgba(245, 158, 11, 0.85)"],
  ["rgba(74, 222, 128, 0.5)", "rgba(74, 222, 128, 0.85)"],
  ["rgba(192, 132, 252, 0.5)", "rgba(192, 132, 252, 0.85)"],
] as const

const ROW_HEIGHT = 32
const ROW_GAP = 8
const LABEL_WIDTH = 100
const PADDING_TOP = 8
const PADDING_BOTTOM = 28
const PADDING_RIGHT = 16
const MIN_SEGMENT_WIDTH = 2

export function RunTimeline({
  querySteps,
  databases,
  databaseColors,
  onSelectQuery,
  selectedQuery,
}: RunTimelineProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)

  const filteredSteps = useMemo(
    () => querySteps.filter((s) => databases.includes(s.db)),
    [querySteps, databases],
  )

  const { segmentsByDb, maxElapsed, queryNames } = useMemo(
    () => buildTimelineData(filteredSteps, databases),
    [filteredSteps, databases],
  )

  const queryColorMap = useMemo(() => {
    const map: Record<string, [string, string]> = {}
    for (const [i, name] of queryNames.entries()) {
      map[name] = QUERY_COLORS[i % QUERY_COLORS.length]! as [string, string]
    }
    return map
  }, [queryNames])

  const handleSegmentClick = useCallback(
    (queryName: string) => {
      onSelectQuery?.(queryName)
    },
    [onSelectQuery],
  )

  const handleSegmentEnter = useCallback((segment: TimelineSegment, event: React.MouseEvent) => {
    const container = containerRef.current
    if (!container) return
    const rect = container.getBoundingClientRect()
    setTooltip({
      x: event.clientX - rect.left,
      y: event.clientY - rect.top - 8,
      segment,
    })
  }, [])

  const handleSegmentLeave = useCallback(() => {
    setTooltip(null)
  }, [])

  if (filteredSteps.length === 0) return null

  const chartHeight =
    PADDING_TOP + databases.length * (ROW_HEIGHT + ROW_GAP) - ROW_GAP + PADDING_BOTTOM

  return (
    <PanelCard>
      <PanelHeader>
        <div>
          <SectionTitle as="h3">Run timeline</SectionTitle>
          <BodyText className="mt-1">
            Full query execution timeline per database. Each segment represents one query iteration.
            Click to inspect.
          </BodyText>
        </div>
        <DatabaseLegend databases={databases} databaseColors={databaseColors} />
      </PanelHeader>

      <div className="mt-3 flex flex-wrap gap-1.5">
        {queryNames.map((name) => {
          const short = shortenQueryName(name)
          const colors = queryColorMap[name]!
          const isSelected = selectedQuery === name
          return (
            <button
              key={name}
              type="button"
              onClick={() => onSelectQuery?.(name)}
              className="flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] transition-colors hover:border-slate-600"
              style={{
                backgroundColor: isSelected ? "rgba(108, 142, 239, 0.15)" : "transparent",
                borderColor: isSelected ? "rgba(108, 142, 239, 0.4)" : "rgba(148, 163, 184, 0.15)",
              }}
            >
              <span
                className="inline-block size-2 rounded-sm"
                style={{ backgroundColor: colors[isSelected ? 1 : 0] }}
              />
              <span className="text-slate-400">{short}</span>
            </button>
          )
        })}
      </div>

      <div
        ref={containerRef}
        className="relative mt-4 rounded-xl bg-surface-inset p-4"
        style={{ height: chartHeight + 16 }}
      >
        <TimelineSvg
          databases={databases}
          databaseColors={databaseColors}
          segmentsByDb={segmentsByDb}
          maxElapsed={maxElapsed}
          queryColorMap={queryColorMap}
          selectedQuery={selectedQuery ?? null}
          onSegmentClick={handleSegmentClick}
          onSegmentEnter={handleSegmentEnter}
          onSegmentLeave={handleSegmentLeave}
          height={chartHeight}
        />

        {tooltip ? (
          <div
            className="pointer-events-none absolute z-10 rounded-lg border border-border-default bg-[#161a23] px-3 py-2 text-xs text-slate-200 shadow-lg"
            style={{
              left: tooltip.x,
              top: tooltip.y,
              transform: "translate(-50%, -100%)",
            }}
          >
            <p className="font-medium">{formatQueryLabel(tooltip.segment.query_name)}</p>
            <p className="mt-0.5 text-slate-400">
              Iteration {tooltip.segment.iteration} ·{" "}
              {formatDurationSeconds(tooltip.segment.duration_s)}
            </p>
          </div>
        ) : null}
      </div>
    </PanelCard>
  )
}

function TimelineSvg({
  databases,
  databaseColors,
  segmentsByDb,
  maxElapsed,
  queryColorMap,
  selectedQuery,
  onSegmentClick,
  onSegmentEnter,
  onSegmentLeave,
  height,
}: {
  databases: string[]
  databaseColors: Record<string, string>
  segmentsByDb: Map<string, TimelineSegment[]>
  maxElapsed: number
  queryColorMap: Record<string, [string, string]>
  selectedQuery: string | null
  onSegmentClick: (queryName: string) => void
  onSegmentEnter: (segment: TimelineSegment, event: React.MouseEvent) => void
  onSegmentLeave: () => void
  height: number
}) {
  return (
    <svg width="100%" height={height} className="overflow-visible">
      <SvgContent
        databases={databases}
        databaseColors={databaseColors}
        segmentsByDb={segmentsByDb}
        maxElapsed={maxElapsed}
        queryColorMap={queryColorMap}
        selectedQuery={selectedQuery}
        onSegmentClick={onSegmentClick}
        onSegmentEnter={onSegmentEnter}
        onSegmentLeave={onSegmentLeave}
        height={height}
      />
    </svg>
  )
}

function SvgContent({
  databases,
  databaseColors,
  segmentsByDb,
  maxElapsed,
  queryColorMap,
  selectedQuery,
  onSegmentClick,
  onSegmentEnter,
  onSegmentLeave,
  height,
}: {
  databases: string[]
  databaseColors: Record<string, string>
  segmentsByDb: Map<string, TimelineSegment[]>
  maxElapsed: number
  queryColorMap: Record<string, [string, string]>
  selectedQuery: string | null
  onSegmentClick: (queryName: string) => void
  onSegmentEnter: (segment: TimelineSegment, event: React.MouseEvent) => void
  onSegmentLeave: () => void
  height: number
}) {
  const chartLeft = LABEL_WIDTH
  const chartRight = PADDING_RIGHT
  const ticks = buildElapsedTicks(maxElapsed)

  return (
    <g>
      {ticks.map((tick) => (
        <g key={tick}>
          <line
            x1={`calc(${chartLeft}px + (100% - ${chartLeft + chartRight}px) * ${tick / maxElapsed})`}
            y1={0}
            x2={`calc(${chartLeft}px + (100% - ${chartLeft + chartRight}px) * ${tick / maxElapsed})`}
            y2={height - PADDING_BOTTOM}
            stroke="rgba(148, 163, 184, 0.06)"
          />
          <text
            x={`calc(${chartLeft}px + (100% - ${chartLeft + chartRight}px) * ${tick / maxElapsed})`}
            y={height - PADDING_BOTTOM + 16}
            fill="#64748b"
            fontSize={11}
            textAnchor="middle"
          >
            {formatElapsed(tick)}
          </text>
        </g>
      ))}

      {databases.map((db, dbIndex) => {
        const y = PADDING_TOP + dbIndex * (ROW_HEIGHT + ROW_GAP)
        const segments = segmentsByDb.get(db) ?? []

        return (
          <g key={db}>
            <line
              x1={chartLeft}
              y1={y + ROW_HEIGHT}
              x2="100%"
              y2={y + ROW_HEIGHT}
              stroke="rgba(148, 163, 184, 0.06)"
            />

            <circle
              cx={chartLeft - 14}
              cy={y + ROW_HEIGHT / 2}
              r={4}
              fill={databaseColors[db] ?? "#94a3b8"}
            />
            <text
              x={chartLeft - 22}
              y={y + ROW_HEIGHT / 2}
              fill="#94a3b8"
              fontSize={11}
              textAnchor="end"
              dominantBaseline="middle"
            >
              {db}
            </text>

            {segments.map((seg, segIdx) => {
              const colors = queryColorMap[seg.query_name]
              const isSelected = selectedQuery === seg.query_name
              const fill = colors
                ? isSelected
                  ? colors[1]
                  : colors[0]
                : "rgba(148, 163, 184, 0.4)"

              const xPct = seg.start_s / maxElapsed
              const wPct = (seg.end_s - seg.start_s) / maxElapsed

              return (
                <rect
                  key={segIdx}
                  x={`calc(${chartLeft}px + (100% - ${chartLeft + chartRight}px) * ${xPct})`}
                  y={y + 2}
                  width={`max(${MIN_SEGMENT_WIDTH}px, calc((100% - ${chartLeft + chartRight}px) * ${wPct}))`}
                  height={ROW_HEIGHT - 4}
                  rx={3}
                  fill={fill}
                  className="cursor-pointer transition-opacity hover:opacity-80"
                  onClick={() => onSegmentClick(seg.query_name)}
                  onMouseEnter={(e) => onSegmentEnter(seg, e)}
                  onMouseLeave={onSegmentLeave}
                />
              )
            })}
          </g>
        )
      })}
    </g>
  )
}

function buildTimelineData(steps: TimeSeriesQueryStep[], databases: string[]) {
  const segmentsByDb = new Map<string, TimelineSegment[]>()
  const queryNameSet = new Set<string>()
  let maxElapsed = 0

  for (const step of steps) {
    queryNameSet.add(step.query_name)
    if (step.elapsed_end_s > maxElapsed) maxElapsed = step.elapsed_end_s

    const dbSegments = segmentsByDb.get(step.db) ?? []
    dbSegments.push({
      db: step.db,
      query_name: step.query_name,
      iteration: step.iteration,
      start_s: step.elapsed_start_s,
      end_s: step.elapsed_end_s,
      duration_s: step.duration_s,
    })
    segmentsByDb.set(step.db, dbSegments)
  }

  for (const [db, segs] of segmentsByDb) {
    segmentsByDb.set(
      db,
      segs.sort((a, b) => a.start_s - b.start_s),
    )
  }

  for (const db of databases) {
    if (!segmentsByDb.has(db)) segmentsByDb.set(db, [])
  }

  const queryNames = Array.from(queryNameSet).sort()
  maxElapsed = Math.max(1, Math.ceil(maxElapsed))

  return { segmentsByDb, maxElapsed, queryNames }
}

function buildElapsedTicks(maxSeconds: number): number[] {
  const safeMax = Math.max(1, Math.ceil(maxSeconds))
  const roughStep = safeMax / 5
  const step = getNiceStep(roughStep)
  const ticks: number[] = []
  for (let t = 0; t <= safeMax; t += step) ticks.push(t)
  if (ticks[ticks.length - 1] !== safeMax) ticks.push(safeMax)
  return ticks
}

function getNiceStep(value: number): number {
  const exponent = Math.floor(Math.log10(Math.max(value, 1)))
  const magnitude = 10 ** exponent
  const normalized = value / magnitude
  if (normalized <= 1) return magnitude
  if (normalized <= 2) return 2 * magnitude
  if (normalized <= 5) return 5 * magnitude
  return 10 * magnitude
}

function shortenQueryName(name: string): string {
  const match = /^([a-z]+)_(\d+)/.exec(name)
  if (!match) return name
  return `${match[1]}_${match[2]}`
}

function formatQueryLabel(name: string): string {
  return name.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}

function formatElapsed(value: number): string {
  const total = Math.max(0, Math.round(value))
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  const s = total % 60
  if (h > 0) return `${h}h ${m}m`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}
