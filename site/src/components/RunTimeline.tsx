import { useCallback, useLayoutEffect, useMemo, useRef, useState } from "react"

import { formatDurationSeconds } from "../lib/format"
import type { QueryStep } from "../lib/types"
import { ControlChip, QuietButton } from "./controls/Control"
import { DatabaseLegend } from "./DatabaseLegend"
import { PanelCard, PanelHeader } from "./layout/Panel"
import { Skeleton } from "./Skeleton"
import { BodyText, SectionTitle } from "./Typography"

interface RunTimelineProps {
  querySteps: QueryStep[]
  databases: string[]
  databaseColors: Record<string, string>
  onSelectQuery?: (queryName: string) => void
  selectedQuery?: string | null
  title?: string
  description?: string
  loading?: boolean
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
  ["rgba(96, 165, 250, 0.58)", "rgba(147, 197, 253, 0.9)"],
  ["rgba(251, 146, 60, 0.58)", "rgba(253, 186, 116, 0.9)"],
  ["rgba(52, 211, 153, 0.58)", "rgba(110, 231, 183, 0.9)"],
  ["rgba(250, 204, 21, 0.56)", "rgba(253, 224, 71, 0.9)"],
  ["rgba(244, 114, 182, 0.56)", "rgba(251, 182, 206, 0.9)"],
  ["rgba(168, 85, 247, 0.62)", "rgba(216, 180, 254, 0.92)"],
  ["rgba(251, 113, 133, 0.56)", "rgba(253, 164, 175, 0.9)"],
  ["rgba(45, 212, 191, 0.58)", "rgba(153, 246, 228, 0.9)"],
  ["rgba(129, 140, 248, 0.6)", "rgba(199, 210, 254, 0.92)"],
  ["rgba(245, 158, 11, 0.58)", "rgba(252, 211, 77, 0.9)"],
  ["rgba(74, 222, 128, 0.58)", "rgba(134, 239, 172, 0.9)"],
  ["rgba(192, 132, 252, 0.6)", "rgba(233, 213, 255, 0.92)"],
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
  title = "Run timeline",
  description = "Full query execution timeline per database. Each segment represents one query iteration. Click to inspect.",
  loading = false,
}: RunTimelineProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const [legendExpanded, setLegendExpanded] = useState(false)
  const [svgWidth, setSvgWidth] = useState(0)

  useLayoutEffect(() => {
    if (loading) {
      setSvgWidth(0)
      return
    }
    const svg = svgRef.current
    if (!svg) return
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setSvgWidth(entry.contentRect.width)
      }
    })
    observer.observe(svg)
    setSvgWidth(svg.getBoundingClientRect().width)
    return () => observer.disconnect()
  }, [loading])

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

  if (loading) {
    return (
      <PanelCard>
        <PanelHeader>
          <div>
            <SectionTitle as="h3">{title}</SectionTitle>
            <BodyText className="mt-1">{description}</BodyText>
          </div>
          <div className="flex gap-2">
            <Skeleton className="h-6 w-16 rounded-full" />
            <Skeleton className="h-6 w-20 rounded-full" />
            <Skeleton className="h-6 w-20 rounded-full" />
          </div>
        </PanelHeader>

        <div className="mt-3 flex flex-wrap items-center gap-1.5">
          <Skeleton className="h-6 w-18 rounded-full" />
          <Skeleton className="h-6 w-20 rounded-full" />
          <Skeleton className="h-6 w-16 rounded-full" />
          <Skeleton className="h-6 w-24 rounded-full" />
          <Skeleton className="h-6 w-20 rounded-full" />
        </div>

        <div className="relative mt-4 rounded-xl bg-surface-inset p-4">
          <div className="grid gap-3">
            <div className="grid grid-cols-[6rem_minmax(0,1fr)] items-center gap-4">
              <Skeleton className="h-4 w-14" />
              <Skeleton className="h-8 w-full rounded-xl" />
            </div>
            <div className="grid grid-cols-[6rem_minmax(0,1fr)] items-center gap-4">
              <Skeleton className="h-4 w-16" />
              <Skeleton className="h-8 w-full rounded-xl" />
            </div>
            <div className="grid grid-cols-[6rem_minmax(0,1fr)] items-center gap-4">
              <Skeleton className="h-4 w-12" />
              <Skeleton className="h-8 w-full rounded-xl" />
            </div>
            <div className="grid grid-cols-[6rem_minmax(0,1fr)] items-center gap-4">
              <Skeleton className="h-4 w-18" />
              <Skeleton className="h-8 w-full rounded-xl" />
            </div>
          </div>
          <div className="mt-4 flex justify-between">
            <Skeleton className="h-3 w-8" />
            <Skeleton className="h-3 w-8" />
            <Skeleton className="h-3 w-8" />
            <Skeleton className="h-3 w-8" />
            <Skeleton className="h-3 w-8" />
          </div>
        </div>
      </PanelCard>
    )
  }

  if (filteredSteps.length === 0) return null

  const chartHeight =
    PADDING_TOP + databases.length * (ROW_HEIGHT + ROW_GAP) - ROW_GAP + PADDING_BOTTOM

  return (
    <PanelCard>
      <PanelHeader>
        <div>
          <SectionTitle as="h3">{title}</SectionTitle>
          <BodyText className="mt-1">{description}</BodyText>
        </div>
        <DatabaseLegend databases={databases} databaseColors={databaseColors} />
      </PanelHeader>

      <QueryLegend
        queryNames={queryNames}
        queryColorMap={queryColorMap}
        selectedQuery={selectedQuery ?? null}
        expanded={legendExpanded}
        onToggleExpanded={() => setLegendExpanded((v) => !v)}
        onSelectQuery={onSelectQuery}
      />

      <div
        ref={containerRef}
        className="relative mt-4 rounded-xl bg-surface-inset p-4"
        style={{ height: chartHeight + 16 }}
      >
        <svg ref={svgRef} width="100%" height={chartHeight} className="overflow-visible">
          {svgWidth > 0 ? (
            <SvgContent
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
              svgWidth={svgWidth}
            />
          ) : null}
        </svg>

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

const LEGEND_COLLAPSED_COUNT = 14

function QueryLegend({
  queryNames,
  queryColorMap,
  selectedQuery,
  expanded,
  onToggleExpanded,
  onSelectQuery,
}: {
  queryNames: string[]
  queryColorMap: Record<string, [string, string]>
  selectedQuery: string | null
  expanded: boolean
  onToggleExpanded: () => void
  onSelectQuery?: (queryName: string) => void
}) {
  const visibleNames = expanded ? queryNames : queryNames.slice(0, LEGEND_COLLAPSED_COUNT)
  const hiddenCount = queryNames.length - LEGEND_COLLAPSED_COUNT

  return (
    <div className="mt-3 flex flex-wrap items-center gap-1.5">
      {visibleNames.map((name) => {
        const short = shortenQueryName(name)
        const colors = queryColorMap[name]!
        const isSelected = selectedQuery === name
        return (
          <ControlChip
            key={name}
            onClick={() => onSelectQuery?.(name)}
            className="gap-1.5"
            selected={isSelected}
            size="xs"
            style={{
              boxShadow: isSelected ? `inset 0 0 0 1px ${colors[1]}` : undefined,
            }}
          >
            <span
              className="inline-block size-2 rounded-sm"
              style={{ backgroundColor: colors[isSelected ? 1 : 0] }}
            />
            <span className="text-slate-400">{short}</span>
          </ControlChip>
        )
      })}
      {queryNames.length > LEGEND_COLLAPSED_COUNT ? (
        <QuietButton size="xs" onClick={onToggleExpanded}>
          {expanded ? "Show less" : `+${hiddenCount} more`}
        </QuietButton>
      ) : null}
    </div>
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
  svgWidth,
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
  svgWidth: number
}) {
  const chartLeft = LABEL_WIDTH
  const chartWidth = svgWidth - chartLeft - PADDING_RIGHT
  const ticks = buildElapsedTicks(maxElapsed)
  const hasSelection = selectedQuery !== null

  const toX = (seconds: number) => chartLeft + chartWidth * (seconds / maxElapsed)
  const toW = (seconds: number) => chartWidth * (seconds / maxElapsed)

  return (
    <g>
      {ticks.map((tick, tickIdx) => {
        const isFirst = tickIdx === 0
        const isLast = tickIdx === ticks.length - 1
        const anchor = isFirst ? "start" : isLast ? "end" : "middle"
        const x = toX(tick)

        return (
          <g key={tick}>
            <line
              x1={x}
              y1={0}
              x2={x}
              y2={height - PADDING_BOTTOM}
              stroke="rgba(148, 163, 184, 0.06)"
            />
            <text
              x={x}
              y={height - PADDING_BOTTOM + 16}
              fill="#64748b"
              fontSize={11}
              textAnchor={anchor}
            >
              {formatElapsed(tick)}
            </text>
          </g>
        )
      })}

      {databases.map((db, dbIndex) => {
        const y = PADDING_TOP + dbIndex * (ROW_HEIGHT + ROW_GAP)
        const segments = segmentsByDb.get(db) ?? []
        const orderedSegments = hasSelection
          ? [...segments].sort(
              (a, b) =>
                Number(a.query_name === selectedQuery) - Number(b.query_name === selectedQuery),
            )
          : segments

        return (
          <g key={db}>
            <line
              x1={chartLeft}
              y1={y + ROW_HEIGHT}
              x2={svgWidth}
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

            {orderedSegments.map((seg, segIdx) => {
              const colors = queryColorMap[seg.query_name]
              const isSelected = selectedQuery === seg.query_name
              const isDimmed = hasSelection && !isSelected
              const fill = colors
                ? isSelected
                  ? colors[1]
                  : colors[0]
                : "rgba(148, 163, 184, 0.4)"

              const x = toX(seg.start_s)
              const w = Math.max(MIN_SEGMENT_WIDTH, toW(seg.end_s - seg.start_s))

              return (
                <rect
                  key={segIdx}
                  x={x}
                  y={y + 2}
                  width={w}
                  height={ROW_HEIGHT - 4}
                  rx={3}
                  fill={fill}
                  opacity={isDimmed ? 0.22 : 1}
                  stroke={isSelected ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0)"}
                  strokeWidth={isSelected ? 1.5 : 0}
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

function buildTimelineData(steps: QueryStep[], databases: string[]) {
  const rawByDb = new Map<string, TimelineSegment[]>()
  const queryNameSet = new Set<string>()

  for (const step of steps) {
    queryNameSet.add(step.query_name)

    const dbSegments = rawByDb.get(step.db) ?? []
    dbSegments.push({
      db: step.db,
      query_name: step.query_name,
      iteration: step.iteration,
      start_s: step.elapsed_start_s,
      end_s: step.elapsed_end_s,
      duration_s: step.duration_s,
    })
    rawByDb.set(step.db, dbSegments)
  }

  const segmentsByDb = new Map<string, TimelineSegment[]>()
  let maxElapsed = 0

  for (const [db, segs] of rawByDb) {
    segs.sort((a, b) => a.start_s - b.start_s)
    let cursor = 0
    const packed = segs.map((seg) => {
      const packedSeg = { ...seg, start_s: cursor, end_s: cursor + seg.duration_s }
      cursor = packedSeg.end_s
      return packedSeg
    })
    if (cursor > maxElapsed) maxElapsed = cursor
    segmentsByDb.set(db, packed)
  }

  for (const db of databases) {
    if (!segmentsByDb.has(db)) segmentsByDb.set(db, [])
  }

  const queryNames = Array.from(queryNameSet).sort()
  maxElapsed = Math.max(1, Math.ceil(maxElapsed))

  return { segmentsByDb, maxElapsed, queryNames }
}

const TIME_STEPS = [
  1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 43200, 86400,
]

function buildElapsedTicks(maxSeconds: number): number[] {
  const safeMax = Math.max(1, Math.ceil(maxSeconds))
  const target = safeMax / 5
  const step = TIME_STEPS.find((s) => s >= target) ?? TIME_STEPS[TIME_STEPS.length - 1]!
  const ticks: number[] = []
  for (let t = 0; t <= safeMax; t += step) ticks.push(t)
  return ticks
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
  if (h > 0) return s > 0 ? `${h}h ${m}m ${s}s` : `${h}h ${m}m`
  if (m > 0) return s > 0 ? `${m}m ${s}s` : `${m}m`
  return `${s}s`
}
