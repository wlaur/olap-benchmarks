import { ChevronLeft, ChevronRight, Database, LoaderCircle } from "lucide-react"
import { useCallback, useEffect, useMemo, useState } from "react"

import type { BenchmarkSuiteId } from "../../lib/benchmarks"
import { formatDurationSeconds } from "../../lib/format"
import { formatCpuPercent, formatMegabytes } from "../../lib/metricFormat"
import { fetchFlameSpans } from "../../lib/queries"
import type { FlameSpan, MetricSample } from "../../lib/types"
import { QuietButton } from "../controls/Control"
import { PanelCard } from "../layout/Panel"
import { SqlCodeView } from "../SqlCodeView"
import { MetaLabel, SectionTitle } from "../Typography"
import { FlameGraph } from "./FlameGraph"

interface FlameGraphPanelProps {
  system: string | null
  suite: BenchmarkSuiteId
  databases: string[]
  metricSamples: MetricSample[]
  isLoading?: boolean
}

interface SpanMetrics {
  avgCpu: number
  avgMem: number
  avgDisk: number
  sampleCount: number
}

export function FlameGraphPanel({
  system,
  suite,
  databases,
  metricSamples,
  isLoading = false,
}: FlameGraphPanelProps) {
  const [selectedDb, setSelectedDb] = useState<string | null>(null)
  const [spans, setSpans] = useState<FlameSpan[]>([])
  const [selectedSpanId, setSelectedSpanId] = useState<string | null>(null)
  const [selectedSpanPath, setSelectedSpanPath] = useState<string[]>([])
  const [zoomPath, setZoomPath] = useState<string[]>([])
  const [isLoadingSpans, setIsLoadingSpans] = useState(false)

  const resolvedDb =
    selectedDb && databases.includes(selectedDb) ? selectedDb : (databases[0] ?? null)

  useEffect(() => {
    if (isLoading || system === null) {
      setIsLoadingSpans(false)
      setSpans([])
      return
    }
    if (!resolvedDb) {
      setSpans([])
      return
    }

    let cancelled = false
    setIsLoadingSpans(true)

    fetchFlameSpans(system, suite, resolvedDb)
      .then((result) => {
        if (!cancelled) setSpans(result)
      })
      .catch(() => {
        if (!cancelled) setSpans([])
      })
      .finally(() => {
        if (!cancelled) setIsLoadingSpans(false)
      })

    return () => {
      cancelled = true
    }
  }, [isLoading, system, suite, resolvedDb])

  const selectedSpan = useMemo(() => {
    if (selectedSpanId) {
      const matchedById = spans.find((span) => span.id === selectedSpanId)
      if (matchedById) return matchedById
    }

    for (let index = selectedSpanPath.length - 1; index >= 0; index -= 1) {
      const spanKey = selectedSpanPath[index]
      const matchedSpan = spans.find((span) => getFlameSpanSelectionKey(span) === spanKey)
      if (matchedSpan) return matchedSpan
    }

    return null
  }, [selectedSpanId, selectedSpanPath, spans])

  const spanMetrics = useMemo((): SpanMetrics | null => {
    if (!selectedSpan || !resolvedDb) return null

    const opSamples = metricSamples.filter(
      (s) => s.db === resolvedDb && s.operation === selectedSpan.operation,
    )
    if (opSamples.length === 0) return null

    const opSpan = spans.find(
      (s) => s.depth === "operation" && s.operation === selectedSpan.operation,
    )
    if (!opSpan) return null

    const opStartOffset = opSpan.elapsed_start_s
    const spanLocalStart = selectedSpan.elapsed_start_s - opStartOffset
    const spanLocalEnd = selectedSpan.elapsed_end_s - opStartOffset

    const matching = opSamples.filter(
      (s) => s.elapsed_s >= spanLocalStart - 1 && s.elapsed_s <= spanLocalEnd + 1,
    )

    if (matching.length === 0) return null

    let totalCpu = 0
    let totalMem = 0
    let totalDisk = 0
    for (const s of matching) {
      totalCpu += s.cpu_percent
      totalMem += s.mem_mb
      totalDisk += s.disk_mb
    }
    const n = matching.length
    return { avgCpu: totalCpu / n, avgMem: totalMem / n, avgDisk: totalDisk / n, sampleCount: n }
  }, [selectedSpan, resolvedDb, metricSamples, spans])

  const handleSelectSpan = useCallback((span: FlameSpan | null) => {
    setSelectedSpanId(span?.id ?? null)
    setSelectedSpanPath(span ? getFlameSpanSelectionPath(span) : [])
  }, [])
  const showLoadingOverlay = isLoading || isLoadingSpans
  const hasSpans = spans.length > 0

  return (
    <PanelCard className="flex h-[clamp(28rem,70vh,42rem)] min-h-0 min-w-0 flex-col p-3">
      <div className="flex shrink-0 items-start justify-between gap-3">
        <SectionTitle as="h3">Flame graph</SectionTitle>
        {databases.length > 0 && !isLoading ? (
          <DatabaseSelector databases={databases} selected={resolvedDb} onChange={setSelectedDb} />
        ) : null}
      </div>

      <div className="relative mt-2 flex min-h-0 flex-1 flex-col rounded-lg border border-border-default bg-surface-inset p-3">
        <div className="flex min-h-0 flex-1 flex-col gap-3">
          <div className="shrink-0 overflow-x-auto overflow-y-hidden">
            {hasSpans ? (
              <FlameGraph
                spans={spans}
                selectedSpan={selectedSpan}
                onSelectSpan={handleSelectSpan}
                zoomPath={zoomPath}
                onZoomPathChange={setZoomPath}
              />
            ) : (
              <div className="flex h-24 items-center justify-center rounded-xl border border-dashed border-border-default bg-surface-primary/20 text-xs text-slate-500">
                {showLoadingOverlay
                  ? "Loading execution data…"
                  : "No execution data found for this database."}
              </div>
            )}
          </div>

          <div className="flex min-h-0 flex-1 flex-col">
            {selectedSpan ? (
              <SelectedSpanDetail span={selectedSpan} metrics={spanMetrics} />
            ) : hasSpans ? (
              <div className="flex min-h-[9rem] flex-1 items-center rounded-lg border border-dashed border-border-default bg-surface-primary/40 px-3 py-2 text-xs text-slate-500">
                Click a segment to view details and resource metrics.
              </div>
            ) : (
              <div className="min-h-[9rem] flex-1 rounded-lg border border-dashed border-border-default bg-surface-primary/20" />
            )}
          </div>
        </div>

        {showLoadingOverlay ? (
          <div className="absolute inset-3 z-10 flex items-center justify-center rounded-lg bg-surface-inset/88 backdrop-blur-[1px]">
            <div className="inline-flex items-center gap-2 rounded-full border border-border-default bg-surface-primary/80 px-3 py-1.5 text-xs text-slate-300 shadow-sm">
              <LoaderCircle className="size-3.5 animate-spin" />
              <span>Loading execution data…</span>
            </div>
          </div>
        ) : null}
      </div>
    </PanelCard>
  )
}

interface SelectedSpanDetailProps {
  span: FlameSpan
  metrics: SpanMetrics | null
}

function SelectedSpanDetail({ span, metrics }: SelectedSpanDetailProps) {
  const label = span.depth === "operation" ? span.operation : (span.query_name ?? span.step_name)

  return (
    <div className="flex h-full min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-border-default bg-surface-primary/60 p-3">
      <div className="flex shrink-0 flex-wrap items-start gap-x-4 gap-y-3">
        <div className="min-w-0 flex-1">
          <MetaLabel>
            {span.depth === "operation" ? "Operation" : span.depth === "step" ? "Step" : "Query"}
          </MetaLabel>
          <p className="mt-0.5 text-xs font-medium text-slate-200">
            {label}
            {span.iteration !== null && span.iteration > 1 ? (
              <span className="ml-1 text-slate-500">#{span.iteration}</span>
            ) : null}
          </p>
        </div>

        <div className="flex flex-wrap gap-3">
          <StatMini label="Duration" value={formatDurationSeconds(span.duration_s)} />
          {metrics ? (
            <>
              <StatMini label="Avg CPU" value={formatCpuPercent(metrics.avgCpu)} />
              <StatMini label="Avg Mem" value={formatMegabytes(metrics.avgMem)} />
              <StatMini label="Avg Disk" value={formatMegabytes(metrics.avgDisk)} />
            </>
          ) : (
            <span className="self-center text-[0.65rem] text-slate-500">No metrics</span>
          )}
        </div>
      </div>

      {span.query_sql ? (
        <div className="mt-3 min-h-0 flex-1 overflow-hidden rounded-md border border-border-default bg-surface-inset">
          <SqlCodeView code={span.query_sql} wrapLines fillHeight />
        </div>
      ) : null}
    </div>
  )
}

function StatMini({ label, value }: { label: string; value: string }) {
  return (
    <div className="text-center">
      <MetaLabel>{label}</MetaLabel>
      <p className="mt-0.5 text-sm font-semibold text-slate-200 tabular-nums">{value}</p>
    </div>
  )
}

interface DatabaseSelectorProps {
  databases: string[]
  selected: string | null
  onChange: (db: string) => void
}

function DatabaseSelector({ databases, selected, onChange }: DatabaseSelectorProps) {
  if (databases.length === 0) return null

  const value = selected ?? databases[0]!
  const currentIndex = databases.indexOf(value)
  const previousDb = databases[(currentIndex - 1 + databases.length) % databases.length]!
  const nextDb = databases[(currentIndex + 1) % databases.length]!

  return (
    <div className="inline-flex items-center gap-3 rounded-full border border-border-default bg-surface-inset px-2 py-1">
      <QuietButton
        size="xs"
        aria-label="Show previous flame graph database"
        onClick={() => onChange(previousDb)}
        className="size-7 rounded-full px-0"
      >
        <ChevronLeft className="size-3.5" />
      </QuietButton>
      <div className="min-w-[11.5rem] px-1 text-center">
        <MetaLabel className="tracking-[0.16em] text-slate-500">Database</MetaLabel>
        <p className="mt-0.5 inline-flex items-center justify-center gap-1.5 text-sm font-medium text-slate-100">
          <Database className="size-3.5 text-slate-400" strokeWidth={1.8} />
          <span>{value}</span>
        </p>
      </div>
      <QuietButton
        size="xs"
        aria-label="Show next flame graph database"
        onClick={() => onChange(nextDb)}
        className="size-7 rounded-full px-0"
      >
        <ChevronRight className="size-3.5" />
      </QuietButton>
    </div>
  )
}

function getFlameSpanSelectionKey(span: FlameSpan): string {
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

function getFlameSpanSelectionPath(span: FlameSpan): string[] {
  const operationKey = `operation:${span.operation}`
  if (span.depth === "operation") {
    return [operationKey]
  }

  const stepKey = `step:${span.operation}:${span.query_name ?? span.step_name}`
  if (span.depth === "step") {
    return [operationKey, stepKey]
  }

  return [operationKey, stepKey, getFlameSpanSelectionKey(span)]
}

function normalizeFlameQuerySql(querySql: string | null): string {
  return querySql?.replace(/\s+/g, " ").trim() ?? ""
}
