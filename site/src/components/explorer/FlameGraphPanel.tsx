import * as Select from "@radix-ui/react-select"
import { Check, ChevronDown, ChevronUp, Database } from "lucide-react"
import { useCallback, useEffect, useMemo, useState } from "react"

import type { BenchmarkSuiteId } from "../../lib/benchmarks"
import { formatDurationSeconds } from "../../lib/format"
import { formatCpuPercent, formatMegabytes } from "../../lib/metricFormat"
import { fetchFlameSpans } from "../../lib/queries"
import type { FlameSpan, MetricSample } from "../../lib/types"
import { PanelCard } from "../layout/Panel"
import { BodyText, MetaLabel, SectionTitle } from "../Typography"
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
  const [isExpanded, setIsExpanded] = useState(false)
  const [selectedDb, setSelectedDb] = useState<string | null>(null)
  const [spans, setSpans] = useState<FlameSpan[]>([])
  const [selectedSpan, setSelectedSpan] = useState<FlameSpan | null>(null)
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
    if (!isExpanded) return

    let cancelled = false
    setIsLoadingSpans(true)
    setSelectedSpan(null)

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
  }, [isLoading, system, suite, resolvedDb, isExpanded])

  const spanMetrics = useMemo((): SpanMetrics | null => {
    if (!selectedSpan || !resolvedDb) return null

    const opSamples = metricSamples.filter(
      (s) => s.db === resolvedDb && s.operation === selectedSpan.operation,
    )
    if (opSamples.length === 0) return null

    // Find the run start time to map elapsed_s to the global timeline
    // operation samples have elapsed_s relative to their run start
    // spans have elapsed_s relative to the global (earliest run) start
    // We need to find the offset between them

    // Get the operation's start offset by finding the operation-level span
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
    setSelectedSpan(span)
  }, [])

  return (
    <PanelCard>
      <div className="flex items-start justify-between gap-4">
        <div>
          <SectionTitle as="h3">Execution flame graph</SectionTitle>
          <BodyText className="mt-1 max-w-3xl">
            Timeline of all operations, steps, and queries for a single database. Select a segment
            to view resource metrics during that window.
          </BodyText>
        </div>
        <button
          type="button"
          onClick={() => setIsExpanded((c) => !c)}
          disabled={isLoading || databases.length === 0}
          className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border-default bg-surface-inset px-4 py-2 text-sm font-medium text-slate-300 transition-colors hover:border-slate-700 hover:text-slate-100 disabled:cursor-not-allowed disabled:text-slate-500"
        >
          {isExpanded ? <ChevronUp className="size-4" /> : <ChevronDown className="size-4" />}
          {isExpanded ? "Hide" : "Show"}
        </button>
      </div>

      {!isExpanded ? null : (
        <div className="mt-5 rounded-2xl border border-border-default bg-surface-inset p-4">
          <div className="mb-4">
            <DatabaseSelector
              databases={databases}
              selected={resolvedDb}
              onChange={setSelectedDb}
            />
          </div>

          {isLoading ? (
            <div className="flex h-32 items-center justify-center text-sm text-slate-500">
              Loading execution data...
            </div>
          ) : isLoadingSpans ? (
            <div className="flex h-32 items-center justify-center text-sm text-slate-500">
              Loading execution data...
            </div>
          ) : (
            <div className="space-y-4">
              <FlameGraph
                spans={spans}
                selectedSpan={selectedSpan}
                onSelectSpan={handleSelectSpan}
              />

              <div className="min-h-[4.5rem]">
                {selectedSpan ? (
                  <SelectedSpanDetail span={selectedSpan} metrics={spanMetrics} />
                ) : spans.length > 0 ? (
                  <div className="rounded-xl border border-dashed border-border-default bg-surface-primary/40 px-4 py-3 text-xs text-slate-500">
                    Click a segment to view details and resource metrics.
                  </div>
                ) : null}
              </div>
            </div>
          )}
        </div>
      )}
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
    <div className="rounded-xl border border-border-default bg-surface-primary/60 p-4">
      <div className="flex flex-wrap items-start gap-x-6 gap-y-3">
        <div className="min-w-0 flex-1">
          <MetaLabel>
            {span.depth === "operation" ? "Operation" : span.depth === "step" ? "Step" : "Query"}
          </MetaLabel>
          <p className="mt-0.5 text-sm font-medium text-slate-200">
            {label}
            {span.iteration !== null && span.iteration > 1 ? (
              <span className="ml-1.5 text-slate-500">#{span.iteration}</span>
            ) : null}
          </p>
          {span.query_sql ? (
            <pre className="panel-scrollbar mt-2 max-h-24 overflow-auto rounded-lg bg-surface-inset p-2 text-[11px] leading-relaxed text-slate-400">
              {span.query_sql}
            </pre>
          ) : null}
        </div>

        <div className="flex gap-4">
          <StatMini label="Duration" value={formatDurationSeconds(span.duration_s)} />
          {metrics ? (
            <>
              <StatMini label="Avg CPU" value={formatCpuPercent(metrics.avgCpu)} />
              <StatMini label="Avg Memory" value={formatMegabytes(metrics.avgMem)} />
              <StatMini label="Avg Disk" value={formatMegabytes(metrics.avgDisk)} />
              <StatMini label="Samples" value={String(metrics.sampleCount)} />
            </>
          ) : (
            <span className="self-center text-xs text-slate-500">
              No resource metrics in this window
            </span>
          )}
        </div>
      </div>
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

  return (
    <Select.Root value={value} onValueChange={onChange}>
      <Select.Trigger
        aria-label="Database"
        className="inline-flex min-w-0 items-center justify-between gap-1.5 rounded-full bg-surface-raised/92 py-1 pr-2 pl-1.5 text-left shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] transition outline-none hover:bg-surface-raised hover:shadow-[inset_0_0_0_1px_rgba(120,154,214,0.18)] focus:shadow-[inset_0_0_0_1px_rgba(90,151,255,0.52),0_0_0_1px_rgba(90,151,255,0.2)]"
      >
        <span className="flex min-w-0 flex-1 items-center gap-2">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-emerald-400/12 text-emerald-300 shadow-[inset_0_0_0_1px_rgba(110,231,183,0.14)]">
            <Database className="h-3 w-3" strokeWidth={1.8} />
          </span>
          <span className="flex min-w-0 flex-1 items-center gap-2 whitespace-nowrap">
            <span className="shrink-0 text-[0.6rem] font-semibold tracking-[0.18em] text-slate-500 uppercase">
              Database
            </span>
            <span className="h-3 w-px shrink-0 bg-slate-700/50" />
            <Select.Value className="block min-w-0 flex-1 truncate text-[0.8125rem] font-medium whitespace-nowrap text-slate-100" />
          </span>
        </span>
        <Select.Icon className="shrink-0 text-slate-500">
          <ChevronDown className="h-3.5 w-3.5" strokeWidth={1.8} />
        </Select.Icon>
      </Select.Trigger>

      <Select.Portal>
        <Select.Content
          position="popper"
          sideOffset={10}
          className="z-50 max-h-80 w-[var(--radix-select-trigger-width)] overflow-hidden rounded-2xl border border-border-default bg-surface-primary/98 p-2 text-slate-100 shadow-2xl shadow-black/40 backdrop-blur data-[side=bottom]:translate-y-1 data-[side=top]:-translate-y-1"
        >
          <Select.ScrollUpButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronUp className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollUpButton>
          <Select.Viewport className="space-y-1">
            {databases.map((db) => (
              <Select.Item
                key={db}
                value={db}
                className="relative flex cursor-default items-center rounded-xl py-2.5 pr-8 pl-3 text-[0.8125rem] font-medium text-slate-200 transition outline-none data-[highlighted]:bg-emerald-500/10 data-[highlighted]:text-emerald-100 data-[state=checked]:bg-surface-raised data-[state=checked]:text-slate-50"
              >
                <span className="min-w-0 truncate">
                  <Select.ItemText>{db}</Select.ItemText>
                </span>
                <Select.ItemIndicator className="absolute right-3 inline-flex items-center text-emerald-300">
                  <Check className="h-3.5 w-3.5" strokeWidth={2} />
                </Select.ItemIndicator>
              </Select.Item>
            ))}
          </Select.Viewport>
          <Select.ScrollDownButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronDown className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollDownButton>
        </Select.Content>
      </Select.Portal>
    </Select.Root>
  )
}
