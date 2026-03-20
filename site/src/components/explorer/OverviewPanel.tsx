import { useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Rectangle,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import {
  buildOverviewChartData,
  withAlpha,
  type OverviewChartRow,
  type OverviewOperationVisibility,
} from "../../lib/chartTransforms"
import {
  formatDurationAxisTick,
  formatDurationSeconds,
  getDurationAxisDomain,
  getDurationAxisTicks,
  type DurationScaleMode,
} from "../../lib/format"
import type { SuiteConfig } from "../../lib/suiteConfig"
import type { BenchmarkOperation, OperationSummary } from "../../lib/types"
import { DurationScaleToggle } from "../DurationScaleToggle"
import { ChartFrame, PanelCard, PanelHeader } from "../layout/Panel"
import { SectionTitle } from "../Typography"
import { OverviewChartSkeleton, OverviewControlsSkeleton } from "./ExplorerSkeletons"

const OVERVIEW_CHART_HEIGHT = 200

type BarMode = "grouped" | "stacked"

interface OverviewPanelProps {
  suiteConfig: SuiteConfig
  databases: string[]
  includedDatabases: string[]
  operationSummaries: OperationSummary[]
  databaseColors: Record<string, string>
  isLoading: boolean
}

const OPERATION_CHIPS: ReadonlyArray<readonly [BenchmarkOperation, string, string]> = [
  ["populate", "Populate", "rgba(148, 163, 184, 0.45)"],
  ["mutate", "Mutate", "rgba(168, 85, 247, 0.85)"],
  ["select", "Select", "rgba(148, 163, 184, 1)"],
]

export function OverviewPanel({
  suiteConfig,
  databases,
  includedDatabases,
  operationSummaries,
  databaseColors,
  isLoading,
}: OverviewPanelProps) {
  const [overviewScaleMode, setOverviewScaleMode] = useState<DurationScaleMode>("linear")
  const [barMode, setBarMode] = useState<BarMode>("grouped")
  const [operationVisibility, setOperationVisibility] = useState<OverviewOperationVisibility>({
    populate: true,
    select: true,
    mutate: true,
  })

  const runChartData = buildOverviewChartData(
    operationSummaries,
    overviewScaleMode,
    operationVisibility,
  ).map((entry) => ({
    ...entry,
    fill: databaseColors[entry.db] ?? "#94a3b8",
  }))

  const overviewMaxDuration =
    barMode === "stacked"
      ? Math.max(
          0,
          ...runChartData.map((run) => {
            let sum = 0
            if (operationVisibility.populate) sum += run.populate_chart_duration_s
            if (operationVisibility.mutate) sum += run.mutate_chart_duration_s
            if (operationVisibility.select) sum += run.select_chart_duration_s
            return sum
          }),
        )
      : Math.max(
          0,
          ...runChartData.flatMap((run) => {
            const durations: number[] = []
            if (operationVisibility.populate) durations.push(run.populate_duration_s)
            if (operationVisibility.mutate) durations.push(run.mutate_duration_s)
            if (operationVisibility.select) durations.push(run.select_duration_s)
            return durations
          }),
        )

  const overviewAxisDomain = getDurationAxisDomain(overviewMaxDuration, overviewScaleMode)
  const overviewAxisTicks = getDurationAxisTicks(overviewMaxDuration, overviewScaleMode)
  const hasVisibleOverviewSegments = runChartData.some((entry) => entry.total_duration_s > 0)

  function toggleOverviewOperation(operation: BenchmarkOperation) {
    setOperationVisibility((current) => ({
      ...current,
      [operation]: !current[operation],
    }))
  }

  const availableChips = OPERATION_CHIPS.filter(([op]) => suiteConfig.operations.includes(op))

  return (
    <PanelCard className="h-full p-3">
      <PanelHeader>
        <SectionTitle as="h3">Aggregate overview</SectionTitle>
        {isLoading ? (
          <OverviewControlsSkeleton />
        ) : (
          <div className="flex flex-wrap items-center gap-1.5">
            <div className="inline-flex rounded-full border border-border-default bg-surface-inset p-0.5">
              {availableChips.map(([operation, label, chipColor]) => {
                const isActive = operationVisibility[operation]
                return (
                  <button
                    key={operation}
                    type="button"
                    aria-pressed={isActive}
                    onClick={() => toggleOverviewOperation(operation)}
                    className={
                      isActive
                        ? "inline-flex items-center gap-1.5 rounded-full bg-white/8 px-2.5 py-1 text-xs font-medium text-slate-100 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.2)]"
                        : "inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
                    }
                  >
                    <span
                      className="size-1.5 rounded-full"
                      style={{
                        backgroundColor: isActive ? chipColor : "rgba(71, 85, 105, 0.6)",
                      }}
                    />
                    {label}
                  </button>
                )
              })}
            </div>
            <BarModeToggle mode={barMode} onChange={setBarMode} />
            <DurationScaleToggle mode={overviewScaleMode} onChange={setOverviewScaleMode} />
            <span className="text-[0.65rem] text-slate-400 tabular-nums">
              {includedDatabases.length}/{databases.length}
            </span>
          </div>
        )}
      </PanelHeader>

      <ChartFrame className="mt-2 p-2" height={OVERVIEW_CHART_HEIGHT}>
        {isLoading ? (
          <OverviewChartSkeleton />
        ) : !hasVisibleOverviewSegments ? (
          <div className="flex h-full items-center justify-center text-xs text-slate-500">
            Enable at least one phase to display overview bars.
          </div>
        ) : (
          <OverviewBarChart
            data={runChartData}
            axisDomain={overviewAxisDomain}
            axisTicks={overviewAxisTicks}
            scaleMode={overviewScaleMode}
            operationVisibility={operationVisibility}
            barMode={barMode}
          />
        )}
      </ChartFrame>
    </PanelCard>
  )
}

function BarModeToggle({ mode, onChange }: { mode: BarMode; onChange: (m: BarMode) => void }) {
  return (
    <div className="inline-flex rounded-full border border-border-default bg-surface-inset p-0.5">
      <button
        type="button"
        onClick={() => onChange("grouped")}
        className={
          mode === "grouped"
            ? "rounded-full bg-white/8 px-2.5 py-1 text-xs font-medium text-slate-100 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.2)]"
            : "rounded-full px-2.5 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
        }
      >
        Grouped
      </button>
      <button
        type="button"
        onClick={() => onChange("stacked")}
        className={
          mode === "stacked"
            ? "rounded-full bg-white/8 px-2.5 py-1 text-xs font-medium text-slate-100 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.2)]"
            : "rounded-full px-2.5 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
        }
      >
        Stacked
      </button>
    </div>
  )
}

interface OverviewBarChartProps {
  data: OverviewChartRow[]
  axisDomain: [number, number]
  axisTicks: number[] | undefined
  scaleMode: DurationScaleMode
  operationVisibility: OverviewOperationVisibility
  barMode: BarMode
}

function OverviewBarChart({
  data,
  axisDomain,
  axisTicks,
  scaleMode,
  operationVisibility,
  barMode,
}: OverviewBarChartProps) {
  const stackId = barMode === "stacked" ? "ops" : undefined

  return (
    <ResponsiveContainer
      width="100%"
      height="100%"
      initialDimension={{ width: 640, height: OVERVIEW_CHART_HEIGHT }}
    >
      <BarChart data={data} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
        <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />
        <XAxis
          dataKey="db"
          tick={{ fill: "#94a3b8", fontSize: 11 }}
          axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
          tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
        />
        <YAxis
          domain={axisDomain}
          ticks={axisTicks}
          tick={{ fill: "#94a3b8", fontSize: 10 }}
          axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
          tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
          tickFormatter={(value: number) => formatDurationAxisTick(value, scaleMode)}
        />
        <Tooltip
          cursor={{ fill: "rgba(15, 23, 42, 0.3)" }}
          contentStyle={{
            backgroundColor: "#1e2330",
            border: "1px solid rgba(148, 163, 184, 0.12)",
            borderRadius: 10,
            fontSize: 12,
            color: "#e2e8f0",
          }}
          labelStyle={{ color: "#e2e8f0" }}
          itemStyle={{ color: "#e2e8f0" }}
          itemSorter={(item) => {
            const order: Record<string, number> = { Populate: 0, Mutate: 1, Select: 2 }
            return order[item.name ?? ""] ?? 3
          }}
          formatter={(_value, name, item) => {
            const row = item.payload as OverviewChartRow
            const duration =
              name === "Populate"
                ? row.populate_duration_s
                : name === "Select"
                  ? row.select_duration_s
                  : row.mutate_duration_s

            return [formatDurationSeconds(duration), name ?? ""] as const
          }}
          labelFormatter={(label, payload) => {
            const row = payload?.[0]?.payload as OverviewChartRow | undefined
            if (!row) return label

            const labelText =
              typeof label === "string" || typeof label === "number" ? String(label) : ""
            const visibleCount = Object.values(operationVisibility).filter(Boolean).length

            return visibleCount > 1
              ? `${labelText} · total ${formatDurationSeconds(row.total_duration_s)}`
              : `${labelText} · ${formatDurationSeconds(row.total_duration_s)}`
          }}
        />
        <Bar
          dataKey="populate_chart_duration_s"
          hide={!operationVisibility.populate}
          stackId={stackId}
          radius={barMode === "stacked" ? undefined : [3, 3, 0, 0]}
          name="Populate"
          shape={(props) => (
            <Rectangle
              {...props}
              fill={withAlpha(
                (props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8",
                0.45,
              )}
            />
          )}
        />
        <Bar
          dataKey="mutate_chart_duration_s"
          hide={!operationVisibility.mutate}
          stackId={stackId}
          radius={barMode === "stacked" ? undefined : [3, 3, 0, 0]}
          name="Mutate"
          shape={(props) => (
            <Rectangle
              {...props}
              fill={withAlpha(
                (props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8",
                0.7,
              )}
            />
          )}
        />
        <Bar
          dataKey="select_chart_duration_s"
          hide={!operationVisibility.select}
          stackId={stackId}
          radius={barMode === "stacked" ? undefined : [3, 3, 0, 0]}
          name="Select"
          shape={(props) => (
            <Rectangle
              {...props}
              fill={(props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8"}
            />
          )}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
