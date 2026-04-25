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
import { ControlGroup, SegmentedButton } from "../controls/Control"
import { DurationScaleToggle } from "../DurationScaleToggle"
import { ChartFrame, PanelCard, PanelHeader } from "../layout/Panel"
import { MetaLabel, SectionTitle } from "../Typography"
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
  ["populate", "Populate", "rgba(245, 158, 11, 0.92)"],
  ["mutate", "Mutate", "rgba(139, 92, 246, 0.94)"],
  ["select", "Select", "rgba(96, 165, 250, 0.94)"],
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
  const visibleOperations: OverviewOperationVisibility = {
    populate: suiteConfig.operations.includes("populate") && operationVisibility.populate,
    mutate: suiteConfig.operations.includes("mutate") && operationVisibility.mutate,
    select: suiteConfig.operations.includes("select") && operationVisibility.select,
  }

  const runChartData = buildOverviewChartData(
    operationSummaries,
    includedDatabases,
    overviewScaleMode,
    visibleOperations,
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
            if (visibleOperations.populate) sum += run.populate_chart_duration_s
            if (visibleOperations.mutate) sum += run.mutate_chart_duration_s
            if (visibleOperations.select) sum += run.select_chart_duration_s
            return sum
          }),
        )
      : Math.max(
          0,
          ...runChartData.flatMap((run) => {
            const durations: number[] = []
            if (visibleOperations.populate) durations.push(run.populate_duration_s)
            if (visibleOperations.mutate) durations.push(run.mutate_duration_s)
            if (visibleOperations.select) durations.push(run.select_duration_s)
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
      <PanelHeader className="flex-col items-start gap-3">
        <SectionTitle as="h3">Aggregate overview</SectionTitle>
        {isLoading ? (
          <OverviewControlsSkeleton />
        ) : (
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex flex-wrap items-center gap-2">
              {availableChips.map(([operation, label, chipColor]) => {
                const isActive = operationVisibility[operation]
                return (
                  <SegmentedButton
                    key={operation}
                    aria-pressed={isActive}
                    onClick={() => toggleOverviewOperation(operation)}
                    selected={isActive}
                    size="md"
                    className="gap-2"
                  >
                    <span
                      className="size-2 rounded-full"
                      style={{
                        backgroundColor: chipColor,
                        opacity: isActive ? 1 : 0.55,
                      }}
                    />
                    {label}
                  </SegmentedButton>
                )
              })}
            </div>
            <BarModeToggle mode={barMode} onChange={setBarMode} />
            <DurationScaleToggle mode={overviewScaleMode} onChange={setOverviewScaleMode} />
            <MetaLabel className="tracking-normal text-slate-400 normal-case">
              {includedDatabases.length === databases.length
                ? `${databases.length} databases shown`
                : `${includedDatabases.length} of ${databases.length} databases shown`}
            </MetaLabel>
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
            operationVisibility={visibleOperations}
            barMode={barMode}
          />
        )}
      </ChartFrame>
    </PanelCard>
  )
}

function BarModeToggle({ mode, onChange }: { mode: BarMode; onChange: (m: BarMode) => void }) {
  return (
    <ControlGroup compact>
      <SegmentedButton selected={mode === "grouped"} size="md" onClick={() => onChange("grouped")}>
        Grouped
      </SegmentedButton>
      <SegmentedButton selected={mode === "stacked"} size="md" onClick={() => onChange("stacked")}>
        Stacked
      </SegmentedButton>
    </ControlGroup>
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
          isAnimationActive={false}
          shape={(props) => (
            <Rectangle
              {...props}
              fill={withAlpha(
                (props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8",
                0.65,
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
          isAnimationActive={false}
          shape={(props) => (
            <Rectangle
              {...props}
              fill={withAlpha(
                (props.payload as { fill?: string } | undefined)?.fill ?? "#94a3b8",
                0.85,
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
          isAnimationActive={false}
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
