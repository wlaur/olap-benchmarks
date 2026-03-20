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
import { getDatabaseColors } from "../../lib/databaseColors"
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
import { DatabaseMultiSelect } from "../filters/DatabaseMultiSelect"
import { ChartFrame, PanelCard, PanelHeader } from "../layout/Panel"
import { BodyText, DisplayTitle, Eyebrow, SectionTitle } from "../Typography"
import {
  FilterChipsSkeleton,
  OverviewChartSkeleton,
  OverviewControlsSkeleton,
} from "./ExplorerSkeletons"

const OVERVIEW_CHART_HEIGHT = 220
const TOP_GRID_CLASS = "grid shrink-0 gap-4 xl:grid-cols-[minmax(24rem,0.95fr)_minmax(0,1.15fr)]"
const TOP_CARD_MIN_HEIGHT_CLASS = "min-h-[26rem]"
const OVERVIEW_HEADER_CLASS = "min-h-[8.5rem]"

interface OverviewPanelProps {
  suiteConfig: SuiteConfig
  databases: string[]
  includedDatabases: string[]
  operationSummaries: OperationSummary[]
  isLoading: boolean
  onSelectAll: () => void
  onToggleDatabase: (database: string) => void
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
  isLoading,
  onSelectAll,
  onToggleDatabase,
}: OverviewPanelProps) {
  const [overviewScaleMode, setOverviewScaleMode] = useState<DurationScaleMode>("linear")
  const [operationVisibility, setOperationVisibility] = useState<OverviewOperationVisibility>({
    populate: true,
    select: true,
    mutate: true,
  })

  const databaseColors = getDatabaseColors(databases)

  const runChartData = buildOverviewChartData(
    operationSummaries,
    overviewScaleMode,
    operationVisibility,
  ).map((entry) => ({
    ...entry,
    fill: databaseColors[entry.db] ?? "#94a3b8",
  }))
  const overviewMaxDuration = Math.max(
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
    <div className={TOP_GRID_CLASS}>
      <PanelCard className={TOP_CARD_MIN_HEIGHT_CLASS}>
        <div className="space-y-2">
          <Eyebrow>{suiteConfig.label}</Eyebrow>
          <DisplayTitle as="h2">Results explorer</DisplayTitle>
          <BodyText className="max-w-2xl leading-6 text-slate-300">
            Scope the comparison to the databases you care about, scan the aggregate spread, then
            drill into query-level behavior and SQL without leaving the screen.
          </BodyText>
        </div>

        <div className="mt-5">
          {isLoading ? (
            <FilterChipsSkeleton />
          ) : (
            <DatabaseMultiSelect
              databases={databases}
              selectedDatabases={includedDatabases}
              onSelectAll={onSelectAll}
              onToggleDatabase={onToggleDatabase}
            />
          )}
        </div>
      </PanelCard>

      <PanelCard className={TOP_CARD_MIN_HEIGHT_CLASS}>
        <PanelHeader className={OVERVIEW_HEADER_CLASS}>
          <div>
            <SectionTitle as="h3">Aggregate overview</SectionTitle>
            <BodyText className="mt-1">
              Latest completed durations per database. Toggle any phase on or off, then switch
              between log and linear scale before diving into per-query detail.
            </BodyText>
          </div>
          {isLoading ? (
            <OverviewControlsSkeleton />
          ) : (
            <div className="flex flex-wrap items-center justify-end gap-2">
              <div className="inline-flex rounded-full border border-border-default bg-surface-inset p-1">
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
                          ? "inline-flex items-center gap-2 rounded-full bg-sky-500/10 px-3 py-1 text-xs font-medium text-sky-100 shadow-[inset_0_0_0_1px_rgba(96,165,250,0.28)]"
                          : "inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
                      }
                    >
                      <span
                        className="size-2 rounded-full"
                        style={{
                          backgroundColor: isActive ? chipColor : "rgba(71, 85, 105, 0.9)",
                        }}
                      />
                      {label}
                    </button>
                  )
                })}
              </div>
              <DurationScaleToggle mode={overviewScaleMode} onChange={setOverviewScaleMode} />
              <div className="rounded-full border border-border-default bg-surface-inset px-3 py-1 text-xs font-medium whitespace-nowrap text-slate-400">
                {includedDatabases.length} of {databases.length} databases
              </div>
            </div>
          )}
        </PanelHeader>

        <ChartFrame className="mt-4" height={OVERVIEW_CHART_HEIGHT}>
          {isLoading ? (
            <OverviewChartSkeleton />
          ) : !hasVisibleOverviewSegments ? (
            <div className="flex h-full min-h-28 items-center justify-center rounded-xl border border-dashed border-border-default bg-surface-inset px-6 text-center text-sm text-slate-500">
              Enable at least one phase to display overview bars for the selected databases.
            </div>
          ) : (
            <OverviewBarChart
              data={runChartData}
              axisDomain={overviewAxisDomain}
              axisTicks={overviewAxisTicks}
              scaleMode={overviewScaleMode}
              operationVisibility={operationVisibility}
            />
          )}
        </ChartFrame>
      </PanelCard>
    </div>
  )
}

interface OverviewBarChartProps {
  data: OverviewChartRow[]
  axisDomain: [number, number]
  axisTicks: number[] | undefined
  scaleMode: DurationScaleMode
  operationVisibility: OverviewOperationVisibility
}

function OverviewBarChart({
  data,
  axisDomain,
  axisTicks,
  scaleMode,
  operationVisibility,
}: OverviewBarChartProps) {
  return (
    <ResponsiveContainer
      width="100%"
      height="100%"
      initialDimension={{ width: 640, height: OVERVIEW_CHART_HEIGHT }}
    >
      <BarChart data={data} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
        <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />
        <XAxis
          dataKey="db"
          tick={{ fill: "#64748b", fontSize: 11 }}
          axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
          tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
        />
        <YAxis
          domain={axisDomain}
          ticks={axisTicks}
          tick={{ fill: "#64748b", fontSize: 11 }}
          axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
          tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
          tickFormatter={(value: number) => formatDurationAxisTick(value, scaleMode)}
        />
        <Tooltip
          cursor={{ fill: "rgba(15, 23, 42, 0.3)" }}
          contentStyle={{
            backgroundColor: "#161a23",
            border: "1px solid rgba(148, 163, 184, 0.12)",
            borderRadius: 12,
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
          radius={[4, 4, 0, 0]}
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
          radius={[4, 4, 0, 0]}
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
          radius={[4, 4, 0, 0]}
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
