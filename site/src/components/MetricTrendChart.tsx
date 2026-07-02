import type { CSSProperties } from "react"
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { cn } from "../lib/cn"
import { buildElapsedTicks, formatElapsedSeconds } from "../lib/format"
import type { MetricScaleBuilder } from "../lib/metricFormat"
import { CHART_TOOLTIP_STYLES } from "./layout/Panel"

export interface MetricTrendRow {
  elapsed_s: number
  [db: string]: number | undefined
}

type MetricTrendChartSize = "sm" | "md"

interface SizeConfig {
  labelClassName: string
  chartClassName: string
  initialDimension: { width: number; height: number }
  tickFontSize: number
  yAxisWidth: number
  strokeWidth: number
  tooltipItemStyle: CSSProperties | undefined
}

const SIZE_CONFIGS: Record<MetricTrendChartSize, SizeConfig> = {
  sm: {
    labelClassName: "mb-1.5",
    chartClassName: "h-28",
    initialDimension: { width: 400, height: 128 },
    tickFontSize: 10,
    yAxisWidth: 60,
    strokeWidth: 1.5,
    tooltipItemStyle: undefined,
  },
  md: {
    labelClassName: "mb-2",
    chartClassName: "h-36",
    initialDimension: { width: 640, height: 160 },
    tickFontSize: 11,
    yAxisWidth: 70,
    strokeWidth: 2,
    tooltipItemStyle: CHART_TOOLTIP_STYLES.itemStyle,
  },
}

interface MetricTrendChartProps {
  label: string
  data: MetricTrendRow[]
  databases: string[]
  databaseColors: Record<string, string>
  maxElapsed: number
  formatter: (value: number) => string
  scaleBuilder: MetricScaleBuilder
  syncId: string
  size: MetricTrendChartSize
}

export function MetricTrendChart({
  label,
  data,
  databases,
  databaseColors,
  maxElapsed,
  formatter,
  scaleBuilder,
  syncId,
  size,
}: MetricTrendChartProps) {
  const config = SIZE_CONFIGS[size]

  const yValues = data.flatMap((row) =>
    databases.map((db) => row[db]).filter((v): v is number => v !== undefined),
  )
  const yScale = scaleBuilder(Math.max(1, ...yValues))
  const tickFormatter = yScale.formatter ?? formatter

  const xTicks = buildElapsedTicks(maxElapsed)

  return (
    <>
      <p className={cn("text-xs font-semibold text-slate-200", config.labelClassName)}>{label}</p>
      <div className={config.chartClassName}>
        <ResponsiveContainer width="100%" height="100%" initialDimension={config.initialDimension}>
          <LineChart data={data} syncId={syncId} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="rgba(148, 163, 184, 0.06)" vertical={false} />
            <XAxis
              type="number"
              dataKey="elapsed_s"
              domain={[0, maxElapsed]}
              ticks={xTicks}
              tick={{ fill: "#94a3b8", fontSize: config.tickFontSize }}
              axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickFormatter={formatElapsedSeconds}
            />
            <YAxis
              domain={yScale.domain}
              ticks={yScale.ticks}
              width={config.yAxisWidth}
              tick={{ fill: "#94a3b8", fontSize: config.tickFontSize }}
              axisLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickLine={{ stroke: "rgba(148, 163, 184, 0.1)" }}
              tickFormatter={tickFormatter}
            />
            <Tooltip
              contentStyle={CHART_TOOLTIP_STYLES.contentStyle}
              labelStyle={CHART_TOOLTIP_STYLES.labelStyle}
              itemStyle={config.tooltipItemStyle}
              cursor={{ stroke: "rgba(148, 163, 184, 0.15)", strokeDasharray: "4 4" }}
              labelFormatter={(value) => {
                const numericValue = typeof value === "number" ? value : Number(value ?? 0)
                return `Elapsed ${formatElapsedSeconds(numericValue)}`
              }}
              formatter={(value, _name, item) => {
                const numericValue = typeof value === "number" ? value : Number(value ?? 0)
                return [formatter(numericValue), item.name ?? ""] as const
              }}
            />
            {databases.map((db) => (
              <Line
                key={db}
                type="stepAfter"
                name={db}
                dataKey={(row: MetricTrendRow) => row[db]}
                connectNulls
                dot={false}
                activeDot={{ r: 3, strokeWidth: 0 }}
                stroke={databaseColors[db] ?? "#94a3b8"}
                strokeWidth={config.strokeWidth}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </>
  )
}
