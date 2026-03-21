import { memo } from "react"

import type { DurationScaleMode } from "../lib/format"

const LOG_FLOOR = 1e-6
const LOG_MIN = Math.log10(LOG_FLOOR)
const CHART_WIDTH = 100
const MIN_VISIBLE_BAR_WIDTH = 1.2

interface InlineDurationBarsProps {
  byDatabase: Record<string, number | null>
  databases: string[]
  databaseColors: Record<string, string>
  maxDuration: number
  linearMaxDuration: number
  scaleMode: DurationScaleMode
}

export const InlineDurationBars = memo(function InlineDurationBars({
  byDatabase,
  databases,
  databaseColors,
  maxDuration,
  linearMaxDuration,
  scaleMode,
}: InlineDurationBarsProps) {
  const barHeight = 5
  const gap = 2
  const height = databases.length * (barHeight + gap)
  const logMax = Math.log10(Math.max(maxDuration, LOG_FLOOR))
  const range = logMax - LOG_MIN

  return (
    <svg
      width="100%"
      height={height}
      viewBox={`0 0 ${CHART_WIDTH} ${height}`}
      preserveAspectRatio="none"
      className="min-w-[110px]"
    >
      {databases.map((db, idx) => {
        const val = byDatabase[db]
        if (val === null || val === undefined) return null
        const rawWidth =
          scaleMode === "linear"
            ? linearMaxDuration <= 0
              ? CHART_WIDTH
              : Math.min(1, Math.max(0, val / linearMaxDuration)) * CHART_WIDTH
            : (() => {
                const logVal = Math.log10(Math.max(val, LOG_FLOOR))
                return range <= 0
                  ? CHART_WIDTH
                  : Math.max(0, (logVal - LOG_MIN) / range) * CHART_WIDTH
              })()
        const width = val > 0 ? Math.min(CHART_WIDTH, Math.max(rawWidth, MIN_VISIBLE_BAR_WIDTH)) : 0

        return (
          <rect
            key={db}
            x={0}
            y={idx * (barHeight + gap)}
            width={width}
            height={barHeight}
            fill={databaseColors[db]}
            rx={2}
          />
        )
      })}
    </svg>
  )
})
