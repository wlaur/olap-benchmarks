import type { ReactNode } from "react"

import { cn } from "../lib/cn"
import { TIME_SERIES_OVERVIEW_CHART_HEIGHT, TIME_SERIES_TOP_CARD_CLASS } from "./timeSeriesLayout"

export function TimeSeriesTopCard({
  children,
  className,
}: {
  children: ReactNode
  className?: string
}) {
  return <div className={cn(TIME_SERIES_TOP_CARD_CLASS, className)}>{children}</div>
}

export function TimeSeriesOverviewHeader({
  children,
  className,
}: {
  children: ReactNode
  className?: string
}) {
  return (
    <div className={cn("flex min-h-[8.5rem] items-start justify-between gap-4", className)}>
      {children}
    </div>
  )
}

export function TimeSeriesOverviewChartFrame({ children }: { children: ReactNode }) {
  return (
    <div
      className="mt-4 rounded-2xl border border-slate-800 bg-slate-950/50 p-4"
      style={{ height: TIME_SERIES_OVERVIEW_CHART_HEIGHT }}
    >
      {children}
    </div>
  )
}
