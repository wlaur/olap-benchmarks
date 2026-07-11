import type { CSSProperties, ReactNode } from "react"

import { cn } from "../../lib/cn"

export const CHART_TOOLTIP_STYLES = {
  contentStyle: {
    backgroundColor: "#0b1018",
    border: "1px solid rgba(148, 163, 184, 0.3)",
    borderRadius: 6,
    color: "#e2e8f0",
  },
  labelStyle: { color: "#e2e8f0" },
  itemStyle: { color: "#e2e8f0" },
} satisfies Record<string, CSSProperties>

export function PanelCard({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={cn(
        "min-h-0 min-w-0 overflow-hidden rounded-lg border border-border-default bg-surface-raised p-3 shadow-[0_1px_0_rgba(255,255,255,0.025)_inset,0_12px_32px_-24px_rgba(0,0,0,0.9)]",
        className,
      )}
    >
      {children}
    </div>
  )
}

export function PanelHeader({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex items-start justify-between gap-3", className)}>{children}</div>
}

export function ChartFrame({
  children,
  className,
  height,
  style,
}: {
  children: ReactNode
  className?: string
  height?: number
  style?: CSSProperties
}) {
  const resolvedStyle = height === undefined ? style : { ...style, height }

  return (
    <div
      className={cn(
        "min-h-0 min-w-0 overflow-hidden rounded-md border border-border-subtle bg-surface-inset p-3",
        className,
      )}
      style={resolvedStyle}
    >
      {children}
    </div>
  )
}
