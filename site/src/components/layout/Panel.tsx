import type { CSSProperties, ReactNode } from "react"

import { cn } from "../../lib/cn"

export function PanelCard({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={cn(
        "min-h-0 min-w-0 overflow-hidden rounded-xl border border-border-default bg-surface-raised p-3 shadow-[0_1px_0_rgba(255,255,255,0.04)_inset,0_8px_24px_-12px_rgba(0,0,0,0.6)]",
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
      className={cn("min-h-0 min-w-0 overflow-hidden rounded-lg bg-surface-inset p-3", className)}
      style={resolvedStyle}
    >
      {children}
    </div>
  )
}
