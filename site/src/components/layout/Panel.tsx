import type { CSSProperties, ReactNode } from "react"

import { cn } from "../../lib/cn"

export function PanelCard({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cn("rounded-3xl border border-slate-800 bg-slate-900/70 p-5", className)}>
      {children}
    </div>
  )
}

export function PanelHeader({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex items-start justify-between gap-4", className)}>{children}</div>
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
      className={cn("rounded-2xl border border-slate-800 bg-slate-950/50 p-4", className)}
      style={resolvedStyle}
    >
      {children}
    </div>
  )
}
