import type { CSSProperties, ReactNode } from "react"

import { cn } from "../../lib/cn"

export function PanelCard({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("rounded-2xl bg-surface-raised p-5", className)}>{children}</div>
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
    <div className={cn("rounded-xl bg-surface-inset p-4", className)} style={resolvedStyle}>
      {children}
    </div>
  )
}
