import { ChevronDown } from "lucide-react"
import type { CSSProperties, ReactNode } from "react"

import { cn } from "../../lib/cn"
import { SectionTitle } from "../Typography"

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

export function PanelDisclosure({ children, title }: { children: ReactNode; title: string }) {
  return (
    <PanelCard className="p-0">
      <details className="group/panel">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 p-4 outline-none focus-visible:ring-2 focus-visible:ring-accent-300 focus-visible:ring-inset sm:p-5 [&::-webkit-details-marker]:hidden">
          <SectionTitle as="h2">{title}</SectionTitle>
          <ChevronDown
            aria-hidden="true"
            className="size-4 shrink-0 text-slate-400 transition-transform group-open/panel:rotate-180"
          />
        </summary>
        <div className="space-y-3 border-t border-border-subtle p-4 sm:p-5">{children}</div>
      </details>
    </PanelCard>
  )
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

export function NoticeStrip({
  children,
  tone = "info",
  className,
}: {
  children: ReactNode
  tone?: "info" | "warning"
  className?: string
}) {
  return (
    <div
      role={tone === "warning" ? "alert" : "note"}
      className={cn(
        "flex min-w-0 items-start gap-2.5 rounded-md border border-border-subtle bg-surface-inset px-3 py-2 font-sans text-xs leading-5 text-slate-300",
        className,
      )}
    >
      <span
        aria-hidden="true"
        className={cn(
          "mt-1.5 size-1.5 shrink-0 rounded-full",
          tone === "warning" ? "bg-amber-300" : "bg-accent-300",
        )}
      />
      <div className="min-w-0">{children}</div>
    </div>
  )
}
