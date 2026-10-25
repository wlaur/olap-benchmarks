import { ChevronDown } from "lucide-react"
import type { ComponentPropsWithRef, CSSProperties, ReactNode } from "react"

import { cn } from "../../lib/cn"
import { SectionTitle } from "../Typography"

export function PanelCard({ children, className, ...props }: ComponentPropsWithRef<"div">) {
  return (
    <div
      {...props}
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

export function PanelDisclosure({
  children,
  title,
  meta,
}: {
  children: ReactNode
  title: string
  meta?: ReactNode
}) {
  return (
    <PanelCard className="p-0">
      <details className="group/panel">
        <summary className="flex min-h-14 cursor-pointer list-none items-center justify-between gap-3 p-4 outline-none hover:bg-surface-elevated/40 focus-visible:ring-2 focus-visible:ring-accent-300/60 focus-visible:ring-inset sm:px-5 [&::-webkit-details-marker]:hidden">
          <span className="min-w-0">
            <SectionTitle as="h2">{title}</SectionTitle>
            {meta ? <span className="mt-1 block text-sm text-slate-300">{meta}</span> : null}
          </span>
          <ChevronDown
            aria-hidden="true"
            className="size-5 shrink-0 text-slate-400 transition-transform group-open/panel:rotate-180"
          />
        </summary>
        <div className="space-y-4 border-t border-border-subtle p-4 sm:p-5">{children}</div>
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
  role = "note",
  className,
}: {
  children: ReactNode
  tone?: "info" | "warning"
  role?: "note" | "alert"
  className?: string
}) {
  return (
    <div
      role={role}
      className={cn(
        "flex min-w-0 items-start gap-2.5 rounded-md border border-border-subtle bg-surface-inset px-3 py-2 text-sm leading-6 text-slate-200",
        tone === "warning" && "border-amber-300/30",
        className,
      )}
    >
      <span
        aria-hidden="true"
        className={cn(
          "mt-2 size-2 shrink-0 rounded-full",
          tone === "warning" ? "bg-amber-300" : "bg-accent-300",
        )}
      />
      <div className="min-w-0">{children}</div>
    </div>
  )
}

export type StatusTone = "positive" | "neutral" | "warning" | "failure"

const STATUS_TONE_CLASS: Record<StatusTone, string> = {
  positive: "border-emerald-300/35 text-emerald-200",
  neutral: "border-border-default text-slate-300",
  warning: "border-amber-300/40 text-amber-200",
  failure: "border-rose-300/40 text-rose-200",
}

export function StatusBadge({
  tone,
  children,
  className,
}: {
  tone: StatusTone
  children: ReactNode
  className?: string
}) {
  return (
    <span
      className={cn(
        "inline-flex max-w-full items-center rounded-md border bg-surface-inset px-2 py-0.5 text-sm leading-6 font-medium [overflow-wrap:anywhere]",
        STATUS_TONE_CLASS[tone],
        className,
      )}
    >
      {children}
    </span>
  )
}
