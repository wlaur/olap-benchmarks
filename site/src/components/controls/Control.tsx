import type { ButtonHTMLAttributes } from "react"

import { cn } from "../../lib/cn"

type ControlSize = "xs" | "sm" | "md"

const BUTTON_SIZE_CLASS: Record<ControlSize, string> = {
  xs: "px-2 py-0.5 text-[11px]",
  sm: "px-2.5 py-1 text-xs",
  md: "px-3.5 py-1.5 text-sm",
}

interface ControlButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  selected?: boolean
  size?: ControlSize
}

export function SegmentedButton({
  selected = false,
  size = "sm",
  type,
  className,
  children,
  ...props
}: ControlButtonProps) {
  return (
    <button
      type={type ?? "button"}
      className={cn(
        "inline-flex items-center justify-center rounded-md border font-medium transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "focus-visible:border-accent-300 focus-visible:ring-2 focus-visible:ring-accent-400/15",
        BUTTON_SIZE_CLASS[size],
        selected
          ? "border-accent-300/60 bg-surface-elevated text-slate-50 shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]"
          : "border-border-default bg-surface-inset text-slate-300 hover:border-slate-500 hover:bg-surface-raised hover:text-slate-100",
        className,
      )}
      {...props}
    >
      {children}
    </button>
  )
}

export function ControlChip({
  selected = false,
  size = "sm",
  type,
  className,
  children,
  ...props
}: ControlButtonProps) {
  return (
    <button
      type={type ?? "button"}
      className={cn(
        "inline-flex items-center justify-center rounded-md border font-medium transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "focus-visible:border-accent-300 focus-visible:ring-2 focus-visible:ring-accent-400/15",
        BUTTON_SIZE_CLASS[size],
        selected
          ? "border-accent-300/60 bg-surface-elevated text-slate-50 shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]"
          : "border-border-default bg-surface-inset text-slate-300 hover:border-slate-500 hover:bg-surface-raised hover:text-slate-100",
        className,
      )}
      {...props}
    >
      {children}
    </button>
  )
}

interface QuietButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  size?: ControlSize
}

export function QuietButton({
  size = "sm",
  type,
  className,
  children,
  ...props
}: QuietButtonProps) {
  return (
    <button
      type={type ?? "button"}
      className={cn(
        "inline-flex items-center justify-center rounded-md border border-border-default bg-surface-inset font-medium text-slate-200 transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "hover:border-slate-500 hover:bg-surface-raised hover:text-slate-50 focus-visible:border-accent-300 focus-visible:ring-2 focus-visible:ring-accent-400/15",
        BUTTON_SIZE_CLASS[size],
        className,
      )}
      {...props}
    >
      {children}
    </button>
  )
}
