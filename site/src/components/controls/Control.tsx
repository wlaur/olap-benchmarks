import type { ButtonHTMLAttributes, HTMLAttributes, ReactNode } from "react"

import { cn } from "../../lib/cn"

type ControlSize = "xs" | "sm" | "md"

const BUTTON_SIZE_CLASS: Record<ControlSize, string> = {
  xs: "px-2 py-0.5 text-[11px]",
  sm: "px-2.5 py-1 text-xs",
  md: "px-3.5 py-1.5 text-sm",
}

interface ControlGroupProps extends HTMLAttributes<HTMLDivElement> {
  compact?: boolean
}

export function ControlGroup({
  compact = false,
  className,
  children,
  ...props
}: ControlGroupProps) {
  return (
    <div
      className={cn(
        "inline-flex items-center rounded-full border border-border-default bg-surface-inset",
        compact ? "gap-0.5 p-0.5" : "gap-1 p-1",
        className,
      )}
      {...props}
    >
      {children}
    </div>
  )
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
        "inline-flex items-center justify-center rounded-full border font-medium transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "focus-visible:border-slate-400/60 focus-visible:ring-2 focus-visible:ring-slate-300/15",
        BUTTON_SIZE_CLASS[size],
        selected
          ? "border-slate-300/40 bg-surface-primary text-slate-100 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.06)]"
          : "border-border-default bg-surface-inset text-slate-400 hover:border-slate-600 hover:text-slate-200",
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
        "inline-flex items-center justify-center rounded-full border font-medium transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "focus-visible:border-slate-400/60 focus-visible:ring-2 focus-visible:ring-slate-300/15",
        BUTTON_SIZE_CLASS[size],
        selected
          ? "border-slate-300/40 bg-surface-primary text-slate-100 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.06)]"
          : "border-border-default bg-surface-inset text-slate-500 hover:border-slate-600 hover:text-slate-300",
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
        "inline-flex items-center justify-center rounded-full border border-border-default bg-surface-primary/55 font-medium text-slate-300 transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "hover:border-slate-600 hover:text-slate-100 focus-visible:border-slate-400/60 focus-visible:ring-2 focus-visible:ring-slate-300/15",
        BUTTON_SIZE_CLASS[size],
        className,
      )}
      {...props}
    >
      {children}
    </button>
  )
}

interface InlineButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  active?: boolean
  size?: ControlSize
  leadingVisual?: ReactNode
}

export function InlineButton({
  active = false,
  size = "sm",
  type,
  className,
  children,
  leadingVisual,
  ...props
}: InlineButtonProps) {
  return (
    <button
      type={type ?? "button"}
      className={cn(
        "inline-flex items-center rounded-md font-medium transition-colors outline-none disabled:pointer-events-none disabled:opacity-50",
        "focus-visible:bg-surface-inset focus-visible:text-slate-100 focus-visible:ring-2 focus-visible:ring-slate-300/15",
        BUTTON_SIZE_CLASS[size],
        active
          ? "bg-surface-inset text-slate-100"
          : "text-slate-400 hover:bg-surface-inset hover:text-slate-200",
        className,
      )}
      {...props}
    >
      {leadingVisual}
      {children}
    </button>
  )
}
