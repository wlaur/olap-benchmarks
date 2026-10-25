import type { ComponentPropsWithRef } from "react"

import { cn } from "../../lib/cn"

type ControlSize = "xs" | "sm" | "md"

const BUTTON_SIZE_CLASS: Record<ControlSize, string> = {
  xs: "min-h-9 px-2.5 py-1 text-sm",
  sm: "min-h-10 px-3 py-1.5 text-sm",
  md: "min-h-11 px-4 py-2 text-base",
}

const BUTTON_BASE_CLASS =
  "inline-flex items-center justify-center gap-1.5 rounded-md border font-medium transition-colors outline-none disabled:pointer-events-none disabled:opacity-50 focus-visible:border-accent-300 focus-visible:ring-2 focus-visible:ring-accent-300/60"

const SELECTED_CLASS =
  "border-accent-300/70 bg-surface-elevated text-slate-50 shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]"

const INACTIVE_CLASS =
  "border-border-default bg-surface-inset text-slate-200 hover:border-slate-500 hover:bg-surface-raised hover:text-slate-50"

interface ControlButtonProps extends ComponentPropsWithRef<"button"> {
  selected?: boolean
  size?: ControlSize
}

function ToggleButton({
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
        BUTTON_BASE_CLASS,
        BUTTON_SIZE_CLASS[size],
        selected ? SELECTED_CLASS : INACTIVE_CLASS,
        className,
      )}
      {...props}
    >
      {children}
    </button>
  )
}

export function SegmentedButton(props: ControlButtonProps) {
  return <ToggleButton {...props} />
}

export function ControlChip(props: ControlButtonProps) {
  return <ToggleButton {...props} />
}

interface QuietButtonProps extends ComponentPropsWithRef<"button"> {
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
      className={cn(BUTTON_BASE_CLASS, BUTTON_SIZE_CLASS[size], INACTIVE_CLASS, className)}
      {...props}
    >
      {children}
    </button>
  )
}
