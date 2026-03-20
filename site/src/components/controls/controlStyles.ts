import { cn } from "../../lib/cn"

type ControlSize = "xs" | "sm" | "md"

const SIZE_CLASS: Record<ControlSize, string> = {
  xs: "px-2 py-0.5 text-[11px]",
  sm: "px-2.5 py-1 text-xs",
  md: "px-3.5 py-1.5 text-sm",
}

export function controlGroupClass(compact = false) {
  return cn(
    "inline-flex items-center rounded-full border border-border-default bg-surface-inset",
    compact ? "gap-0.5 p-0.5" : "gap-1 p-1",
  )
}

export function controlButtonClass(selected: boolean, size: ControlSize = "sm") {
  return cn(
    "inline-flex items-center rounded-full border font-medium transition-colors",
    SIZE_CLASS[size],
    selected
      ? "border-slate-300/45 bg-surface-primary text-slate-100 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.08)]"
      : "border-border-default bg-surface-inset text-slate-400 hover:border-slate-600 hover:text-slate-200",
  )
}

export function controlChipClass(selected: boolean, size: ControlSize = "sm") {
  return cn(
    "inline-flex items-center rounded-full border font-medium transition-colors",
    SIZE_CLASS[size],
    selected
      ? "border-slate-300/45 bg-surface-primary text-slate-100 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.08)]"
      : "border-border-default bg-surface-inset text-slate-500 hover:border-slate-600 hover:text-slate-300",
  )
}

export function quietActionButtonClass(size: ControlSize = "sm") {
  return cn(
    "inline-flex items-center rounded-full border border-border-default bg-surface-primary/55 font-medium text-slate-300 transition-colors hover:border-slate-600 hover:text-slate-100",
    SIZE_CLASS[size],
  )
}
