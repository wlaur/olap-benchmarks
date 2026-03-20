import type { DurationScaleMode } from "../lib/format"

interface DurationScaleToggleProps {
  mode: DurationScaleMode
  onChange: (mode: DurationScaleMode) => void
  compact?: boolean
}

export function DurationScaleToggle({ mode, onChange, compact = false }: DurationScaleToggleProps) {
  const containerClassName = compact
    ? "inline-flex rounded-full border border-border-default bg-surface-inset p-0.5"
    : "inline-flex rounded-full border border-border-default bg-surface-inset p-1"
  const activeClassName = compact
    ? "rounded-full bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-100 shadow-[inset_0_0_0_1px_rgba(96,165,250,0.28)]"
    : "rounded-full bg-sky-500/10 px-3 py-1 text-xs font-medium text-sky-100 shadow-[inset_0_0_0_1px_rgba(96,165,250,0.28)]"
  const inactiveClassName = compact
    ? "rounded-full px-2 py-0.5 text-[10px] font-medium text-slate-400 transition-colors hover:text-slate-200"
    : "rounded-full px-3 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"

  return (
    <div className={containerClassName}>
      {(["linear", "log"] as const).map((option) => (
        <button
          key={option}
          onClick={() => onChange(option)}
          className={mode === option ? activeClassName : inactiveClassName}
        >
          {option === "log" ? "Log" : "Linear"}
        </button>
      ))}
    </div>
  )
}
