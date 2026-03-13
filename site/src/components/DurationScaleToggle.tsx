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
    ? "rounded-full bg-accent-400/10 px-2 py-0.5 text-[10px] font-medium text-accent-200 shadow-[inset_0_0_0_1px_rgba(108,142,239,0.4)]"
    : "rounded-full bg-accent-400/10 px-3 py-1 text-xs font-medium text-accent-200 shadow-[inset_0_0_0_1px_rgba(108,142,239,0.4)]"
  const inactiveClassName = compact
    ? "rounded-full px-2 py-0.5 text-[10px] font-medium text-slate-400 transition-colors hover:text-slate-200"
    : "rounded-full px-3 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"

  return (
    <div className={containerClassName}>
      {(["log", "linear"] as const).map((option) => (
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
