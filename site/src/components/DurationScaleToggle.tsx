import type { DurationScaleMode } from "../lib/format"

interface DurationScaleToggleProps {
  mode: DurationScaleMode
  onChange: (mode: DurationScaleMode) => void
}

export function DurationScaleToggle({ mode, onChange }: DurationScaleToggleProps) {
  return (
    <div className="inline-flex rounded-full border border-slate-800 bg-slate-950/80 p-1">
      {(["log", "linear"] as const).map((option) => (
        <button
          key={option}
          onClick={() => onChange(option)}
          className={
            mode === option
              ? "rounded-full bg-cyan-400/10 px-3 py-1 text-xs font-medium text-cyan-200 shadow-[inset_0_0_0_1px_rgba(34,211,238,0.5)]"
              : "rounded-full px-3 py-1 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200"
          }
        >
          {option === "log" ? "Log" : "Linear"}
        </button>
      ))}
    </div>
  )
}
