import type { DurationScaleMode } from "../lib/format"
import { controlButtonClass, controlGroupClass } from "./controls/controlStyles"

interface DurationScaleToggleProps {
  mode: DurationScaleMode
  onChange: (mode: DurationScaleMode) => void
  compact?: boolean
}

export function DurationScaleToggle({ mode, onChange, compact = false }: DurationScaleToggleProps) {
  return (
    <div className={controlGroupClass(compact)}>
      {(["linear", "log"] as const).map((option) => (
        <button
          key={option}
          onClick={() => onChange(option)}
          className={controlButtonClass(mode === option, compact ? "xs" : "sm")}
        >
          {option === "log" ? "Log" : "Linear"}
        </button>
      ))}
    </div>
  )
}
