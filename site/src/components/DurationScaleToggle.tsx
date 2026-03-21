import type { DurationScaleMode } from "../lib/format"
import { ControlGroup, SegmentedButton } from "./controls/Control"

interface DurationScaleToggleProps {
  mode: DurationScaleMode
  onChange: (mode: DurationScaleMode) => void
  compact?: boolean
}

export function DurationScaleToggle({ mode, onChange, compact = false }: DurationScaleToggleProps) {
  return (
    <ControlGroup compact={compact}>
      {(["linear", "log"] as const).map((option) => (
        <SegmentedButton
          key={option}
          onClick={() => onChange(option)}
          selected={mode === option}
          size={compact ? "xs" : "sm"}
        >
          {option === "log" ? "Log" : "Linear"}
        </SegmentedButton>
      ))}
    </ControlGroup>
  )
}
