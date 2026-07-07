import { Scale } from "lucide-react"

import { ControlSelect, ControlSelectSkeleton } from "../controls/ControlSelect"

interface ScaleFactorSelectorProps {
  scaleFactors: number[]
  selected: number | null
  onChange: (scaleFactor: number) => void
  disabled?: boolean
}

export function ScaleFactorSelectorSkeleton() {
  return <ControlSelectSkeleton label="Scale" className="max-w-[10rem] sm:min-w-[8rem]" />
}

export function ScaleFactorSelector({
  scaleFactors,
  selected,
  onChange,
  disabled = false,
}: ScaleFactorSelectorProps) {
  if (scaleFactors.length === 0) return null

  const value = String(selected ?? scaleFactors[0]!)

  return (
    <ControlSelect
      ariaLabel="Scale factor"
      label="Scale"
      value={value}
      onChange={(nextValue) => onChange(Number(nextValue))}
      options={scaleFactors.map((scaleFactor) => ({
        value: String(scaleFactor),
        label: `SF ${scaleFactor}`,
      }))}
      icon={<Scale className="h-3 w-3" strokeWidth={1.8} />}
      disabled={disabled}
      className="max-w-[10rem] sm:min-w-[8rem]"
    />
  )
}
