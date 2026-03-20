import { Server } from "lucide-react"

import { ControlSelect, ControlSelectSkeleton } from "../controls/ControlSelect"

interface SystemSelectorProps {
  systems: string[]
  selected: string | null
  onChange: (system: string) => void
  disabled?: boolean
}

export function SystemSelectorSkeleton() {
  return <ControlSelectSkeleton label="System" className="max-w-[18rem] sm:min-w-[12.5rem]" />
}

export function SystemSelector({
  systems,
  selected,
  onChange,
  disabled = false,
}: SystemSelectorProps) {
  if (systems.length === 0) return null

  const value = selected ?? systems[0]!

  return (
    <ControlSelect
      ariaLabel="System"
      label="System"
      value={value}
      onChange={onChange}
      options={systems.map((system) => ({ value: system, label: system }))}
      icon={<Server className="h-3 w-3" strokeWidth={1.8} />}
      disabled={disabled}
      className="max-w-[18rem] sm:min-w-[12.5rem]"
    />
  )
}
