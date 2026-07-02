import { ChevronLeft, ChevronRight } from "lucide-react"
import type { ReactNode } from "react"

import { MetaLabel } from "../Typography"
import { QuietButton } from "./Control"

interface CycleSelectOption<T extends string> {
  value: T
  label: ReactNode
}

interface CycleSelectProps<T extends string> {
  label: string
  value: T
  options: CycleSelectOption<T>[]
  onChange: (value: T) => void
  ariaLabel?: string
}

export function CycleSelect<T extends string>({
  label,
  value,
  options,
  onChange,
  ariaLabel,
}: CycleSelectProps<T>) {
  if (options.length === 0) return null

  const currentIndex = Math.max(
    0,
    options.findIndex((option) => option.value === value),
  )
  const currentOption = options[currentIndex]!
  const previousOption = options[(currentIndex - 1 + options.length) % options.length]!
  const nextOption = options[(currentIndex + 1) % options.length]!
  const target = ariaLabel ?? label.toLowerCase()

  return (
    <div className="inline-flex items-center gap-3 rounded-full border border-border-default bg-surface-inset px-2 py-1">
      <QuietButton
        size="xs"
        aria-label={`Show previous ${target}`}
        onClick={() => onChange(previousOption.value)}
        className="size-7 rounded-full px-0"
      >
        <ChevronLeft className="size-3.5" />
      </QuietButton>
      <div className="min-w-[11.5rem] px-1 text-center">
        <MetaLabel className="tracking-[0.16em] text-slate-500">{label}</MetaLabel>
        <p className="mt-0.5 inline-flex items-center justify-center gap-1.5 text-sm font-medium text-slate-100">
          {currentOption.label}
        </p>
      </div>
      <QuietButton
        size="xs"
        aria-label={`Show next ${target}`}
        onClick={() => onChange(nextOption.value)}
        className="size-7 rounded-full px-0"
      >
        <ChevronRight className="size-3.5" />
      </QuietButton>
    </div>
  )
}
