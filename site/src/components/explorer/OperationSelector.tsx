import { ChevronLeft, ChevronRight } from "lucide-react"

import { QuietButton } from "../controls/Control"
import { MetaLabel } from "../Typography"

type QueryGroupOperation = "select" | "mutate"

interface OperationSelectorProps {
  operation: QueryGroupOperation
  onChange: (operation: QueryGroupOperation) => void
}

const OPERATIONS: QueryGroupOperation[] = ["mutate", "select"]

export function OperationSelector({ operation, onChange }: OperationSelectorProps) {
  const currentIndex = OPERATIONS.indexOf(operation)
  const previousOperation = OPERATIONS[(currentIndex - 1 + OPERATIONS.length) % OPERATIONS.length]!
  const nextOperation = OPERATIONS[(currentIndex + 1) % OPERATIONS.length]!
  const label = operation === "mutate" ? "Mutate queries" : "Select queries"

  return (
    <div className="inline-flex items-center gap-3 rounded-full border border-border-default bg-surface-inset px-2 py-1">
      <QuietButton
        size="xs"
        aria-label="Show previous query group"
        onClick={() => onChange(previousOperation)}
        className="size-7 rounded-full px-0"
      >
        <ChevronLeft className="size-3.5" />
      </QuietButton>
      <div className="min-w-[11.5rem] px-1 text-center">
        <MetaLabel className="tracking-[0.16em] text-slate-500">Query Group</MetaLabel>
        <p className="mt-0.5 text-sm font-medium text-slate-100">{label}</p>
      </div>
      <QuietButton
        size="xs"
        aria-label="Show next query group"
        onClick={() => onChange(nextOperation)}
        className="size-7 rounded-full px-0"
      >
        <ChevronRight className="size-3.5" />
      </QuietButton>
    </div>
  )
}
