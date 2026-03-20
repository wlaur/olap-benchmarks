import * as Select from "@radix-ui/react-select"
import { Check, ChevronDown, ChevronUp, FlaskConical } from "lucide-react"

import { benchmarkDefinitions, type BenchmarkSuiteId } from "../../lib/benchmarks"

interface SuiteSelectorProps {
  selected: BenchmarkSuiteId
  onChange: (suite: BenchmarkSuiteId) => void
}

const selectorTriggerClass =
  "inline-flex max-w-[18rem] min-w-0 items-center justify-between gap-1.5 rounded-full bg-surface-raised/92 py-1 pr-2 pl-1.5 text-left shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] transition outline-none hover:bg-surface-raised hover:shadow-[inset_0_0_0_1px_rgba(120,154,214,0.18)] focus:shadow-[inset_0_0_0_1px_rgba(90,151,255,0.52),0_0_0_1px_rgba(90,151,255,0.2)] sm:min-w-[12.5rem]"

export function SuiteSelector({ selected, onChange }: SuiteSelectorProps) {
  return (
    <Select.Root value={selected} onValueChange={(value) => onChange(value as BenchmarkSuiteId)}>
      <Select.Trigger aria-label="Suite" className={selectorTriggerClass}>
        <span className="flex min-w-0 flex-1 items-center gap-2">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-sky-400/12 text-sky-300 shadow-[inset_0_0_0_1px_rgba(125,211,252,0.14)]">
            <FlaskConical className="h-3 w-3" strokeWidth={1.8} />
          </span>
          <span className="flex min-w-0 flex-1 items-center gap-2 whitespace-nowrap">
            <span className="hidden shrink-0 text-[0.6rem] font-semibold tracking-[0.18em] text-slate-500 uppercase md:inline">
              Suite
            </span>
            <span className="hidden h-3 w-px shrink-0 bg-slate-700/50 md:inline" />
            <Select.Value className="block min-w-0 flex-1 truncate text-[0.8125rem] font-medium whitespace-nowrap text-slate-100" />
          </span>
        </span>
        <Select.Icon className="shrink-0 text-slate-500">
          <ChevronDown className="h-3.5 w-3.5" strokeWidth={1.8} />
        </Select.Icon>
      </Select.Trigger>

      <Select.Portal>
        <Select.Content
          position="popper"
          sideOffset={10}
          className="z-50 max-h-80 w-[var(--radix-select-trigger-width)] overflow-hidden rounded-2xl border border-border-default bg-surface-primary/98 p-2 text-slate-100 shadow-2xl shadow-black/40 backdrop-blur data-[side=bottom]:translate-y-1 data-[side=top]:-translate-y-1"
        >
          <Select.ScrollUpButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronUp className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollUpButton>
          <Select.Viewport className="space-y-1">
            {benchmarkDefinitions.map((benchmark) => (
              <Select.Item
                key={benchmark.id}
                value={benchmark.id}
                className="relative flex cursor-default items-center rounded-xl py-2.5 pr-8 pl-3 text-[0.8125rem] font-medium text-slate-200 transition outline-none data-[highlighted]:bg-sky-500/10 data-[highlighted]:text-sky-100 data-[state=checked]:bg-surface-raised data-[state=checked]:text-slate-50"
              >
                <span className="min-w-0 truncate">
                  <Select.ItemText>{benchmark.navLabel}</Select.ItemText>
                </span>
                <Select.ItemIndicator className="absolute right-3 inline-flex items-center text-sky-300">
                  <Check className="h-3.5 w-3.5" strokeWidth={2} />
                </Select.ItemIndicator>
              </Select.Item>
            ))}
          </Select.Viewport>
          <Select.ScrollDownButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronDown className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollDownButton>
        </Select.Content>
      </Select.Portal>
    </Select.Root>
  )
}
