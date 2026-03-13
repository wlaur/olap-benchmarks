import * as Select from "@radix-ui/react-select"
import { Check, ChevronDown, ChevronUp, Server } from "lucide-react"

interface SystemSelectorProps {
  systems: string[]
  selected: string | null
  onChange: (system: string) => void
  disabled?: boolean
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
    <Select.Root value={value} onValueChange={onChange} disabled={disabled}>
      <Select.Trigger
        aria-label="System"
        className="inline-flex max-w-[20rem] min-w-0 items-center justify-between gap-2 rounded-full bg-surface-raised py-1.5 pr-2.5 pl-2 text-left shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)] transition outline-none hover:shadow-[inset_0_0_0_1px_rgba(148,163,184,0.18)] focus:shadow-[inset_0_0_0_1px_rgba(108,142,239,0.6)] disabled:cursor-not-allowed disabled:opacity-60 data-[placeholder]:text-slate-500 sm:min-w-[14rem]"
      >
        <span className="flex min-w-0 flex-1 items-center gap-3">
          <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-accent-400/10 text-accent-300">
            <Server className="h-3.5 w-3.5" strokeWidth={1.8} />
          </span>
          <span className="flex min-w-0 flex-1 items-center gap-2 whitespace-nowrap">
            <span className="hidden shrink-0 text-[0.65rem] font-semibold tracking-wide text-slate-500 uppercase sm:inline">
              System
            </span>
            <span className="hidden h-3.5 w-px shrink-0 bg-slate-700/50 sm:inline" />
            <Select.Value className="block min-w-0 flex-1 truncate text-sm font-medium whitespace-nowrap text-slate-100" />
          </span>
        </span>
        <Select.Icon className="shrink-0 text-slate-500">
          <ChevronDown className="h-4 w-4" strokeWidth={1.8} />
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
            {systems.map((system) => (
              <Select.Item
                key={system}
                value={system}
                className="relative flex cursor-default items-center rounded-xl py-3 pr-9 pl-3 text-sm font-medium text-slate-200 transition outline-none data-[highlighted]:bg-accent-400/10 data-[highlighted]:text-accent-200 data-[state=checked]:bg-surface-raised data-[state=checked]:text-slate-50"
              >
                <span className="min-w-0 truncate">
                  <Select.ItemText>{system}</Select.ItemText>
                </span>
                <Select.ItemIndicator className="absolute right-3 inline-flex items-center text-accent-300">
                  <Check className="h-4 w-4" strokeWidth={2} />
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
