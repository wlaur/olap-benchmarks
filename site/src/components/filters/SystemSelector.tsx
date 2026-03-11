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
        className="inline-flex min-w-[12rem] max-w-[14rem] items-center justify-between gap-2 rounded-full border border-slate-700/80 bg-slate-900/80 py-1.5 pr-2.5 pl-2 text-left shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] outline-none transition hover:border-slate-500 focus:border-cyan-400 data-[placeholder]:text-slate-500 disabled:cursor-not-allowed disabled:opacity-60"
      >
        <span className="flex min-w-0 items-center gap-2.5">
          <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-cyan-400/10 text-cyan-300">
            <Server className="h-4 w-4" strokeWidth={1.8} />
          </span>
          <span className="flex min-w-0 items-center gap-2">
            <span className="shrink-0 text-[0.62rem] font-semibold uppercase tracking-[0.2em] text-slate-500">
              System
            </span>
            <Select.Value className="truncate text-sm font-medium text-slate-100" />
          </span>
        </span>
        <Select.Icon className="text-slate-500">
          <ChevronDown className="h-4 w-4" strokeWidth={1.8} />
        </Select.Icon>
      </Select.Trigger>

      <Select.Portal>
        <Select.Content
          position="popper"
          sideOffset={10}
          className="z-50 max-h-80 w-[var(--radix-select-trigger-width)] overflow-hidden rounded-3xl border border-slate-700 bg-slate-950/98 p-2 text-slate-100 shadow-2xl shadow-slate-950/60 backdrop-blur data-[side=bottom]:translate-y-1 data-[side=top]:-translate-y-1"
        >
          <Select.ScrollUpButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronUp className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollUpButton>
          <Select.Viewport className="space-y-1">
            {systems.map((system) => (
              <Select.Item
                key={system}
                value={system}
                className="relative flex cursor-default items-center rounded-2xl py-3 pr-9 pl-3 text-sm font-medium text-slate-200 outline-none transition data-[highlighted]:bg-cyan-400/12 data-[highlighted]:text-cyan-100 data-[state=checked]:bg-slate-900 data-[state=checked]:text-slate-50"
              >
                <span className="min-w-0 truncate">
                  <Select.ItemText>{system}</Select.ItemText>
                </span>
                <Select.ItemIndicator className="absolute right-3 inline-flex items-center text-cyan-300">
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
