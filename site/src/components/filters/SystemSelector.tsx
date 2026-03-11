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
        className="inline-flex max-w-[22rem] min-w-[16rem] items-center justify-between gap-3 rounded-full bg-slate-900/80 py-2 pr-2.5 pl-2.5 text-left shadow-[inset_0_0_0_1px_rgba(51,65,85,0.8),inset_0_1px_0_rgba(255,255,255,0.04)] transition outline-none hover:shadow-[inset_0_0_0_1px_rgba(100,116,139,0.95),inset_0_1px_0_rgba(255,255,255,0.04)] focus:shadow-[inset_0_0_0_1px_rgba(34,211,238,0.9),inset_0_1px_0_rgba(255,255,255,0.04)] disabled:cursor-not-allowed disabled:opacity-60 data-[placeholder]:text-slate-500"
      >
        <span className="flex min-w-0 flex-1 items-center gap-3">
          <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-cyan-400/10 text-cyan-300">
            <Server className="h-4 w-4" strokeWidth={1.8} />
          </span>
          <span className="flex min-w-0 flex-1 items-center gap-2.5 whitespace-nowrap">
            <span className="shrink-0 text-[0.7rem] font-semibold tracking-[0.22em] text-slate-500 uppercase">
              System
            </span>
            <span className="h-4 w-px shrink-0 bg-slate-700" />
            <Select.Value className="block min-w-0 flex-1 truncate text-[1.05rem] font-medium whitespace-nowrap text-slate-100" />
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
                className="relative flex cursor-default items-center rounded-2xl py-3 pr-9 pl-3 text-sm font-medium text-slate-200 transition outline-none data-[highlighted]:bg-cyan-400/12 data-[highlighted]:text-cyan-100 data-[state=checked]:bg-slate-900 data-[state=checked]:text-slate-50"
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
