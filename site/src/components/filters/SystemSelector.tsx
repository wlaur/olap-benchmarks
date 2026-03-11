import { ChevronDown, Server } from "lucide-react"

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

  return (
    <div className="relative min-w-0">
      <label className="sr-only" htmlFor="system-selector">
        System
      </label>
      <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center gap-2 pl-3 text-slate-400">
        <Server className="h-4 w-4" strokeWidth={1.8} />
        <span className="text-xs font-semibold uppercase tracking-[0.18em]">
          System
        </span>
      </div>
      <select
        id="system-selector"
        className="min-w-[14rem] appearance-none rounded-full border border-slate-700/80 bg-slate-900/80 py-2 pr-10 pl-26 text-sm font-medium text-slate-100 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] outline-none transition focus:border-cyan-400 focus:bg-slate-900 disabled:cursor-not-allowed disabled:opacity-60"
        value={selected ?? ""}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
      >
        {systems.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </select>
      <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3 text-slate-500">
        <ChevronDown className="h-4 w-4" strokeWidth={1.8} />
      </div>
    </div>
  )
}
