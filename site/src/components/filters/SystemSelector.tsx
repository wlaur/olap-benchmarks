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
    <div className="flex items-center gap-2">
      <label className="text-sm font-medium text-gray-400">System</label>
      <select
        className="rounded-md border border-gray-700 bg-gray-900 px-3 py-1.5 text-sm text-gray-100"
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
    </div>
  )
}
