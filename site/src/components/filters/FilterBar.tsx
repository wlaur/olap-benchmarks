interface FilterBarProps {
  label: string
  options: string[]
  selected: string | null
  onChange: (value: string | null) => void
}

export function FilterBar({ label, options, selected, onChange }: FilterBarProps) {
  return (
    <div className="flex items-center gap-2">
      <label className="text-sm font-medium text-gray-400">{label}</label>
      <select
        className="rounded-md border border-gray-700 bg-gray-900 px-3 py-1.5 text-sm text-gray-100"
        value={selected ?? ""}
        onChange={(e) => onChange(e.target.value || null)}
      >
        <option value="">All</option>
        {options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    </div>
  )
}
