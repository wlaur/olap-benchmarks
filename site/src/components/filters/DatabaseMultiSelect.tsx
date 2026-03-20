import { cn } from "../../lib/cn"

interface DatabaseMultiSelectProps {
  databases: string[]
  selectedDatabases: string[]
  databaseColors: Record<string, string>
  onSelectAll: () => void
  onToggleDatabase: (database: string) => void
}

export function DatabaseMultiSelect({
  databases,
  selectedDatabases,
  databaseColors,
  onSelectAll,
  onToggleDatabase,
}: DatabaseMultiSelectProps) {
  const allSelected = selectedDatabases.length === databases.length

  return (
    <div className="flex items-center gap-2">
      <div className="flex flex-wrap items-center gap-1.5">
        {databases.map((database) => {
          const selected = selectedDatabases.includes(database)
          const color = databaseColors[database] ?? "#94a3b8"
          return (
            <button
              key={database}
              type="button"
              className={cn(
                "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition",
                selected
                  ? "border-transparent text-slate-100"
                  : "border-border-default bg-surface-inset text-slate-500 hover:text-slate-300",
              )}
              style={
                selected
                  ? { backgroundColor: `${color}20`, boxShadow: `inset 0 0 0 1px ${color}40` }
                  : undefined
              }
              onClick={() => onToggleDatabase(database)}
            >
              <span
                className="size-2 rounded-full"
                style={{ backgroundColor: selected ? color : "rgba(71, 85, 105, 0.6)" }}
              />
              {database}
            </button>
          )
        })}
      </div>
      <button
        type="button"
        className={cn(
          "shrink-0 rounded-full border px-2.5 py-1 text-xs font-medium transition",
          allSelected
            ? "border-accent-400/30 bg-accent-500/10 text-accent-200"
            : "border-border-default text-slate-400 hover:text-slate-200",
        )}
        onClick={onSelectAll}
      >
        All
      </button>
    </div>
  )
}
