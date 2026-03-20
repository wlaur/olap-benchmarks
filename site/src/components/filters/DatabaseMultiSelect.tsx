import { cn } from "../../lib/cn"
import { controlChipClass } from "../controls/controlStyles"

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
              className={cn("gap-1.5", controlChipClass(selected, "sm"))}
              style={selected ? { boxShadow: `inset 0 0 0 1px ${color}55` } : undefined}
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
        className={cn("shrink-0", controlChipClass(allSelected, "sm"))}
        onClick={onSelectAll}
      >
        All
      </button>
    </div>
  )
}
