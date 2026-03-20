import { ControlChip } from "../controls/Control"

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
            <ControlChip
              key={database}
              className="gap-1.5"
              selected={selected}
              style={selected ? { boxShadow: `inset 0 0 0 1px ${color}55` } : undefined}
              onClick={() => onToggleDatabase(database)}
            >
              <span
                className="size-2 rounded-full"
                style={{ backgroundColor: selected ? color : "rgba(71, 85, 105, 0.6)" }}
              />
              {database}
            </ControlChip>
          )
        })}
      </div>
      <ControlChip className="shrink-0" selected={allSelected} onClick={onSelectAll}>
        All
      </ControlChip>
    </div>
  )
}
