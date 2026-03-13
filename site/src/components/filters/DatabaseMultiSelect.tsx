import { cn } from "../../lib/cn"
import { BodyText, MetaLabel } from "../Typography"

interface DatabaseMultiSelectProps {
  databases: string[]
  selectedDatabases: string[]
  onSelectAll: () => void
  onToggleDatabase: (database: string) => void
}

export function DatabaseMultiSelect({
  databases,
  selectedDatabases,
  onSelectAll,
  onToggleDatabase,
}: DatabaseMultiSelectProps) {
  const allSelected = selectedDatabases.length === databases.length

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <MetaLabel>Included Databases</MetaLabel>
          <BodyText className="mt-1">
            Filter the comparison, charts, and SQL overrides to the databases you care about.
          </BodyText>
        </div>
        <button
          type="button"
          className={cn(
            "rounded-full border px-3 py-1.5 text-xs font-medium transition",
            allSelected
              ? "border-cyan-400/40 bg-cyan-400/10 text-cyan-200"
              : "border-slate-700 bg-slate-900 text-slate-300 hover:border-slate-500 hover:text-slate-100",
          )}
          onClick={onSelectAll}
        >
          All
        </button>
      </div>

      <div className="flex flex-wrap gap-2">
        {databases.map((database) => {
          const selected = selectedDatabases.includes(database)
          return (
            <button
              key={database}
              type="button"
              className={cn(
                "rounded-full border px-3 py-1.5 text-sm font-medium transition",
                selected
                  ? "border-cyan-400/40 bg-cyan-400/10 text-cyan-100"
                  : "border-slate-700 bg-slate-900 text-slate-300 hover:border-slate-500 hover:text-slate-100",
              )}
              onClick={() => onToggleDatabase(database)}
            >
              {database}
            </button>
          )
        })}
      </div>
    </div>
  )
}
