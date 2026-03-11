interface DatabaseLegendProps {
  databases: string[]
  databaseColors: Record<string, string>
}

export function DatabaseLegend({ databases, databaseColors }: DatabaseLegendProps) {
  return (
    <div className="flex flex-wrap gap-4">
      {databases.map((db) => (
        <span key={db} className="flex items-center gap-1.5 text-xs text-slate-300">
          <span
            className="inline-block size-2.5 rounded-full"
            style={{ backgroundColor: databaseColors[db] }}
          />
          {db}
        </span>
      ))}
    </div>
  )
}
