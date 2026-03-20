export function DatabaseLegend({
  databases,
  databaseColors,
}: {
  databases: string[]
  databaseColors: Record<string, string>
}) {
  return (
    <div className="flex flex-wrap gap-x-3 gap-y-1.5">
      {databases.map((db) => (
        <span key={db} className="flex items-center gap-1.5 text-xs text-slate-400">
          <span
            className="inline-block size-2 rounded-full"
            style={{ backgroundColor: databaseColors[db] }}
          />
          {db}
        </span>
      ))}
    </div>
  )
}
