const DATABASE_COLOR_PALETTE = [
  "#38bdf8",
  "#f97316",
  "#34d399",
  "#facc15",
  "#ec4899",
  "#a78bfa",
  "#fb7185",
  "#2dd4bf",
  "#60a5fa",
  "#f59e0b",
  "#4ade80",
  "#c084fc",
] as const

const KNOWN_DATABASE_COLORS: Record<string, string> = {
  clickhouse: DATABASE_COLOR_PALETTE[0],
  duckdb: DATABASE_COLOR_PALETTE[1],
  monetdb: DATABASE_COLOR_PALETTE[2],
  polars: DATABASE_COLOR_PALETTE[5],
  postgres: DATABASE_COLOR_PALETTE[3],
  questdb: DATABASE_COLOR_PALETTE[4],
  starrocks: DATABASE_COLOR_PALETTE[6],
  timescaledb: "#c084fc",
}

const FALLBACK_DATABASE_COLORS = DATABASE_COLOR_PALETTE.slice(
  Object.keys(KNOWN_DATABASE_COLORS).length,
)

// Database identities from the query layer are variant labels: the engine
// name, suffixed with the db version when several versions are present.
export function databaseEngine(dbLabel: string): string {
  return dbLabel.split(" ")[0]!
}

function shadeColor(hex: string, amount: number): string {
  const value = Number.parseInt(hex.slice(1), 16)
  const shift = (channel: number) => Math.min(255, Math.max(0, Math.round(channel + amount * 255)))
  const r = shift((value >> 16) & 0xff)
  const g = shift((value >> 8) & 0xff)
  const b = shift(value & 0xff)
  return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, "0")}`
}

export function getDatabaseColors(databases: string[]): Record<string, string> {
  const engineColors = { ...KNOWN_DATABASE_COLORS }
  const unknownEngines = [...new Set(databases.map(databaseEngine))]
    .filter((engine) => !Object.hasOwn(engineColors, engine))
    .sort()

  for (const [index, engine] of unknownEngines.entries()) {
    engineColors[engine] =
      FALLBACK_DATABASE_COLORS[index % FALLBACK_DATABASE_COLORS.length] ??
      DATABASE_COLOR_PALETTE[index % DATABASE_COLOR_PALETTE.length]!
  }

  const variantsPerEngine = new Map<string, string[]>()
  for (const database of [...databases].sort()) {
    const engine = databaseEngine(database)
    const variants = variantsPerEngine.get(engine) ?? []
    variants.push(database)
    variantsPerEngine.set(engine, variants)
  }

  const databaseColors: Record<string, string> = { ...engineColors }
  for (const [engine, variants] of variantsPerEngine) {
    const base = engineColors[engine]!
    for (const [index, variant] of variants.entries()) {
      // shade successive versions of the same engine apart
      databaseColors[variant] =
        index === 0 ? base : shadeColor(base, index % 2 === 1 ? 0.22 : -0.24)
    }
  }

  return databaseColors
}
