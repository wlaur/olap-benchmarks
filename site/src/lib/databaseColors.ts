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
  postgres: DATABASE_COLOR_PALETTE[3],
  questdb: DATABASE_COLOR_PALETTE[4],
  timescaledb: "#c084fc",
}

const FALLBACK_DATABASE_COLORS = DATABASE_COLOR_PALETTE.slice(
  Object.keys(KNOWN_DATABASE_COLORS).length,
)

export function getDatabaseColors(databases: string[]): Record<string, string> {
  const databaseColors = { ...KNOWN_DATABASE_COLORS }
  const unknownDatabases = databases
    .filter((database) => !Object.hasOwn(KNOWN_DATABASE_COLORS, database))
    .sort()

  for (const [index, database] of unknownDatabases.entries()) {
    databaseColors[database] =
      FALLBACK_DATABASE_COLORS[index % FALLBACK_DATABASE_COLORS.length] ??
      DATABASE_COLOR_PALETTE[index % DATABASE_COLOR_PALETTE.length]!
  }

  return databaseColors
}
