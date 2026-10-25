const DATABASE_NAMES: Record<string, string> = {
  arc: "Arc",
  clickhouse: "ClickHouse",
  datafusion: "DataFusion",
  doris: "Doris",
  duckdb: "DuckDB",
  monetdb: "MonetDB",
  polars: "Polars",
  postgres: "PostgreSQL",
  questdb: "QuestDB",
  starrocks: "StarRocks",
  tdengine: "TDengine",
  timescaledb: "TimescaleDB",
}

export function formatDatabaseName(database: string): string {
  return DATABASE_NAMES[database] ?? database
}

export function formatSqlDialectName(dialect: string): string {
  return dialect === "base" ? "Base SQL" : formatDatabaseName(dialect)
}
