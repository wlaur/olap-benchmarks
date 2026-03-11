import * as duckdb from "@duckdb/duckdb-wasm"
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url"
import duckdbWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url"

let dbInstance: duckdb.AsyncDuckDB | null = null
let connInstance: duckdb.AsyncDuckDBConnection | null = null

const BASE = import.meta.env.BASE_URL

export async function getDB(): Promise<duckdb.AsyncDuckDB> {
  if (dbInstance) return dbInstance

  const worker = new Worker(duckdbWorker, { type: "module" })
  const logger = new duckdb.VoidLogger()
  const db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(duckdbWasm)

  await db.registerFileURL(
    "results.duckdb",
    `${BASE}data/results.duckdb`,
    duckdb.DuckDBDataProtocol.HTTP,
    false,
  )

  const conn = await db.connect()
  await conn.query(`ATTACH 'results.duckdb' AS results (READ_ONLY)`)
  await conn.close()

  dbInstance = db
  return db
}

export async function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  if (connInstance) return connInstance
  const db = await getDB()
  connInstance = await db.connect()
  return connInstance
}

export async function query<T>(sql: string): Promise<T[]> {
  const conn = await getConnection()
  const result = await conn.query(sql)
  return result.toArray().map((row) => row.toJSON() as T)
}
