import * as duckdb from "@duckdb/duckdb-wasm"
import { DuckDbDialect } from "@coji/kysely-duckdb-wasm"
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url"
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url"
import { Kysely } from "kysely"
import type { DB } from "./generated/db"

const BASE = import.meta.env.BASE_URL
const RESULTS_DB_FILE = "results.duckdb"

let duckDbInstance: duckdb.AsyncDuckDB | null = null
let kyselyInstance: Kysely<DB> | null = null

export async function getDuckDb(): Promise<duckdb.AsyncDuckDB> {
  if (duckDbInstance) return duckDbInstance

  const worker = new Worker(duckdbEhWorker)
  const logger = new duckdb.VoidLogger()
  const database = new duckdb.AsyncDuckDB(logger, worker)

  await database.instantiate(duckdbEhWasm)
  await database.registerFileURL(
    RESULTS_DB_FILE,
    `${BASE}data/results.duckdb`,
    duckdb.DuckDBDataProtocol.HTTP,
    false,
  )
  await database.open({
    path: RESULTS_DB_FILE,
    accessMode: duckdb.DuckDBAccessMode.READ_ONLY,
  })

  duckDbInstance = database
  return database
}

export async function getKyselyDb(): Promise<Kysely<DB>> {
  if (kyselyInstance) return kyselyInstance

  kyselyInstance = new Kysely<DB>({
    dialect: new DuckDbDialect({
      database: getDuckDb,
      tableMappings: {},
    }),
  })

  return kyselyInstance
}
