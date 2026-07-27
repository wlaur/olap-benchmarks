import { DuckDbDialect } from "@coji/kysely-duckdb-wasm"
import * as duckdb from "@duckdb/duckdb-wasm"
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url"
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url"
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url"
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url"
import { Kysely } from "kysely"

import type { DB } from "./generated/db"

const BASE = import.meta.env.BASE_URL
const RESULTS_DB_FILE = "results.db"
const DUCKDB_BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: duckdbMvpWasm,
    mainWorker: duckdbMvpWorker,
  },
  eh: {
    mainModule: duckdbEhWasm,
    mainWorker: duckdbEhWorker,
  },
}

export type ResultsDb = Kysely<DB>

let duckDbPromise: Promise<duckdb.AsyncDuckDB> | null = null
let kyselyInstance: ResultsDb | null = null

async function initDuckDb(): Promise<duckdb.AsyncDuckDB> {
  const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES)
  const worker = new Worker(bundle.mainWorker!)
  const logger = new duckdb.VoidLogger()
  const database = new duckdb.AsyncDuckDB(logger, worker)

  await database.instantiate(bundle.mainModule, bundle.pthreadWorker)
  await database.registerFileURL(
    RESULTS_DB_FILE,
    `${BASE}data/results.db`,
    duckdb.DuckDBDataProtocol.HTTP,
    false,
  )
  await database.open({
    path: RESULTS_DB_FILE,
    accessMode: duckdb.DuckDBAccessMode.READ_ONLY,
  })

  return database
}

export function getDuckDb(): Promise<duckdb.AsyncDuckDB> {
  if (!duckDbPromise) {
    duckDbPromise = initDuckDb()
  }
  return duckDbPromise
}

// Start DuckDB initialization immediately at module load
getDuckDb()

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
