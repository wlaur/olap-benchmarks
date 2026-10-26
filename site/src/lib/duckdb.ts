import { DuckDbDialect } from "@coji/kysely-duckdb-wasm"
import * as duckdb from "@duckdb/duckdb-wasm"
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url"
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url"
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url"
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url"
import { Kysely } from "kysely"

import { fetchComparisonDocument, parseComparisonManifest, record } from "./comparison"
import type { DB } from "./generated/db"
import { assertResultsReaderCompatible, parseResultsDbFormat } from "./resultsFormat"
import { fetchResultsManifest } from "./resultsManifest"

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

  try {
    await database.instantiate(bundle.mainModule, bundle.pthreadWorker)
    const manifest = await fetchResultsManifest()
    const format = parseResultsDbFormat(manifest.results_db_format)
    assertResultsReaderCompatible(format, await database.getVersion())
    const document = await fetchComparisonDocument()
    if (record(document).version !== 1) {
      const comparison = parseComparisonManifest(document, manifest.results_db_sha256)
      const response = await fetch(`${BASE}data/results.db`)
      if (!response.ok) throw new Error(`Failed to load certified results.db: ${response.status}`)
      const bytes = await response.arrayBuffer()
      const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))
      const digest = [...hash].map((value) => value.toString(16).padStart(2, "0")).join("")
      if (digest !== comparison.results_db_sha256 || bytes.byteLength !== manifest.results_db_bytes)
        throw new Error("Certified results.db bytes do not match the publication binding")
      await database.registerFileBuffer(RESULTS_DB_FILE, new Uint8Array(bytes))
    } else {
      await database.registerFileURL(
        RESULTS_DB_FILE,
        `${BASE}data/results.db`,
        duckdb.DuckDBDataProtocol.HTTP,
        false,
      )
    }
    await database.open({
      path: RESULTS_DB_FILE,
      accessMode: duckdb.DuckDBAccessMode.READ_ONLY,
    })
  } catch (error) {
    await database.terminate()
    throw error
  }

  return database
}

export function getDuckDb(): Promise<duckdb.AsyncDuckDB> {
  if (!duckDbPromise) {
    duckDbPromise = initDuckDb()
  }
  return duckDbPromise
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
