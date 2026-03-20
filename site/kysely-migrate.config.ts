import { fileURLToPath } from "node:url"

import { DuckDbDialect } from "@20chan/kysely-duckdb"
import duckdb from "duckdb"
import { Kysely } from "kysely"
import { defineConfig, postgresDefinitions } from "kysely-migrate"
import ts from "typescript"

const RESULTS_DB_PATH = fileURLToPath(new URL("./public/data/results.db", import.meta.url))

const numberType = ts.factory.createKeywordTypeNode(ts.SyntaxKind.NumberKeyword)
const stringType = ts.factory.createKeywordTypeNode(ts.SyntaxKind.StringKeyword)
const booleanType = ts.factory.createKeywordTypeNode(ts.SyntaxKind.BooleanKeyword)

export default defineConfig(async () => ({
  db: new Kysely({
    dialect: new DuckDbDialect({
      database: new duckdb.Database(RESULTS_DB_PATH, {
        access_mode: "READ_ONLY",
      }),
      tableMappings: {},
    }),
  }),
  migrationFolder: ".",
  codegen: {
    dialect: "postgres",
    definitions: {
      ...postgresDefinitions,
      INTEGER: numberType,
      VARCHAR: stringType,
      TIMESTAMP: postgresDefinitions.timestamp,
      JSON: postgresDefinitions.json,
      FLOAT: numberType,
      DOUBLE: numberType,
      BOOLEAN: booleanType,
    },
    out: "src/lib/generated/db.ts",
  },
}))
