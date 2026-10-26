import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises"
import { fileURLToPath } from "node:url"

import { DuckDBInstance } from "@duckdb/node-api"
import { expect, test, type Page } from "@playwright/test"

import { assertResultsReaderCompatible, parseResultsDbFormat } from "../../src/lib/resultsFormat"

const FIXTURE_ROOT = fileURLToPath(
  new URL("../../../data/duckdb2-upgrade/site-reader-fixtures/", import.meta.url),
)
const FORMAT = {
  storage_compatibility_version: "v1.4.0",
  minimum_reader_engine_version: "v1.4.0",
  writer_engine_version: "2.0.0",
}
const EVIDENCE_SQL = `select id::varchar as id, metadata::varchar as metadata,
  recorded_at::varchar as recorded_at, query_text, failure
  from reader_evidence order by id`

let fixtureDirectory: string
let fixture: Buffer
let nativeFixture: Buffer
let expectedRows: Record<string, unknown>[]

test.beforeAll(async () => {
  await mkdir(FIXTURE_ROOT, { recursive: true })
  fixtureDirectory = await mkdtemp(`${FIXTURE_ROOT}reader-`)
  const path = `${fixtureDirectory}/compatible.db`
  const instance = await DuckDBInstance.create()
  try {
    const connection = await instance.connect()
    try {
      const version = await connection.runAndReadAll("select version() as version")
      expect(version.getRowObjects()).toEqual([{ version: "v2.0.0" }])
      await connection.run(`attach '${path.replaceAll("'", "''")}' as published
        (storage_version 'v1.4.0')`)
      await connection.run(`create table published.reader_evidence (
        id bigint, metadata json, recorded_at timestamp, query_text varchar, failure varchar
      )`)
      await connection.run(`insert into published.reader_evidence values
        (9007199254740993, '{"engine":"duckdb","version":"1.5.6","nested":[1,null,"λ"]}',
          timestamp '2026-10-19 16:47:49.871698', 'SELECT count(*) FROM source', null),
        (42, '{"status":"failed","reason":"unsupported"}', null,
          'SELECT missing_function()', 'Binder Error: missing_function does not exist')`)
      const rows = await connection.runAndReadAll(
        EVIDENCE_SQL.replace("reader_evidence", "published.reader_evidence"),
      )
      expectedRows = rows.getRowObjects()
      await connection.run(`attach '${path.replaceAll("'", "''")}.native' as native
        (storage_version 'v2.0.0')`)
      await connection.run(`create table native.reader_evidence as
        select * from published.reader_evidence`)
      await connection.run("detach native")
      await connection.run("detach published")
    } finally {
      connection.closeSync()
    }
  } finally {
    instance.closeSync()
  }
  fixture = await readFile(path)
  nativeFixture = await readFile(`${path}.native`)
})

test.afterAll(async () => {
  if (fixtureDirectory) await rm(fixtureDirectory, { recursive: true, force: true })
})

async function openReaderPage(page: Page): Promise<void> {
  await page.goto("data/manifest.json")
}

async function declareFormat(page: Page, format: unknown): Promise<void> {
  await page.route("**/data/manifest.json", async (route) => {
    const response = await route.fetch()
    const manifest = await response.json()
    await route.fulfill({ json: { ...manifest, results_db_format: format } })
  })
}

async function readerError(page: Page): Promise<string> {
  return page.evaluate(async () => {
    const modulePath = "/olap-benchmarks/src/lib/duckdb.ts"
    const { getDuckDb } = await import(modulePath)
    try {
      await getDuckDb()
      return "Reader unexpectedly opened results"
    } catch (error) {
      return String(error)
    }
  })
}

test("published results open in the pinned browser engine", async ({ page }) => {
  await openReaderPage(page)
  const runtime = await page.evaluate(async () => {
    const modulePath = "/olap-benchmarks/src/lib/duckdb.ts"
    const { getDuckDb } = await import(modulePath)
    const database = await getDuckDb()
    const connection = await database.connect()
    try {
      const result = await connection.query(`select version() as version,
        (select count(*)::integer from run) as runs,
        (select count(*)::integer from query_execution) as executions`)
      return {
        apiVersion: await database.getVersion(),
        rows: result.toArray().map((row: { toJSON(): unknown }) => row.toJSON()),
      }
    } finally {
      await connection.close()
    }
  })
  expect(runtime.apiVersion).toBe("v1.5.4")
  expect(runtime.rows).toEqual([
    { version: "v1.5.4", runs: expect.any(Number), executions: expect.any(Number) },
  ])
  expect(runtime.rows[0].runs).toBeGreaterThan(0)
  expect(runtime.rows[0].executions).toBeGreaterThan(0)
})

test("DuckDB 2.0 v1.4-compatible storage preserves evidence in the browser", async ({ page }) => {
  await declareFormat(page, FORMAT)
  await page.route("**/data/comparison.json", (route) => route.fulfill({ json: { version: 1 } }))
  await page.route("**/data/results.db", (route) =>
    route.fulfill({
      body: fixture,
      contentType: "application/octet-stream",
    }),
  )
  await openReaderPage(page)
  const actual = await page.evaluate(async (sql) => {
    const modulePath = "/olap-benchmarks/src/lib/duckdb.ts"
    const { getDuckDb } = await import(modulePath)
    const database = await getDuckDb()
    const connection = await database.connect()
    try {
      const result = await connection.query(sql)
      return result.toArray().map((row: { toJSON(): unknown }) => row.toJSON())
    } finally {
      await connection.close()
    }
  }, EVIDENCE_SQL)
  expect(actual).toEqual(expectedRows)
})

test("unsupported reader contract fails before requesting the database", async ({ page }) => {
  let databaseRequests = 0
  page.on("request", (request) => {
    if (request.url().endsWith("/data/results.db")) databaseRequests += 1
  })
  await declareFormat(page, {
    ...FORMAT,
    storage_compatibility_version: "v2.0.0",
    minimum_reader_engine_version: "v2.0.0",
  })
  await openReaderPage(page)
  expect(await readerError(page)).toContain(
    "These results need a newer viewer (requires DuckDB v2.0.0, viewer uses v1.5.4)",
  )
  expect(databaseRequests).toBe(0)
})

test("unsupported results show an actionable error in the results page", async ({ page }) => {
  const pageErrors: string[] = []
  page.on("pageerror", (error) => pageErrors.push(error.message))
  await declareFormat(page, {
    ...FORMAT,
    storage_compatibility_version: "v2.0.0",
    minimum_reader_engine_version: "v2.0.0",
  })
  await page.goto("#/")
  await expect(
    page.getByRole("alert").filter({ hasText: "Failed to load results:" }),
  ).toContainText("These results need a newer viewer")
  expect(pageErrors).toEqual([])
})

test("native 2.0 storage cannot masquerade as a compatible publication", async ({ page }) => {
  await declareFormat(page, FORMAT)
  await page.route("**/data/comparison.json", (route) => route.fulfill({ json: { version: 1 } }))
  await page.route("**/data/results.db", (route) =>
    route.fulfill({
      body: nativeFixture,
      contentType: "application/octet-stream",
    }),
  )
  await openReaderPage(page)
  const error = await readerError(page)
  expect(error).toMatch(/version|storage|serializ|magic/i)
  expect(error).not.toContain("Reader unexpectedly opened results")
})

for (const [label, format] of [
  ["missing", undefined],
  ["malformed", null],
] as const) {
  test(`${label} format declaration fails explicitly`, async ({ page }) => {
    await declareFormat(page, format)
    await openReaderPage(page)
    expect(await readerError(page)).toContain("manifest.json has no valid results_db_format")
  })
}

test("format contract validates engine versions numerically", () => {
  expect(() => assertResultsReaderCompatible(FORMAT, "v1.10.0")).not.toThrow()
  expect(() => assertResultsReaderCompatible(FORMAT, "v1.3.9")).toThrow("newer viewer")
  expect(() => parseResultsDbFormat({ ...FORMAT, writer_engine_version: "2.0.0-dev1" })).toThrow(
    "Invalid results engine version",
  )
  expect(() =>
    parseResultsDbFormat({ ...FORMAT, minimum_reader_engine_version: "v1.3.0" }),
  ).toThrow("inconsistent results_db_format")
})
