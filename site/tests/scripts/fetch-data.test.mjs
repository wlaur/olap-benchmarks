import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

import { fetchData } from "../../scripts/fetch-data.mjs"
import { readResultsManifest, verifyResultsDatabase } from "../../scripts/results-asset.mjs"

const content = "verified database bytes"
const oldContent = "existing good local database"
const sha256 = createHash("sha256").update(content).digest("hex")
const manifest = {
  results_db_sha256: sha256,
  results_db_bytes: Buffer.byteLength(content),
  results_db_format: {
    storage_compatibility_version: "v1.4.0",
    minimum_reader_engine_version: "v1.4.0",
    writer_engine_version: "v2.0.0",
  },
}

let dataDirectory

beforeEach(async () => {
  dataDirectory = await mkdtemp(join(tmpdir(), "olap-fetch-test-"))
  await writeFile(join(dataDirectory, "manifest.json"), JSON.stringify(manifest))
  await writeFile(join(dataDirectory, "results.db"), oldContent)
})

afterEach(async () => {
  await rm(dataDirectory, { recursive: true, force: true })
})

async function expectOldFilePreserved() {
  expect(await readFile(join(dataDirectory, "results.db"), "utf8")).toBe(oldContent)
  expect((await readdir(dataDirectory)).sort()).toEqual(["manifest.json", "results.db"])
}

describe("manifest-pinned results download", () => {
  test("fetches only the expected hash asset and replaces the local name after verification", async () => {
    const requests = []
    const fetchImpl = async (url) => {
      requests.push(url)
      expect(await readFile(join(dataDirectory, "results.db"), "utf8")).toBe(oldContent)
      return new Response(content)
    }
    expect(await fetchData({ dataDirectory, fetchImpl })).toBe(`results-${sha256}.db`)
    expect(requests).toEqual([
      `https://github.com/wlaur/olap-benchmarks/releases/download/data/results-${sha256}.db`,
    ])
    expect(await readFile(join(dataDirectory, "results.db"), "utf8")).toBe(content)
    expect((await readdir(dataDirectory)).sort()).toEqual(["manifest.json", "results.db"])
    const loadedManifest = await readResultsManifest(join(dataDirectory, "manifest.json"))
    expect(await verifyResultsDatabase(join(dataDirectory, "results.db"), loadedManifest)).toEqual({
      size: manifest.results_db_bytes,
      sha256,
    })
  })

  test.each([
    ["HTTP failure", () => new Response("not found", { status: 404 }), "HTTP 404"],
    ["wrong size", () => new Response("truncated"), "bytes"],
    ["wrong hash", () => new Response("x".repeat(content.length)), "SHA-256"],
    ["network failure", () => Promise.reject(new Error("network failed")), "network failed"],
    [
      "interrupted body",
      () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("partial"))
              controller.error(new Error("body interrupted"))
            },
          }),
        ),
      "body interrupted",
    ],
  ])(
    "preserves the local database and cleans its partial download on %s",
    async (_, respond, message) => {
      await expect(fetchData({ dataDirectory, fetchImpl: async () => respond() })).rejects.toThrow(
        message,
      )
      await expectOldFilePreserved()
    },
  )

  test.each([
    ["missing hash", { ...manifest, results_db_sha256: undefined }],
    ["invalid hash", { ...manifest, results_db_sha256: "../results.db" }],
    ["missing size", { ...manifest, results_db_bytes: undefined }],
    ["invalid size", { ...manifest, results_db_bytes: -1 }],
    ["missing format", { ...manifest, results_db_format: undefined }],
    [
      "inconsistent format",
      {
        ...manifest,
        results_db_format: {
          ...manifest.results_db_format,
          minimum_reader_engine_version: "v1.3.0",
        },
      },
    ],
    ["invalid object", []],
    ["null", null],
  ])("rejects %s before any fetch, with no legacy fallback", async (_, value) => {
    await writeFile(join(dataDirectory, "manifest.json"), JSON.stringify(value))
    let requests = 0
    const fetchImpl = async () => {
      requests += 1
      throw new Error("Unexpected network request")
    }
    await expect(fetchData({ dataDirectory, fetchImpl })).rejects.toThrow()
    expect(requests).toBe(0)
    await expectOldFilePreserved()
  })

  test("rejects malformed or missing manifest before any fetch", async () => {
    const fetchImpl = async () => {
      throw new Error("Unexpected network request")
    }
    await writeFile(join(dataDirectory, "manifest.json"), "{")
    await expect(fetchData({ dataDirectory, fetchImpl })).rejects.toBeInstanceOf(SyntaxError)
    await expectOldFilePreserved()
    await rm(join(dataDirectory, "manifest.json"))
    await expect(fetchData({ dataDirectory, fetchImpl })).rejects.toThrow("ENOENT")
    expect(await readFile(join(dataDirectory, "results.db"), "utf8")).toBe(oldContent)
  })
})
