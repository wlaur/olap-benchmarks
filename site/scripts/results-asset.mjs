import { createHash } from "node:crypto"
import { createReadStream } from "node:fs"
import { readFile, stat } from "node:fs/promises"

import { parseResultsDbFormat } from "../src/lib/resultsFormat.ts"

export const DATA_DIRECTORY = new URL("../public/data/", import.meta.url)

export async function readResultsManifest(manifestPath) {
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"))
  if (typeof manifest !== "object" || manifest === null || Array.isArray(manifest)) {
    throw new Error("manifest.json has no valid results manifest")
  }
  parseResultsDbFormat(manifest.results_db_format)
  if (
    typeof manifest.results_db_sha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(manifest.results_db_sha256)
  ) {
    throw new Error("manifest.json has no valid results_db_sha256; republish with olap publish")
  }
  if (!Number.isSafeInteger(manifest.results_db_bytes) || manifest.results_db_bytes <= 0) {
    throw new Error("manifest.json has no valid results_db_bytes; republish with olap publish")
  }
  return manifest
}

export async function verifyResultsDatabase(databasePath, manifest) {
  const { size } = await stat(databasePath)
  if (size !== manifest.results_db_bytes) {
    throw new Error(
      `results.db is ${size} bytes, manifest expects ${manifest.results_db_bytes}; the release asset does not match the committed manifest`,
    )
  }
  const hash = createHash("sha256")
  for await (const chunk of createReadStream(databasePath)) {
    hash.update(chunk)
  }
  const sha256 = hash.digest("hex")
  if (sha256 !== manifest.results_db_sha256) {
    throw new Error(
      `results.db has SHA-256 ${sha256}, manifest expects ${manifest.results_db_sha256}; the release asset does not match the committed manifest`,
    )
  }
  return { size, sha256 }
}
