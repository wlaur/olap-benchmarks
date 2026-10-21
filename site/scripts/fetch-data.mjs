import { createWriteStream } from "node:fs"
import { mkdtemp, rename, rm } from "node:fs/promises"
import { join } from "node:path"
import { pipeline } from "node:stream/promises"
import { fileURLToPath } from "node:url"

import { DATA_DIRECTORY, readResultsManifest, verifyResultsDatabase } from "./results-asset.mjs"

export async function fetchData({
  dataDirectory = fileURLToPath(DATA_DIRECTORY),
  fetchImpl = fetch,
} = {}) {
  const manifest = await readResultsManifest(join(dataDirectory, "manifest.json"))
  const assetName = `results-${manifest.results_db_sha256}.db`
  const temporaryDirectory = await mkdtemp(join(dataDirectory, ".results-download-"))
  const partialPath = join(temporaryDirectory, "results.db.part")
  try {
    const response = await fetchImpl(
      `https://github.com/wlaur/olap-benchmarks/releases/download/data/${assetName}`,
    )
    if (!response.ok || !response.body) {
      throw new Error(`Failed to download ${assetName}: HTTP ${response.status}`)
    }
    await pipeline(response.body, createWriteStream(partialPath, { flags: "wx" }))
    await verifyResultsDatabase(partialPath, manifest)
    await rename(partialPath, join(dataDirectory, "results.db"))
    return assetName
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true })
  }
}

if (import.meta.main) {
  try {
    const assetName = await fetchData()
    console.log(`fetch-data: verified ${assetName} and saved as public/data/results.db`)
  } catch (error) {
    console.error(`fetch-data: ${error.message}`)
    process.exitCode = 1
  }
}
