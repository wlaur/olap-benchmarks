import { fileURLToPath } from "node:url"

import { DATA_DIRECTORY, readResultsManifest, verifyResultsDatabase } from "./results-asset.mjs"

try {
  const manifest = await readResultsManifest(new URL("manifest.json", DATA_DIRECTORY))
  const format = manifest.results_db_format
  console.log(
    `verify-data: declared storage ${format.storage_compatibility_version}, minimum reader ${format.minimum_reader_engine_version}, writer ${format.writer_engine_version}; bun run test:reader verifies browser readability`,
  )
  const { size, sha256 } = await verifyResultsDatabase(
    fileURLToPath(new URL("results.db", DATA_DIRECTORY)),
    manifest,
  )
  console.log(`verify-data: results.db matches manifest (${size} bytes, sha256 ${sha256})`)
} catch (error) {
  console.error(`verify-data: ${error.message}`)
  process.exitCode = 1
}
