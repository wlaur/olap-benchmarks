import { createHash } from "node:crypto"
import { createReadStream } from "node:fs"
import { readFile, stat } from "node:fs/promises"
import { fileURLToPath } from "node:url"

const RESULTS_DB_PATH = fileURLToPath(new URL("../public/data/results.db", import.meta.url))
const MANIFEST_PATH = fileURLToPath(new URL("../public/data/manifest.json", import.meta.url))

function fail(message) {
  console.error(`verify-data: ${message}`)
  process.exit(1)
}

const manifest = JSON.parse(await readFile(MANIFEST_PATH, "utf8"))
const expectedSha256 = manifest.results_db_sha256
const expectedBytes = manifest.results_db_bytes

if (typeof expectedSha256 !== "string" || !/^[0-9a-f]{64}$/.test(expectedSha256)) {
  fail(`${MANIFEST_PATH} has no valid results_db_sha256; republish with olap publish`)
}
if (!Number.isSafeInteger(expectedBytes) || expectedBytes <= 0) {
  fail(`${MANIFEST_PATH} has no valid results_db_bytes; republish with olap publish`)
}

const actualBytes = await stat(RESULTS_DB_PATH).then(
  (stats) => stats.size,
  () => fail(`${RESULTS_DB_PATH} is missing; run bun run fetch-data`),
)
if (actualBytes !== expectedBytes) {
  fail(
    `${RESULTS_DB_PATH} is ${actualBytes} bytes, manifest expects ${expectedBytes}; the release asset does not match the committed manifest`,
  )
}

const hash = createHash("sha256")
for await (const chunk of createReadStream(RESULTS_DB_PATH)) {
  hash.update(chunk)
}
const actualSha256 = hash.digest("hex")
if (actualSha256 !== expectedSha256) {
  fail(
    `${RESULTS_DB_PATH} has SHA-256 ${actualSha256}, manifest expects ${expectedSha256}; the release asset does not match the committed manifest`,
  )
}

console.log(
  `verify-data: results.db matches manifest (${actualBytes} bytes, sha256 ${actualSha256})`,
)
