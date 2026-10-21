export interface ResultsDbFormat {
  storage_compatibility_version: string
  minimum_reader_engine_version: string
  writer_engine_version: string
}

function versionParts(version: unknown): number[] {
  if (typeof version !== "string" || !/^v?\d+\.\d+\.\d+$/.test(version)) {
    throw new Error(`Invalid results engine version: ${String(version)}`)
  }
  return version.replace(/^v/, "").split(".").map(Number)
}

function compareVersions(left: string, right: string): number {
  const a = versionParts(left)
  const b = versionParts(right)
  for (let index = 0; index < 3; index += 1) {
    const difference = a[index]! - b[index]!
    if (difference !== 0) return difference
  }
  return 0
}

export function parseResultsDbFormat(value: unknown): ResultsDbFormat {
  if (typeof value !== "object" || value === null) {
    throw new Error("manifest.json has no valid results_db_format; republish the results")
  }
  const format = value as ResultsDbFormat
  versionParts(format.storage_compatibility_version)
  versionParts(format.minimum_reader_engine_version)
  versionParts(format.writer_engine_version)
  if (
    compareVersions(format.minimum_reader_engine_version, format.storage_compatibility_version) <
      0 ||
    compareVersions(format.writer_engine_version, format.minimum_reader_engine_version) < 0
  ) {
    throw new Error("manifest.json has inconsistent results_db_format versions")
  }
  return format
}

export function assertResultsReaderCompatible(
  format: ResultsDbFormat,
  readerVersion: string,
): void {
  if (compareVersions(readerVersion, format.minimum_reader_engine_version) < 0) {
    throw new Error(
      `These results need a newer viewer (requires DuckDB ${format.minimum_reader_engine_version}, viewer uses ${readerVersion}). Update the viewer or republish compatible results.`,
    )
  }
}
