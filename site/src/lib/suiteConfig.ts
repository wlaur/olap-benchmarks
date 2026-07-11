import type { BenchmarkDefinition, BenchmarkSuiteId } from "./benchmarks"
import type { BenchmarkOperation } from "./types"

export interface ParsedQueryName {
  queryId: string
  queryLabel: string
  tableFamily: string
}

export interface SuiteConfig {
  id: BenchmarkSuiteId
  label: string
  defaultScaleFactor: number
  /** Key into queries.json, which is keyed by suite source directory name. */
  queriesKey: string
  operations: BenchmarkOperation[]
  parseQueryName: (queryName: string) => ParsedQueryName
  compareQueryNames: (left: string, right: string) => number
}

function parseTimeSeriesQueryName(queryName: string): ParsedQueryName {
  const match = /^(?<table>[a-z]+)_(?<queryId>\d+)_(?<description>.+)$/.exec(queryName)
  if (!match?.groups) {
    return {
      queryId: "00",
      queryLabel: toTitleCase(queryName.replace(/_/g, " ")),
      tableFamily: "Unknown",
    }
  }

  const queryId = match.groups.queryId ?? "00"
  const description = match.groups.description ?? queryName
  const table = match.groups.table ?? "unknown"

  return {
    queryId,
    queryLabel: toTitleCase(description.replace(/_/g, " ")),
    tableFamily: formatTimeSeriesTableFamily(table),
  }
}

function formatTimeSeriesTableFamily(value: string): string {
  if (value === "wide") return "Wide"
  if (value === "tall") return "Tall"
  if (value === "large") return "Large"
  return toTitleCase(value)
}

function compareTimeSeriesQueryNames(left: string, right: string): number {
  const leftMeta = parseTimeSeriesQueryName(left)
  const rightMeta = parseTimeSeriesQueryName(right)
  const tableDelta = leftMeta.tableFamily.localeCompare(rightMeta.tableFamily)
  if (tableDelta !== 0) return tableDelta

  const queryIdDelta =
    Number.parseInt(leftMeta.queryId, 10) - Number.parseInt(rightMeta.queryId, 10)
  if (queryIdDelta !== 0) return queryIdDelta

  const labelDelta = leftMeta.queryLabel.localeCompare(rightMeta.queryLabel)
  if (labelDelta !== 0) return labelDelta

  return left.localeCompare(right)
}

function parseGenericQueryName(queryName: string): ParsedQueryName {
  return {
    queryId: "00",
    queryLabel: toTitleCase(queryName.replace(/_/g, " ")),
    tableFamily: "General",
  }
}

function compareGenericQueryNames(left: string, right: string): number {
  return left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" })
}

export function getSuiteConfig(definition: BenchmarkDefinition): SuiteConfig {
  const parser =
    definition.queryNameParser === "time_series"
      ? {
          parseQueryName: parseTimeSeriesQueryName,
          compareQueryNames: compareTimeSeriesQueryNames,
        }
      : {
          parseQueryName: parseGenericQueryName,
          compareQueryNames: compareGenericQueryNames,
        }

  return {
    id: definition.id,
    queriesKey: definition.queriesKey,
    label: definition.title,
    defaultScaleFactor: definition.defaultScaleFactor,
    operations: definition.operations,
    ...parser,
  }
}

export const METRIC_SAMPLE_RATE_S = 2

function toTitleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toUpperCase())
}
