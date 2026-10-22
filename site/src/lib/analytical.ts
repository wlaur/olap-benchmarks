export const ANALYTICAL_PROFILE = "analytical-streams-v1"

export interface AnalyticalRunRow {
  id: number
  suite: string
  suite_scale_factor: number
  system: string
  db: string
  db_version: string
  db_driver: string | null
  status: string
  finished_at: string | null
  error_message: string | null
  run_metadata: string | null
}

export interface AnalyticalPercentile {
  valueMs: number | null
  status: "available" | "insufficient_samples" | "unknown"
  minimumN: number
}

export interface AnalyticalSummary {
  offered: number | null
  completed: number | null
  errors: number | null
  timeouts: number | null
  qps: number | null
  samples: number | null
  percentiles: AnalyticalPercentile[]
}

export interface AnalyticalQueryEvidence {
  name: string
  hash: string | null
  skip: string | null
  manifestStatus: "selected" | "documented skip" | "unknown"
  summary: AnalyticalSummary
}

export interface AnalyticalRun {
  row: AnalyticalRunRow
  clients: number | null
  requestedSeconds: number | null
  elapsedSeconds: number | null
  startedAt: string | null
  finishedAt: string | null
  drainSeconds: number | null
  overall: AnalyticalSummary
  queries: AnalyticalQueryEvidence[]
  canonicalCount: number | null
  selectedCount: number | null
  skipCount: number | null
  manifestStatus: "full" | "documented skips" | "incomplete" | "unknown"
  budget: {
    scope: string | null
    shared: boolean | null
    threads: number | null
    engineBytes: number | null
    rssBytes: number | null
    rssScope: string | null
    effective: Record<string, unknown> | null
  }
  workerRuntime: Record<string, unknown> | null
  limitations: string[]
  recorded: Record<string, unknown> | null
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null
}

function nonnegative(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null
}

function count(value: unknown): number | null {
  const number = nonnegative(value)
  return number !== null && Number.isSafeInteger(number) ? number : null
}

function positive(value: unknown): number | null {
  const number = nonnegative(value)
  return number !== null && number > 0 ? number : null
}

function hash(value: unknown): string | null {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value) ? value : null
}

function timestamp(value: unknown): string | null {
  const string = text(value)
  return string !== null && Number.isFinite(Date.parse(string)) ? string : null
}

export function parseAnalyticalSummary(
  value: unknown,
  elapsedSeconds: number | null,
): AnalyticalSummary {
  const summary = record(value)
  let offered = count(summary?.offered)
  let completed = count(summary?.completed)
  let errors = count(summary?.errors)
  let timeouts = count(summary?.timeouts)
  if (
    offered !== null &&
    ((completed ?? 0) + (errors ?? 0) + (timeouts ?? 0) > offered ||
      (completed !== null &&
        errors !== null &&
        timeouts !== null &&
        offered !== completed + errors + timeouts))
  ) {
    offered = completed = errors = timeouts = null
  }
  const recordedQps = nonnegative(summary?.throughput_per_second)
  const expectedQps =
    completed !== null && elapsedSeconds !== null && elapsedSeconds > 0
      ? completed / elapsedSeconds
      : null
  const qps =
    recordedQps !== null &&
    expectedQps !== null &&
    Number.isFinite(expectedQps) &&
    Math.abs(recordedQps - expectedQps) <= expectedQps * 1e-6
      ? recordedQps
      : null
  const latency = record(summary?.latency_ms)
  const n = count(latency?.n)
  const samples = n !== null && completed !== null && n === completed ? n : null
  const percentiles = [5, 100, 1000].map((required, index): AnalyticalPercentile => {
    const percentile = record(latency?.[`p${[50, 95, 99][index]}`])
    const minimum = count(percentile?.minimum_n)
    const minimumN = minimum !== null && minimum >= required ? minimum : required
    const valueMs = nonnegative(percentile?.value_ms)
    if (
      samples !== null &&
      minimum !== null &&
      minimum >= required &&
      latency?.percentile_method === "nearest_rank"
    ) {
      if (percentile?.status === "insufficient_samples" && samples < minimumN) {
        return { valueMs: null, status: "insufficient_samples", minimumN }
      }
      if (percentile?.status === "available" && samples >= minimumN && valueMs !== null) {
        return { valueMs, status: "available", minimumN }
      }
    }
    return { valueMs: null, status: "unknown", minimumN }
  })
  return { offered, completed, errors, timeouts, qps, samples, percentiles }
}

export function parseAnalyticalRun(
  row: AnalyticalRunRow,
  canonical: readonly string[] | null,
): AnalyticalRun {
  let metadata: Record<string, unknown> | null = null
  try {
    metadata = record(JSON.parse(row.run_metadata ?? "null"))
  } catch {
    metadata = null
  }
  const recorded = record(metadata?.analytical)
  const supported = recorded?.version === 1 && recorded.track === ANALYTICAL_PROFILE
  const analytical = supported ? recorded : null
  const limitations: string[] = []
  if (!supported)
    limitations.push("Analytical metadata missing, invalid or unsupported; measurements unknown.")
  const clientCount = count(analytical?.clients)
  const clients = clientCount !== null && [1, 4, 16].includes(clientCount) ? clientCount : null
  const requestedSeconds = positive(analytical?.duration_seconds)
  const elapsedSeconds = positive(analytical?.actual_elapsed_seconds)
  const overall = parseAnalyticalSummary(analytical?.overall, elapsedSeconds)
  if (clients === null) limitations.push("Client count unknown; expected 1, 4 or 16.")
  if (requestedSeconds === null || elapsedSeconds === null)
    limitations.push("Requested or actual measurement window unknown.")
  if ([overall.offered, overall.completed, overall.errors, overall.timeouts].includes(null)) {
    limitations.push("Overall request counts missing or inconsistent.")
  }
  if (overall.qps === null)
    limitations.push(
      "QPS unknown: recorded rate, completed count or actual window missing or inconsistent.",
    )
  const manifest = record(analytical?.query_manifest)
  const files = record(manifest?.files)
  const validManifest =
    files !== null &&
    Object.keys(files).length > 0 &&
    manifest?.algorithm === "sha256" &&
    hash(manifest.digest) !== null &&
    Object.entries(files).every(([name, digest]) => text(name) !== null && hash(digest) !== null)
  const excluded = record(analytical?.excluded_queries)
  const skips = new Map<string, string>()
  for (const [name, value] of Object.entries(excluded ?? {})) {
    const skip = record(value)
    if ((skip?.status === "skipped" || skip?.status === "unsupported") && text(skip.reason))
      skips.set(name, `${skip!.status}: ${skip!.reason}`)
  }
  const perQuery = record(analytical?.per_query)
  const names = [
    ...new Set([
      ...(canonical ?? []),
      ...Object.keys(files ?? {}),
      ...Object.keys(excluded ?? {}),
      ...Object.keys(perQuery ?? {}),
    ]),
  ].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
  const queries = names.map((name): AnalyticalQueryEvidence => ({
    name,
    hash: hash(files?.[name]),
    skip: skips.get(name) ?? null,
    manifestStatus:
      validManifest && Object.hasOwn(files!, name) && !Object.hasOwn(excluded ?? {}, name)
        ? "selected"
        : skips.has(name) && !Object.hasOwn(files ?? {}, name)
          ? "documented skip"
          : "unknown",
    summary: parseAnalyticalSummary(perQuery?.[name], elapsedSeconds),
  }))
  let manifestStatus: AnalyticalRun["manifestStatus"] = "unknown"
  if (canonical !== null && canonical.length > 0 && validManifest && excluded !== null) {
    const accounted =
      canonical.every(
        (name) => queries.find((query) => query.name === name)?.manifestStatus !== "unknown",
      ) &&
      Object.keys(files!).every((name) => canonical.includes(name)) &&
      Object.keys(excluded).every((name) => canonical.includes(name) && skips.has(name))
    manifestStatus = accounted ? (skips.size > 0 ? "documented skips" : "full") : "incomplete"
  }
  if (manifestStatus === "unknown" || manifestStatus === "incomplete") {
    limitations.push(
      "Full canonical manifest coverage unknown or incomplete; a selected subset is not a full-suite comparison.",
    )
  }
  if (skips.size > 0)
    limitations.push(
      "Documented skips change the issued workload; inspect per-query evidence before comparing engines.",
    )
  const budget = record(analytical?.budget)
  const shared =
    typeof budget?.multiplied_by_clients === "boolean" ? !budget.multiplied_by_clients : null
  const effective = record(budget?.effective)
  if (
    budget?.configured_threads !== 10 ||
    budget.configured_engine_memory_bytes !== 24 * 1024 ** 3 ||
    budget.client_rss_guard_bytes !== 28 * 1024 ** 3
  ) {
    limitations.push(
      "Configured budget unknown or differs from this profile’s fixed 10 threads, 24 GiB engine budget and 28 GiB operation RSS guard.",
    )
  }
  if (
    shared !== true ||
    text(budget?.scope) === null ||
    effective === null ||
    Object.keys(effective).length === 0
  ) {
    limitations.push(
      "Effective shared budget evidence missing, invalid or not shared; configured limits alone do not establish enforcement.",
    )
  }
  const workerRuntime = record(analytical?.worker_runtime)
  if (
    clients === null ||
    Array.from({ length: clients }, (_, index) => index).some((index) => {
      const runtime = record(workerRuntime?.[String(index)])
      return runtime === null || Object.keys(runtime).length === 0
    })
  )
    limitations.push("Per-worker runtime evidence missing or incomplete.")
  if (record(metadata?.invalidation))
    limitations.push(
      "Run has recorded invalidation metadata; inspect the source before using measurements.",
    )
  if (analytical?.cancelled === true)
    limitations.push("Run was cancelled; completed requests are partial evidence.")
  return {
    row,
    clients,
    requestedSeconds,
    elapsedSeconds,
    startedAt: timestamp(analytical?.measurement_started_at),
    finishedAt: timestamp(analytical?.measurement_finished_at),
    drainSeconds: nonnegative(analytical?.drain_seconds),
    overall,
    queries,
    canonicalCount: canonical?.length || null,
    selectedCount: validManifest ? Object.keys(files!).length : null,
    skipCount: excluded !== null && skips.size === Object.keys(excluded).length ? skips.size : null,
    manifestStatus,
    budget: {
      scope: text(budget?.scope),
      shared,
      threads: positive(budget?.configured_threads),
      engineBytes: positive(budget?.configured_engine_memory_bytes),
      rssBytes: positive(budget?.client_rss_guard_bytes),
      rssScope: text(budget?.client_rss_scope),
      effective,
    },
    workerRuntime,
    limitations,
    recorded,
  }
}
