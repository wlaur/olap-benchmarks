import {
  record,
  type ComparisonAnnotation,
  type ComparisonFamily,
  type ComparisonManifest,
} from "./comparison"

export interface IterationObservation {
  id: number
  queryName: string
  iteration: number | null
  role: string | null
  status: string
  resultStatus: string | null
  durationS: number | null
  rowCount: number | null
  hash: string | null
  hashVersion: string | null
  hashMissingReason: string | null
  skipReason: string | null
  error: string | null
}

export interface RunObservation {
  runId: number
  db: string
  version: string
  driver: string | null
  system: string
  suite: string
  scale: number
  status: string
  finishedAt: string
  snapshotId: number | null
  metadata: Record<string, unknown>
  snapshot: Record<string, unknown>
  error: string | null
  queries: Map<string, IterationObservation[]>
}

export type ValidationEvidence =
  | "peer hash"
  | "repeat-only"
  | "hash-limited"
  | "excluded"
  | "unknown"
  | "conflict"

export interface QueryObservation {
  queryName: string
  steps: IterationObservation[]
  warmup: number
  measured: number
  complete: boolean
  executionComplete: boolean
  medianS: number | null
  reasons: string[]
  evidence: ValidationEvidence
  evidenceReason: string
}

export interface FamilyEngineResult {
  key: string
  db: string
  label: string
  run: RunObservation | null
  annotations: ComparisonAnnotation[]
  registration: "registered" | "unimplemented" | "scale not registered"
  queries: QueryObservation[]
  eligible: boolean
  reasons: string[]
  score: number | null
  wins: number
  completed: number
  total: number
}

export function variantKey(run: Pick<RunObservation, "db" | "version" | "driver">): string {
  return JSON.stringify([run.db, run.version, run.driver])
}

export function matchingAnnotations(
  run: RunObservation | null,
  db: string,
  suite: string,
  scale: number,
  manifest: ComparisonManifest,
): ComparisonAnnotation[] {
  return manifest.annotations.filter(
    (annotation) =>
      annotation.db === db &&
      annotation.suite === suite &&
      (annotation.scale_factor === null || annotation.scale_factor === scale) &&
      (annotation.run_id === null || annotation.run_id === run?.runId),
  )
}

export function median(values: readonly number[]): number | null {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2
}

function string(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null
}

function repeatSignature(steps: readonly IterationObservation[]): string | null {
  if (
    steps.length < 2 ||
    steps.some(
      (step) =>
        step.status !== "completed" ||
        step.resultStatus !== "ok" ||
        step.hash === null ||
        step.hashVersion === null ||
        step.rowCount === null,
    )
  )
    return null
  const signatures = new Set(
    steps.map((step) => JSON.stringify([step.hashVersion, step.hash, step.rowCount])),
  )
  return signatures.size === 1 ? [...signatures][0]! : null
}

export function observeQuery(
  run: RunObservation,
  queryName: string,
  peers: readonly RunObservation[],
  manifest: ComparisonManifest,
): QueryObservation {
  const steps = run.queries.get(queryName) ?? []
  const profile = record(run.metadata.benchmark_profile)
  const expectedWarmup = profile.warmup_iterations
  const reasons: string[] = []
  const warmup = steps.filter(
    (step) => step.role === "warmup" && step.status === "completed" && step.resultStatus === "ok",
  ).length
  const measuredSteps = steps.filter(
    (step) => step.role === "measured" && step.status === "completed" && step.resultStatus === "ok",
  )
  if (
    typeof expectedWarmup !== "number" ||
    !Number.isInteger(expectedWarmup) ||
    expectedWarmup < 1 ||
    profile.measured_iterations !== 3
  )
    reasons.push("warmup + 3 measured profile unknown or different")
  if (
    warmup !== expectedWarmup ||
    measuredSteps.length !== 3 ||
    steps.length !== warmup + 3 ||
    new Set(steps.map((step) => step.iteration)).size !== steps.length ||
    steps.some((step) => step.iteration === null)
  )
    reasons.push("incomplete warmup or measured iterations")
  if (
    typeof expectedWarmup === "number" &&
    steps.some(
      (step) =>
        step.iteration === null ||
        step.iteration < 1 ||
        step.iteration > expectedWarmup + 3 ||
        step.role !== (step.iteration <= expectedWarmup ? "warmup" : "measured"),
    )
  )
    reasons.push("iteration indices or roles do not match the profile")
  if (
    steps.some(
      (step) => step.status !== "completed" || step.resultStatus !== "ok" || step.error !== null,
    )
  )
    reasons.push("non-success query status")
  if (
    measuredSteps.some(
      (step) => step.durationS === null || !Number.isFinite(step.durationS) || step.durationS <= 0,
    )
  )
    reasons.push("measured duration missing or invalid")
  const executionComplete = reasons.length === 0
  const exclusion = manifest.answer_comparison_exclusions.find(
    (entry) =>
      entry.suite === run.suite &&
      entry.query_name === queryName &&
      (entry.db === null || entry.db === run.db),
  )
  let evidence: ValidationEvidence = "unknown"
  let evidenceReason = "Answer hashes or comparable repetitions are missing."
  const signature = repeatSignature(steps)
  if (exclusion) {
    evidence = "excluded"
    evidenceReason = exclusion.reason
  } else if (
    steps.length > 1 &&
    steps.every((step) => step.hash !== null && step.hashVersion !== null) &&
    new Set(steps.map((step) => step.hashVersion)).size === 1 &&
    new Set(steps.map((step) => JSON.stringify([step.hash, step.rowCount]))).size > 1
  ) {
    evidence = "conflict"
    evidenceReason = "Recorded repetitions disagree in hash or row count."
  } else if (signature !== null) {
    evidence = "repeat-only"
    evidenceReason = "Recorded repetitions agree; no comparable peer hash established."
    const input = string(record(run.metadata.input).manifest_sha256)
    const comparable = peers.filter(
      (peer) =>
        peer.db !== run.db &&
        peer.system === run.system &&
        peer.suite === run.suite &&
        peer.scale === run.scale &&
        peer.status === "completed" &&
        (peer.metadata.invalidation === null || peer.metadata.invalidation === undefined) &&
        input !== null &&
        record(peer.metadata.input).manifest_sha256 === input &&
        !manifest.answer_comparison_exclusions.some(
          (entry) =>
            entry.suite === run.suite &&
            entry.query_name === queryName &&
            (entry.db === null || entry.db === peer.db),
        ),
    )
    const peerSignatures = comparable
      .map((peer) => repeatSignature(peer.queries.get(queryName) ?? []))
      .filter(
        (value): value is string =>
          value !== null && JSON.parse(value)[0] === steps[0]?.hashVersion,
      )
    if (peerSignatures.some((value) => value !== signature)) {
      evidence = "conflict"
      evidenceReason = "A peer with the same input fingerprint and hash version disagrees."
    } else if (peerSignatures.length > 0) {
      evidence = "peer hash"
      evidenceReason =
        "Repetitions and at least one other engine agree at the same input fingerprint and hash version."
    }
  } else if (steps.some((step) => step.hashMissingReason !== null)) {
    evidence = "hash-limited"
    evidenceReason = [
      ...new Set(steps.flatMap((step) => (step.hashMissingReason ? [step.hashMissingReason] : []))),
    ].join("; ")
  }
  if (evidence === "conflict") reasons.push("answer evidence conflict")
  return {
    queryName,
    steps,
    warmup,
    measured: measuredSteps.length,
    complete: reasons.length === 0,
    executionComplete,
    medianS: median(
      measuredSteps.flatMap((step) =>
        step.durationS !== null && Number.isFinite(step.durationS) && step.durationS > 0
          ? [step.durationS]
          : [],
      ),
    ),
    reasons,
    evidence,
    evidenceReason,
  }
}

export function compareFamily(
  family: ComparisonFamily,
  scale: number,
  system: string,
  runs: readonly RunObservation[],
  manifest: ComparisonManifest,
): FamilyEngineResult[] {
  const scoped = runs.filter(
    (run) => run.system === system && run.suite === family.suite && run.scale === scale,
  )
  const engines = [
    ...new Set([...manifest.registrations.map((entry) => entry.db), ...runs.map((run) => run.db)]),
  ].sort()
  const rows = engines.flatMap((db): FamilyEngineResult[] => {
    const variants = scoped.filter((run) => run.db === db)
    return (variants.length > 0 ? variants : [null]).map((run) => {
      const registrations = manifest.registrations.filter(
        (entry) =>
          entry.db === db && entry.suite === family.suite && entry.operations.includes("select"),
      )
      const registration =
        registrations.length === 0
          ? "unimplemented"
          : registrations.some((entry) => entry.scale_factors.includes(scale))
            ? "registered"
            : "scale not registered"
      const annotations = matchingAnnotations(run, db, family.suite, scale, manifest)
      const queries = run
        ? family.coverage_query_names.map((query) => observeQuery(run, query, scoped, manifest))
        : []
      const performance = queries.filter((query) => family.query_names.includes(query.queryName))
      const reasons: string[] = []
      if (!run)
        reasons.push(
          registration === "unimplemented"
            ? "SELECT adapter unimplemented in registration snapshot"
            : "no recorded run; execution unknown",
        )
      if (run && run.status !== "completed") reasons.push(`run ${run.status}`)
      if (run && run.metadata.invalidation !== null && run.metadata.invalidation !== undefined)
        reasons.push("run invalidated")
      if (run?.error) reasons.push("run error recorded")
      if (run && string(record(run.metadata.input).manifest_sha256) === null)
        reasons.push("input fingerprint unknown")
      if (annotations.some((entry) => entry.preparation === "incomplete"))
        reasons.push("documented incomplete preparation")
      if (run && performance.some((query) => !query.complete))
        reasons.push(
          `${performance.filter((query) => !query.complete).length}/${family.query_names.length} performance queries incomplete or blocked`,
        )
      if (performance.some((query) => query.evidence === "excluded"))
        reasons.push("required query has a documented answer-comparison exclusion")
      if (family.track === "smoke") reasons.push("smoke track; no performance ranking")
      return {
        key: run ? variantKey(run) : db,
        db,
        label: db,
        run,
        annotations,
        registration,
        queries,
        eligible: reasons.length === 0,
        reasons,
        score: null,
        wins: 0,
        completed: queries.filter((query) => query.executionComplete).length,
        total: family.coverage_query_names.length,
      }
    })
  })
  const inputFingerprints = new Set(
    rows
      .filter((row) => row.eligible)
      .map((row) => record(row.run!.metadata.input).manifest_sha256),
  )
  if (inputFingerprints.size > 1) {
    for (const row of rows.filter((entry) => entry.eligible)) {
      row.eligible = false
      row.reasons.push("publication participants have different input fingerprints")
    }
  }
  const eligible = rows.filter((row) => row.eligible)
  const fastest = new Map(
    family.query_names.map((queryName) => [
      queryName,
      Math.min(
        ...eligible.map(
          (row) => row.queries.find((query) => query.queryName === queryName)!.medianS!,
        ),
      ),
    ]),
  )
  for (const row of eligible) {
    const ratios = family.query_names.map((queryName) => {
      const duration = row.queries.find((query) => query.queryName === queryName)!.medianS!
      const best = fastest.get(queryName)!
      if (duration === best) row.wins += 1
      return duration / best
    })
    row.score = Math.exp(ratios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / ratios.length)
  }
  return rows.sort(
    (a, b) =>
      (a.score ?? Infinity) - (b.score ?? Infinity) ||
      a.db.localeCompare(b.db) ||
      a.key.localeCompare(b.key),
  )
}
