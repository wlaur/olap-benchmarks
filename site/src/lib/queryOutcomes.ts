import type { QueryObservation, RunObservation } from "./observations"

export interface QueryOutcome {
  kind:
    | "measured"
    | "partial"
    | "timeout"
    | "error"
    | "unsupported"
    | "skipped"
    | "not-run"
    | "excluded"
    | "conflict"
    | "blocked"
  label: string
  detail: string
}

export function queryOutcomeSummary(
  rows: readonly { valueMs: number | null; outcome: QueryOutcome }[],
): string {
  if (rows.length === 0) return "No selected variants"
  const recorded = rows.filter((row) => row.valueMs !== null).length
  if (rows.length === 1)
    return `1 selected variant · ${recorded} recorded ${recorded === 1 ? "median" : "medians"} · no comparison`
  const usable = rows.filter(
    (row) => row.valueMs !== null && row.outcome.kind === "measured",
  ).length
  const counts = new Map<string, number>()
  for (const { outcome } of rows) {
    if (outcome.kind !== "measured") {
      const label = outcome.label.toLowerCase()
      counts.set(label, (counts.get(label) ?? 0) + 1)
    }
  }
  return `${recorded} recorded medians · ${usable} usable${[...counts].map(([label, count]) => ` · ${count} ${label}`).join("")}`
}

export function describeQueryOutcome(
  query: QueryObservation | undefined,
  run: RunObservation | undefined,
  comparable: boolean,
  runProblems: readonly string[],
): QueryOutcome {
  if (!query || query.steps.length === 0)
    return { kind: "not-run", label: "Not run", detail: "No execution recorded for this query." }
  if (query.evidence === "conflict")
    return { kind: "conflict", label: "Answer conflict", detail: query.evidenceReason }
  for (const [status, kind, label] of [
    ["wrong_result", "conflict", "Wrong result"],
    ["timeout", "timeout", "Timed out"],
    ["error", "error", "Error"],
    ["unsupported", "unsupported", "Unsupported"],
    ["skipped", "skipped", "Skipped"],
  ] as const) {
    const step = query.steps.find((entry) => entry.resultStatus === status)
    if (step) return { kind, label, detail: step.error ?? step.skipReason ?? label }
  }
  if (runProblems.length > 0 || run?.status !== "completed")
    return {
      kind: "blocked",
      label: "Run blocked",
      detail: runProblems.join("; ") || "The run did not complete.",
    }
  if (!comparable || query.evidence === "excluded")
    return {
      kind: "excluded",
      label: "Excluded",
      detail: query.evidenceReason || "Outside the comparable query set.",
    }
  if (!query.complete)
    return {
      kind: "partial",
      label: "Incomplete",
      detail: query.reasons.join("; ") || "Required measurement iterations are incomplete.",
    }
  return {
    kind: "measured",
    label: "Measured",
    detail: "Complete recorded query measurements; validation details are available.",
  }
}
