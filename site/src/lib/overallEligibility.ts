import { record, type ComparisonManifest, type PartialCertificate } from "./comparison"
import {
  runPerformanceProblems,
  type FamilyEngineResult,
  type RunObservation,
} from "./observations"

export type OverallContext = ComparisonManifest | { version: 1 }

function matchesCertificate(cert: PartialCertificate, run: RunObservation): boolean {
  const cell = cert.cell
  return (
    cert.selected_run_id === run.runId &&
    cell.system === run.system &&
    cell.db === run.db &&
    cell.db_version === run.version &&
    cell.db_driver === run.driver &&
    cell.suite === run.suite &&
    cell.scale_factor === run.scale &&
    cell.benchmark_profile === record(run.metadata.benchmark_profile).id &&
    run.status === "failed" &&
    Boolean(run.finishedAt)
  )
}

export function validateSelectedCertificates(
  manifest: ComparisonManifest,
  runs: readonly RunObservation[],
): void {
  if (manifest.version === 1) return
  for (const cert of manifest.certificates) {
    const selected = runs.filter((run) => run.runId === cert.selected_run_id)
    if (selected.length !== 1 || !matchesCertificate(cert, selected[0]!))
      throw new Error(`Partial-run certificate does not match selected run ${cert.selected_run_id}`)
  }
}

export function overallRunEligibility(row: FamilyEngineResult, manifest: OverallContext): string[] {
  const run = row.run!
  const blockers = runPerformanceProblems(run, row.annotations)
  if (manifest.version === 1) return blockers
  const cert = manifest.certificates.find((value) => value.selected_run_id === run.runId)
  if (!cert) return blockers
  if (!matchesCertificate(cert, run))
    throw new Error(`Partial-run certificate does not match selected run ${run.runId}`)
  if (
    row.queries.some((query) => query.evidence === "conflict") ||
    [...run.queries.values()].some((steps) =>
      steps.some((step) => step.resultStatus === "wrong_result"),
    )
  )
    throw new Error(`Certified run ${run.runId} contains wrong answers or answer conflicts`)
  return blockers.filter((reason) => reason !== "run failed" && reason !== "run error recorded")
}
