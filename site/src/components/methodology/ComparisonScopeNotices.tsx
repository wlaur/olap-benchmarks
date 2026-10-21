import { useProfileScope } from "../../hooks/useProfileScope"
import { usePublication } from "../../hooks/usePublication"
import { NoticeStrip } from "../layout/Panel"

export function CampaignNotice({ className }: { className?: string }) {
  const { publication, error } = usePublication()
  if (error) {
    return (
      <NoticeStrip tone="warning" className={className}>
        Failed to load publication context: {error}
      </NoticeStrip>
    )
  }
  if (publication?.kind !== "campaign") return null

  return (
    <NoticeStrip tone="warning" className={className}>
      <span className="font-semibold text-slate-100">{publication.title}</span>:{" "}
      {publication.select_cohorts} completed SELECT cohorts; {publication.partial_select_cohorts}{" "}
      have unsupported, skipped or unmeasured queries. Missing queries are penalized. Populate,
      mutation and concurrency coverage may also be partial. Measurements span{" "}
      {publication.source_commits.length} source commits.{" "}
      <a
        href={publication.report_url}
        target="_blank"
        rel="noreferrer"
        className="text-accent-300 underline underline-offset-2 hover:text-accent-200"
      >
        Campaign report
      </a>{" "}
      with coverage, exclusions and provenance.
    </NoticeStrip>
  )
}

export function ProfileScopeNotice({ className }: { className?: string }) {
  const { scope, error } = useProfileScope()
  if (error) {
    return (
      <NoticeStrip tone="warning" className={className}>
        Failed to load benchmark profile: {error}
      </NoticeStrip>
    )
  }
  if (!scope?.active) return null

  const { id, warmupIterations, measuredIterations } = scope.active
  const otherRuns = scope.excluded.reduce((total, profile) => total + profile.runs, 0)

  return (
    <NoticeStrip tone={scope.excluded.length > 0 ? "warning" : "info"} className={className}>
      Benchmark profile <span className="font-mono text-slate-100">{id}</span>
      {warmupIterations !== null && measuredIterations !== null
        ? `: ${warmupIterations} unscored warm-up and ${measuredIterations} measured iterations per query`
        : null}
      . Latency is end-to-end client latency, from sending the query until the result is a Polars
      DataFrame, as the median of the measured iterations.
      {scope.excluded.length > 0
        ? ` ${otherRuns} select ${otherRuns === 1 ? "run" : "runs"} recorded under ${scope.excluded
            .map((profile) => profile.id)
            .join(", ")} ${otherRuns === 1 ? "is" : "are"} not compared with it.`
        : null}
    </NoticeStrip>
  )
}

export function SnapshotSpanWarning({
  snapshotCount,
  className,
}: {
  snapshotCount: number
  className?: string
}) {
  if (snapshotCount <= 1) return null

  return (
    <NoticeStrip tone="warning" className={className}>
      The compared runs span {snapshotCount} system snapshots of this system (operating system,
      Docker or hardware details changed between them), so their latencies may not be comparable.
    </NoticeStrip>
  )
}
