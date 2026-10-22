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
      <span className="font-semibold text-slate-100">{publication.title}</span> includes{" "}
      {publication.select_cohorts} query runs, of which {publication.partial_select_cohorts} are
      partial. Coverage and comparison rules are shown with each result.{" "}
      <a
        href={publication.report_url}
        target="_blank"
        rel="noreferrer"
        className="text-accent-300 underline underline-offset-2 hover:text-accent-200"
      >
        Coverage &amp; exclusions
      </a>
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
      <span className="font-mono text-slate-100">{id}</span>
      {warmupIterations !== null && measuredIterations !== null
        ? `: ${warmupIterations} warmup + ${measuredIterations} measured fetches per query`
        : null}
      . Reported medians include the time spent transferring and converting results.
      {scope.excluded.length > 0
        ? ` We exclude ${otherRuns} ${otherRuns === 1 ? "run" : "runs"} from other profiles.`
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
      These runs span {snapshotCount} system snapshots with changes to the OS, Docker or hardware,
      which may affect the comparison.
    </NoticeStrip>
  )
}
