import { useState } from "react"
import { useNavigate } from "react-router-dom"

import { useHomeOverview } from "../../hooks/useHomeOverview"
import { useAppStore } from "../../stores/useAppStore"
import { ControlChip, QuietButton, SegmentedButton } from "../controls/Control"
import { ChartFrame, NoticeStrip, PanelCard, PanelHeader } from "../layout/Panel"
import {
  CampaignNotice,
  ProfileScopeNotice,
  SnapshotSpanWarning,
} from "../methodology/ComparisonScopeNotices"
import { EngineComparison } from "../methodology/EngineComparison"
import { Skeleton } from "../Skeleton"
import { BodyText, MetaLabel, SectionTitle } from "../Typography"

export function HomeScoreTable() {
  const navigate = useNavigate()
  const selectedSystem = useAppStore((state) => state.selectedSystem)
  const systemLoading = useAppStore((state) => state.systemLoading)
  const systemError = useAppStore((state) => state.systemError)
  const overview = useHomeOverview(selectedSystem)
  const [familyId, setFamilyId] = useState("")
  const [selectedScale, setSelectedScale] = useState<number | null>(null)
  const [hidden, setHidden] = useState<string[]>([])
  const active =
    overview.families.find((entry) => entry.family.id === familyId) ??
    overview.families.find((entry) => entry.family.track === "raw") ??
    overview.families[0]
  const cell = active?.scales.find((entry) => entry.scale === selectedScale) ?? active?.scales[0]
  const loading = systemLoading || overview.loading
  const error = overview.error ?? systemError
  return (
    <PanelCard className="space-y-4 p-4 sm:p-5">
      <PanelHeader className="flex-wrap">
        <div className="space-y-1">
          <MetaLabel>Fixed workloads · {selectedSystem ?? "system unknown"}</MetaLabel>
          <SectionTitle as="h2">Compare by workload family</SectionTitle>
          <BodyText>
            Performance, execution coverage and validation evidence answer different questions.
          </BodyText>
        </div>
      </PanelHeader>
      <CampaignNotice />
      <ProfileScopeNotice />
      <SnapshotSpanWarning snapshotCount={overview.snapshotCount} />
      {overview.retrospective ? (
        <NoticeStrip>
          This policy describes the published measurements retrospectively. Preparation annotations
          are sourced observations, not a new execution campaign.
        </NoticeStrip>
      ) : null}
      <details className="text-xs leading-5 text-slate-400">
        <summary className="cursor-pointer text-slate-300">Comparison policy</summary>
        <p className="mt-2">
          Each family and scale uses its full canonical query set. Eligibility requires a completed,
          non-invalidated run, every required warmup and three successful measured iterations per
          performance query, and no recorded errors, wrong results, hash conflicts or documented
          incomplete preparation. Eligible participants must have the same recorded input
          fingerprint. Partial engines stay visible. No success intersection, latency smoothing or
          missing-result penalties are used.
        </p>
        <p className="mt-2">
          Scores are geometric means of per-query median latency divided by the fastest eligible
          publication median. Baselines include the entire eligible publication cohort at this
          system, family and scale. Hiding an engine never changes them. An eligible cohort of one
          has a score of 1.00× and establishes no comparative advantage.
        </p>
        <p className="mt-2">
          Families and scales are separate; no overall ranking is published. Prepared RTABench and
          smoke workloads are separate tracks. Coverage always retains the full metadata-defined
          set, including queries excluded from comparable performance.
        </p>
      </details>
      {loading ? (
        <div className="space-y-3">
          <Skeleton className="h-10 w-full" />
          {Array.from({ length: 5 }, (_, index) => (
            <ChartFrame key={index}>
              <Skeleton className="h-5 w-28" />
              <div className="mt-3 grid gap-3 md:grid-cols-3">
                {Array.from({ length: 3 }, (_column, column) => (
                  <Skeleton key={column} className="h-24 w-full" />
                ))}
              </div>
            </ChartFrame>
          ))}
        </div>
      ) : error ? (
        <NoticeStrip tone="warning">Failed to load comparison: {error}</NoticeStrip>
      ) : active && cell ? (
        <>
          <div role="group" aria-label="Workload family" className="flex flex-wrap gap-2">
            {overview.families.map(({ family }) => (
              <SegmentedButton
                key={family.id}
                selected={family.id === active.family.id}
                aria-pressed={family.id === active.family.id}
                onClick={() => {
                  setFamilyId(family.id)
                  setSelectedScale(null)
                }}
              >
                {family.title}
                {family.track !== "raw" ? ` · ${family.track}` : ""}
              </SegmentedButton>
            ))}
          </div>
          <PanelHeader className="flex-wrap items-center">
            <div>
              <SectionTitle as="h3">{active.family.title}</SectionTitle>
              <BodyText className="mt-1 text-xs">
                {active.family.query_names.length} fixed performance queries ·{" "}
                {active.family.coverage_query_names.length} coverage positions ·{" "}
                {active.family.track} track
              </BodyText>
            </div>
            <div className="flex flex-wrap gap-2">
              <div role="group" aria-label="Scale factor" className="flex flex-wrap gap-1">
                {active.scales.map(({ scale }) => (
                  <SegmentedButton
                    key={scale}
                    selected={cell.scale === scale}
                    aria-pressed={cell.scale === scale}
                    onClick={() => setSelectedScale(scale)}
                  >
                    SF {scale}
                  </SegmentedButton>
                ))}
              </div>
              <QuietButton
                onClick={() => {
                  const params = new URLSearchParams({
                    mode: "database",
                    scale: String(cell.scale),
                    family: active.family.id,
                  })
                  if (selectedSystem) params.set("system", selectedSystem)
                  navigate(`/explorer/${active.family.suite}?${params}`)
                }}
              >
                Open explorer
              </QuietButton>
            </div>
          </PanelHeader>
          <details className="text-xs text-slate-400">
            <summary className="cursor-pointer">Visible engines</summary>
            <div className="mt-2 flex flex-wrap gap-2">
              {[...new Set(cell.rows.map((row) => row.db))].map((db) => (
                <ControlChip
                  key={db}
                  selected={!hidden.includes(db)}
                  aria-pressed={!hidden.includes(db)}
                  onClick={() =>
                    setHidden((current) =>
                      current.includes(db)
                        ? current.filter((entry) => entry !== db)
                        : [...current, db],
                    )
                  }
                >
                  {db}
                </ControlChip>
              ))}
            </div>
          </details>
          <EngineComparison
            rows={cell.rows.filter((row) => !hidden.includes(row.db))}
            family={active.family}
            eligibleCount={cell.rows.filter((row) => row.eligible).length}
          />
        </>
      ) : (
        <BodyText>No recorded system is available.</BodyText>
      )}
    </PanelCard>
  )
}
