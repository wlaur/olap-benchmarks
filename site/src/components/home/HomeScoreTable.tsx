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
import { OverallRankingTable } from "./OverallRankingTable"

export function HomeScoreTable() {
  const navigate = useNavigate()
  const selectedSystem = useAppStore((state) => state.selectedSystem)
  const systemLoading = useAppStore((state) => state.systemLoading)
  const systemError = useAppStore((state) => state.systemError)
  const overview = useHomeOverview(selectedSystem)
  const [view, setView] = useState<"overall" | "workload">("overall")
  const [familyId, setFamilyId] = useState("")
  const [selectedScale, setSelectedScale] = useState<number | null>(null)
  const [hidden, setHidden] = useState<string[]>([])
  const active =
    overview.families.find((entry) => entry.family.id === familyId) ??
    overview.families.find((entry) => entry.family.track === "raw") ??
    overview.families[0]
  const cell = active?.scales.find((entry) => entry.scale === selectedScale) ?? active?.scales[0]
  const loading = systemLoading || overview.loading
  const overallUnavailable = view === "overall" && overview.overallError !== null
  const error = overview.error ?? systemError ?? (view === "overall" ? overview.overallError : null)
  const engines =
    view === "overall"
      ? overview.overall.rows.map((row) => row.db)
      : [...new Set(cell?.rows.map((row) => row.db) ?? [])]

  function selectFamily(id: string) {
    setFamilyId(id)
    setSelectedScale(null)
    setView("workload")
  }

  return (
    <PanelCard className="space-y-4 p-4 sm:p-5">
      <PanelHeader className="flex-wrap">
        <div className="space-y-1">
          <MetaLabel>Fixed workloads · {selectedSystem ?? "system unknown"}</MetaLabel>
          <SectionTitle as="h2">
            {view === "overall" ? "Overall database ranking" : "Compare by workload family"}
          </SectionTitle>
          <BodyText>
            {view === "overall"
              ? "Per query: fastest = 100 points, twice as slow = 50, missing = 0. Higher is better."
              : "Performance, execution coverage and validation evidence answer different questions."}
          </BodyText>
        </div>
      </PanelHeader>
      <div role="group" aria-label="Ranking view" className="flex flex-wrap gap-2">
        <SegmentedButton
          selected={view === "overall"}
          aria-pressed={view === "overall"}
          onClick={() => setView("overall")}
        >
          Overall
        </SegmentedButton>
        <SegmentedButton
          selected={view === "workload"}
          aria-pressed={view === "workload"}
          onClick={() => setView("workload")}
        >
          By workload
        </SegmentedButton>
      </div>
      {loading ? (
        <div className="space-y-3">
          <Skeleton className="h-10 w-full" />
          <ChartFrame className="space-y-3">
            {Array.from({ length: 7 }, (_, index) => (
              <Skeleton key={index} className="h-10 w-full" />
            ))}
          </ChartFrame>
        </div>
      ) : error ? (
        <NoticeStrip tone="warning">
          {overallUnavailable
            ? "Overall score unavailable for this system"
            : "Failed to load comparison"}
          : {error}
        </NoticeStrip>
      ) : overview.families.length > 0 ? (
        <>
          {view === "overall" ? (
            <>
              <BodyText className="text-xs">
                {overview.overall.familyCount} equally weighted workloads ·{" "}
                {overview.overall.queryCount} fixed query positions.
              </BodyText>
              <OverallRankingTable
                rows={overview.overall.rows.filter((row) => !hidden.includes(row.db))}
                familyCount={overview.overall.familyCount}
                onSelectFamily={selectFamily}
              />
            </>
          ) : active && cell ? (
            <>
              <div role="group" aria-label="Workload family" className="flex flex-wrap gap-2">
                {overview.families.map(({ family }) => (
                  <SegmentedButton
                    key={family.id}
                    selected={family.id === active.family.id}
                    aria-pressed={family.id === active.family.id}
                    onClick={() => selectFamily(family.id)}
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
              <EngineComparison
                rows={cell.rows.filter((row) => !hidden.includes(row.db))}
                family={active.family}
                eligibleCount={cell.rows.filter((row) => row.eligible).length}
              />
            </>
          ) : null}
          <details className="text-xs text-slate-400">
            <summary className="cursor-pointer">Visible engines</summary>
            <div className="mt-2 flex flex-wrap gap-2">
              {engines.map((db) => (
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
        </>
      ) : (
        <BodyText>No recorded system is available.</BodyText>
      )}
      <details className="text-xs leading-5 text-slate-400">
        <summary className="cursor-pointer text-slate-300">
          {view === "overall" ? "Overall ranking method" : "Comparison policy"}
        </summary>
        {view === "overall" ? (
          <>
            <p className="mt-2">
              Each query earns 100 × fastest median / database median: 100 points for the fastest,
              50 for twice as slow, 10 for ten times as slow. Missing or unusable results earn zero.
              Average the points within each scale, then each workload, then across workloads using
              their published weights. Every database keeps the same fixed denominator.
            </p>
            <p className="mt-2">
              Each scored query requires one successful warmup and three successful measured
              iterations from an otherwise valid completed run. Other missing queries do not discard
              that evidence. Failed runs, invalidation and documented incomplete preparation block
              the whole run. Conflicting answers, mixed inputs or fewer than two usable engines per
              query block the overall ranking. Query counts show usable results out of all fixed
              query / scale combinations; they are not correctness percentages. Weighted coverage
              and gap reasons are available in each database's details.
            </p>
            <p className="mt-2">
              Reference medians include complete queries from partial completed runs across the
              whole publication. Hiding databases never changes them. Prepared and smoke tracks and
              zero-weight families are excluded. Scores describe the published speed and coverage,
              not universal database capability or an overall latency multiplier. Equal family
              weights are an explicit workload-mix choice. The details include rank sensitivity to
              removing one workload; this is not a confidence interval.
            </p>
          </>
        ) : (
          <>
            <p className="mt-2">
              Each family and scale uses its full canonical query set. Eligibility requires a
              completed, non-invalidated run, every required warmup and three successful measured
              iterations per performance query, and no recorded errors, wrong results, hash
              conflicts or documented incomplete preparation. Eligible participants must have the
              same recorded input fingerprint. Partial engines stay visible. No success
              intersection, latency smoothing or missing-result penalties are used.
            </p>
            <p className="mt-2">
              Workload scores are geometric means of per-query median latency divided by the fastest
              eligible publication median. Baselines include the entire eligible publication cohort
              at this system, family and scale. Hiding an engine never changes them. An eligible
              cohort of one has a score of 1.00× and establishes no comparative advantage.
            </p>
            <p className="mt-2">
              Strict family eligibility and latency ratios remain separate from the overall index.
              Prepared RTABench and smoke workloads are separate tracks. Coverage always retains the
              full metadata-defined set, including queries excluded from comparable performance.
            </p>
          </>
        )}
        <a
          href="https://github.com/wlaur/olap-benchmarks/blob/main/docs/overall-ranking.md"
          target="_blank"
          rel="noreferrer"
          className="mt-2 inline-block text-accent-300 underline underline-offset-2 hover:text-accent-200"
        >
          Full ranking methodology
        </a>
        {overview.retrospective ? (
          <p className="mt-2">
            This policy describes the published measurements retrospectively. Preparation
            annotations are sourced observations, not a new execution campaign.
          </p>
        ) : null}
      </details>
      <details className="text-xs leading-5 text-slate-400">
        <summary className="cursor-pointer text-slate-300">Measurement context</summary>
        <div className="mt-3 space-y-3">
          <CampaignNotice />
          <ProfileScopeNotice />
        </div>
      </details>
      <SnapshotSpanWarning snapshotCount={overview.snapshotCount} />
    </PanelCard>
  )
}
