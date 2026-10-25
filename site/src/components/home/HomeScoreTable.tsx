import { Layers3 } from "lucide-react"
import { useNavigate, useSearchParams } from "react-router-dom"

import { useHomeOverview } from "../../hooks/useHomeOverview"
import type { ComparisonFamily } from "../../lib/comparison"
import { formatDatabaseName } from "../../lib/databaseNames"
import type { OverallEngineResult } from "../../lib/overall"
import { suiteDescriptions } from "../../lib/suiteDescriptions"
import { useAppStore } from "../../stores/useAppStore"
import { QuietButton, SegmentedButton } from "../controls/Control"
import { ControlSelect } from "../controls/ControlSelect"
import { DetailsDialog } from "../layout/DetailsDialog"
import { ChartFrame, NoticeStrip, PanelCard, PanelHeader, StatusBadge } from "../layout/Panel"
import {
  CampaignNotice,
  ProfileScopeNotice,
  SnapshotSpanWarning,
} from "../methodology/ComparisonScopeNotices"
import { EngineComparison } from "../methodology/EngineComparison"
import { Skeleton } from "../Skeleton"
import { BodyText, LeadText, MetaLabel, SecondaryText, SectionTitle } from "../Typography"
import { CoverageLegend, OverallRankingTable } from "./OverallRankingTable"

function overallLead(rows: readonly OverallEngineResult[]): {
  lead: string
  qualification: string
} {
  const ranked = rows.filter((row) => row.rank !== null).sort((a, b) => a.rank! - b.rank!)
  const qualification = "Higher is better · missing, failed or timed-out queries score 0"
  const [first, second] = ranked
  if (!first) return { lead: "No comparable overall scores", qualification }
  const label = (row: OverallEngineResult) =>
    `${formatDatabaseName(row.db)} · ${row.score.toFixed(1)}/100 · ${row.coverage.toFixed(0)}% weighted coverage`
  if (!second) return { lead: `Only comparable result: ${label(first)}`, qualification }
  if (second.score === first.score)
    return {
      lead: `Joint top score: ${formatDatabaseName(first.db)} and ${formatDatabaseName(second.db)} · ${first.score.toFixed(1)}/100`,
      qualification,
    }
  return { lead: `Top score: ${label(first)}`, qualification }
}

const WEIGHTED_COVERAGE =
  "Weighted coverage: share of performance queries with a usable result, weighted like the score"

function familyLabel(family: ComparisonFamily): string {
  const track = family.track === "raw" ? "" : ` · ${family.track} track`
  const weight = family.weight > 0 ? "" : " · not in overall score"
  return `${family.title}${track}${weight}`
}

export function HomeScoreTable() {
  const navigate = useNavigate()
  const selectedSystem = useAppStore((state) => state.selectedSystem)
  const systemLoading = useAppStore((state) => state.systemLoading)
  const systemError = useAppStore((state) => state.systemError)
  const overview = useHomeOverview(selectedSystem)
  const [searchParams, setSearchParams] = useSearchParams()
  const view = searchParams.get("view") === "workload" ? "workload" : "overall"
  const familyId = searchParams.get("family") ?? ""
  const scaleParam = Number(searchParams.get("scale"))
  const selectedScale = Number.isFinite(scaleParam) && scaleParam > 0 ? scaleParam : null
  const active =
    overview.families.find((entry) => entry.family.id === familyId) ??
    overview.families.find((entry) => entry.family.track === "raw") ??
    overview.families[0]
  const cell = active?.scales.find((entry) => entry.scale === selectedScale) ?? active?.scales[0]
  const loading = systemLoading || overview.loading
  const overallUnavailable = view === "overall" && overview.overallError !== null
  const error = overview.error ?? systemError ?? (view === "overall" ? overview.overallError : null)
  const summary = overallLead(overview.overall.rows)

  function update(next: { view: "overall" | "workload"; family?: string; scale?: number }) {
    const params = new URLSearchParams(searchParams)
    for (const key of ["view", "family", "scale"]) params.delete(key)
    if (next.view === "workload") {
      params.set("view", "workload")
      if (next.family) params.set("family", next.family)
      if (next.scale !== undefined) params.set("scale", String(next.scale))
    }
    setSearchParams(params)
  }

  function selectFamily(id: string) {
    update({ view: "workload", family: id })
  }

  return (
    <PanelCard className="space-y-4 p-3 sm:space-y-5 sm:p-6">
      <PanelHeader className="flex-wrap items-end">
        <div className="min-w-0 space-y-1">
          <MetaLabel>{selectedSystem ?? "System unknown"} · fixed SELECT workloads</MetaLabel>
          <SectionTitle as="h2">
            {view === "overall" ? "Overall database ranking" : "Compare one workload"}
          </SectionTitle>
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <div role="group" aria-label="Ranking view" className="flex min-w-0 flex-wrap gap-1">
            <SegmentedButton
              selected={view === "overall"}
              aria-pressed={view === "overall"}
              onClick={() => update({ view: "overall" })}
            >
              Overall
            </SegmentedButton>
            <SegmentedButton
              selected={view === "workload"}
              aria-pressed={view === "workload"}
              onClick={() =>
                update({
                  view: "workload",
                  family: familyId || undefined,
                  scale: selectedScale ?? undefined,
                })
              }
            >
              By workload
            </SegmentedButton>
          </div>
          <DetailsDialog title="Measurement context" trigger="Measurement context">
            <CampaignNotice />
            <ProfileScopeNotice />
            {overview.retrospective ? (
              <BodyText>
                This policy describes the published measurements retrospectively. Preparation
                annotations are sourced observations, not a new execution campaign.
              </BodyText>
            ) : null}
          </DetailsDialog>
        </div>
      </PanelHeader>
      <SnapshotSpanWarning snapshotCount={overview.snapshotCount} />
      {loading ? (
        <div className="space-y-3" aria-busy="true">
          <Skeleton className="h-7 w-3/4" />
          <Skeleton className="h-5 w-1/2" />
          <ChartFrame className="space-y-3">
            {Array.from({ length: 7 }, (_, index) => (
              <Skeleton key={index} className="h-10 w-full" />
            ))}
          </ChartFrame>
        </div>
      ) : error ? (
        <NoticeStrip tone="warning" role="alert">
          {overallUnavailable
            ? "Overall score unavailable for this system"
            : "Failed to load comparison"}
          : {error}
        </NoticeStrip>
      ) : overview.families.length === 0 ? (
        <BodyText>No recorded system is available.</BodyText>
      ) : view === "overall" ? (
        <>
          <div className="space-y-1" data-overall-lead>
            <LeadText className="font-semibold">{summary.lead}</LeadText>
            <SecondaryText>{summary.qualification}</SecondaryText>
            <div data-weighted-coverage-definition>
              <SecondaryText>{WEIGHTED_COVERAGE}</SecondaryText>
            </div>
          </div>
          <OverallRankingTable
            rows={overview.overall.rows}
            familyCount={overview.overall.familyCount}
            onSelectFamily={selectFamily}
          />
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-2">
              <SecondaryText>
                Fastest median per query = 100 points, 2× slower = 50 ·{" "}
                {overview.overall.familyCount} equally weighted workloads ·{" "}
                {overview.overall.queryCount} performance queries across scales
              </SecondaryText>
              <CoverageLegend />
            </div>
            <OverallMethod />
          </div>
        </>
      ) : active && cell ? (
        <>
          <div className="flex flex-wrap items-end gap-3">
            <ControlSelect
              ariaLabel="Workload"
              label="Workload"
              value={active.family.id}
              onChange={selectFamily}
              options={overview.families.map(({ family }) => ({
                value: family.id,
                label: familyLabel(family),
                description: suiteDescriptions[family.suite]?.purpose,
              }))}
              icon={<Layers3 className="size-3.5" strokeWidth={1.8} />}
              labelMode="always"
              className="w-full sm:w-auto"
              menuClassName="max-w-[calc(100vw-1.5rem)] "
            />
            {active.scales.length > 1 ? (
              <div role="group" aria-label="Scale factor" className="flex flex-wrap gap-1">
                {active.scales.map(({ scale }) => (
                  <SegmentedButton
                    key={scale}
                    selected={cell.scale === scale}
                    aria-pressed={cell.scale === scale}
                    onClick={() => update({ view: "workload", family: active.family.id, scale })}
                  >
                    SF {scale}
                  </SegmentedButton>
                ))}
              </div>
            ) : null}
            <QuietButton
              className="sm:ml-auto"
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
              Open in Explorer
            </QuietButton>
          </div>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <SectionTitle as="h3" className="text-lg">
              {active.family.title} · SF {cell.scale}
            </SectionTitle>
            <StatusBadge tone={active.family.track === "raw" ? "neutral" : "warning"}>
              {active.family.track} track
            </StatusBadge>
            {active.family.weight === 0 ? (
              <StatusBadge tone="neutral">Not in overall score</StatusBadge>
            ) : null}
          </div>
          <SecondaryText>
            {active.family.query_names.length} performance queries ·{" "}
            {active.family.coverage_query_names.length} execution queries · {active.family.track}{" "}
            track. Latency ratios use different units and eligibility from the overall 0–100 score.
          </SecondaryText>
          <EngineComparison
            rows={cell.rows}
            family={active.family}
            eligibleCount={cell.rows.filter((row) => row.eligible).length}
          />
        </>
      ) : null}
    </PanelCard>
  )
}

function OverallMethod() {
  return (
    <DetailsDialog title="Overall ranking method" trigger="How the score works">
      <BodyText>
        Each query earns 100 × fastest median / database median: 100 points for the fastest, 50 for
        twice as slow, 10 for ten times as slow. Missing or unusable results earn zero. Average the
        points within each scale, then each workload, then across workloads using their published
        weights. Every database keeps the same fixed denominator.
      </BodyText>
      <BodyText>
        Each scored query requires one successful warmup and three successful measured iterations
        from an otherwise valid completed run. Other missing queries do not discard that evidence.
        Failed runs, invalidation and documented incomplete preparation block the whole run.
        Conflicting answers, mixed inputs or fewer than two usable engines per query block the
        overall ranking.
      </BodyText>
      <BodyText>
        Weighted coverage is the share of performance queries with a usable result, averaged with
        the same scale and workload weights as the score. Unweighted counts show usable results out
        of all performance query / scale combinations; neither is a correctness percentage. Gap
        reasons are in each database&apos;s details.
      </BodyText>
      <BodyText>
        Reference medians include complete queries from partial completed runs across the whole
        publication. Prepared and smoke tracks and zero-weight families are excluded. Scores
        describe the published speed and coverage, not universal database capability or an overall
        latency multiplier. Equal family weights are an explicit workload-mix choice. The details
        include rank sensitivity to removing one workload; this is not a confidence interval.
      </BodyText>
      <a
        href="https://github.com/wlaur/olap-benchmarks/blob/main/docs/overall-ranking.md"
        target="_blank"
        rel="noreferrer"
        className="inline-block text-base text-accent-300 underline underline-offset-2 hover:text-accent-200"
      >
        Full ranking methodology
      </a>
    </DetailsDialog>
  )
}
