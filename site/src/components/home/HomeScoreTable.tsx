import { Trophy } from "lucide-react"
import { useNavigate } from "react-router-dom"

import { useHomeOverview } from "../../hooks/useHomeOverview"
import { cn } from "../../lib/cn"
import { getDatabaseColors } from "../../lib/databaseColors"
import { formatLatencyMs } from "../../lib/format"
import { formatScore, geometricMean } from "../../lib/score"
import { useAppStore } from "../../stores/useAppStore"
import {
  CampaignNotice,
  ProfileScopeNotice,
  SnapshotSpanWarning,
} from "../methodology/ComparisonScopeNotices"
import { Skeleton } from "../Skeleton"
import { BodyText, CaptionText, NumericText, SectionTitle } from "../Typography"

export function HomeScoreTable() {
  const navigate = useNavigate()
  const benchmarkDefinitions = useAppStore((s) => s.benchmarkDefinitions)
  const suitesLoading = useAppStore((s) => s.suitesLoading)
  const suitesError = useAppStore((s) => s.suitesError)
  const selectedSystem = useAppStore((s) => s.selectedSystem)
  const systemLoading = useAppStore((s) => s.systemLoading)
  const overview = useHomeOverview(selectedSystem, benchmarkDefinitions)

  const databaseColors = getDatabaseColors(overview.databases.map((database) => database.label))
  const suites =
    overview.suites.length > 0
      ? overview.suites
      : benchmarkDefinitions.map((definition) => ({
          key: `${definition.id}:sf${definition.defaultScaleFactor}`,
          suiteId: definition.id,
          suiteScaleFactor: definition.defaultScaleFactor,
          title: `${definition.title} SF${definition.defaultScaleFactor}`,
          publicRole: definition.publicRole,
          scores: [],
        }))
  const isLoading = systemLoading || suitesLoading || overview.loading
  const isEmpty = !isLoading && overview.databases.length === 0
  const emptyMessage = overview.error
    ? `Failed to load results: ${overview.error}`
    : suitesError
      ? `Failed to load suites: ${suitesError}`
      : `No completed runs found${selectedSystem ? ` for ${selectedSystem}` : ""}.`

  return (
    <div className="overflow-hidden rounded-lg border border-border-default bg-surface-raised">
      <div className="border-b border-border-default px-5 py-4">
        <SectionTitle as="h2">Database ranking</SectionTitle>
        <BodyText className="mt-1">
          Overall score across benchmark suites, relative to the fastest of the databases shown, so
          scores shift when databases are added or removed. Lower is better; 1.0× means fastest on
          every query. Missing results are penalized. Below each score is the absolute geometric
          mean of the per-query median latency over the queries that database completed.{" "}
          {selectedSystem ? (
            <>
              System: <span className="text-slate-200">{selectedSystem}</span>.
            </>
          ) : null}
        </BodyText>
        <details className="mt-2 font-sans text-xs text-slate-500">
          <summary className="w-fit cursor-pointer rounded-sm outline-none hover:text-slate-300 focus-visible:ring-2 focus-visible:ring-slate-300/20">
            How scoring works
          </summary>
          <p className="mt-2 max-w-4xl leading-5">
            Each suite score is the geometric mean of per-query latency ratios versus the fastest
            database shown, with 10ms smoothing and missing queries penalized. Overall ordering is
            the geometric mean across benchmark suites; smoke suites are shown but excluded. A
            missing suite counts as that suite&apos;s worst recorded score. Latency is end-to-end
            client latency, from sending the query until the result is a Polars DataFrame, as the
            median of the measured iterations; the absolute figures cover only completed queries, so
            compare them between databases with the same completed count.
          </p>
        </details>
        <CampaignNotice className="mt-3" />
        <ProfileScopeNotice className="mt-2" />
        <SnapshotSpanWarning snapshotCount={overview.snapshotCount} className="mt-2" />
      </div>

      <div className="panel-scrollbar hidden overflow-x-auto overscroll-x-contain lg:block">
        <table className="w-full min-w-[59rem] table-fixed border-collapse text-left text-sm">
          <thead className="bg-surface-inset/60 text-xs tracking-wide text-slate-300 uppercase">
            <tr>
              <th className="sticky left-0 z-20 w-44 bg-surface-inset px-4 py-3 font-medium">
                Database
              </th>
              <th className="w-24 px-2 py-3 text-right font-medium">
                Overall
                <CaptionText className="mt-0.5 tracking-normal normal-case">
                  score · geo-mean latency
                </CaptionText>
              </th>
              {suites.map((suite) => (
                <th key={suite.key} className="px-2 py-3 text-right font-medium last:pr-6">
                  <button
                    type="button"
                    className="cursor-pointer rounded-md px-1 text-right leading-tight text-slate-200 transition-colors outline-none hover:text-accent-300 focus-visible:ring-2 focus-visible:ring-slate-300/30"
                    onClick={() =>
                      navigate(`/explorer/${suite.suiteId}?scale=${suite.suiteScaleFactor}`)
                    }
                    title={`Open ${suite.title} explorer`}
                  >
                    <span>{suite.title}</span>
                    {suite.publicRole === "smoke" ? (
                      <span className="ml-1 text-[10px] font-semibold text-slate-500 uppercase">
                        smoke
                      </span>
                    ) : null}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <LoadingRows suiteCount={suites.length} />
            ) : isEmpty ? (
              <tr>
                <td
                  colSpan={suites.length + 2}
                  className="px-5 py-10 text-center text-sm text-slate-300"
                >
                  {emptyMessage}
                </td>
              </tr>
            ) : (
              overview.databases.map((database, index) => (
                <tr key={database.key} className="border-t border-border-subtle">
                  <td className="sticky left-0 z-10 bg-surface-raised px-4 py-3 align-middle">
                    <div className="flex items-center gap-2.5">
                      <span className="w-4 text-right font-mono text-xs text-slate-400 tabular-nums">
                        {index + 1}
                      </span>
                      <span
                        className="size-2.5 rounded-full"
                        style={{ backgroundColor: databaseColors[database.label] ?? "#94a3b8" }}
                      />
                      <span className="font-medium text-slate-100">{database.label}</span>
                    </div>
                  </td>
                  <td className="px-2 py-3 text-right align-middle">
                    <NumericText className="font-semibold">
                      {formatScore(database.overallScore)}
                    </NumericText>
                    <LatencyCaption latencyS={database.overallLatencyS} className="text-right" />
                  </td>
                  {suites.map((suite) => (
                    <ScoreCell
                      key={suite.key}
                      score={overview.scoresByDbAndSuite.get(database.key)?.get(suite.key)}
                      rank={
                        overview.suites
                          .find((s) => s.key === suite.key)
                          ?.scores.findIndex((entry) => entry.dbKey === database.key) ?? -1
                      }
                    />
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="lg:hidden">
        {isLoading ? (
          <MobileLoadingRows />
        ) : isEmpty ? (
          <p className="px-5 py-10 text-center text-sm text-slate-300">{emptyMessage}</p>
        ) : (
          <div
            role="list"
            aria-label="Suite scores by database"
            className="divide-y divide-border-subtle"
          >
            {overview.databases.map((database, index) => (
              <section
                key={database.key}
                role="listitem"
                aria-label={`${database.label} scores`}
                className="p-4"
              >
                <div className="flex min-w-0 items-center justify-between gap-3">
                  <div className="flex min-w-0 items-center gap-2.5">
                    <span className="w-4 text-right font-mono text-xs text-slate-400 tabular-nums">
                      {index + 1}
                    </span>
                    <span
                      className="size-2.5 shrink-0 rounded-full"
                      style={{ backgroundColor: databaseColors[database.label] ?? "#94a3b8" }}
                    />
                    <span className="min-w-0 truncate font-semibold text-slate-100">
                      {database.label}
                    </span>
                  </div>
                  <div className="shrink-0 text-right">
                    <NumericText className="font-semibold">
                      {formatScore(database.overallScore)}
                    </NumericText>
                    <LatencyCaption latencyS={database.overallLatencyS} />
                  </div>
                </div>
                <details className="mt-3">
                  <summary className="w-fit cursor-pointer rounded-sm text-xs text-slate-400 outline-none hover:text-slate-200 focus-visible:ring-2 focus-visible:ring-slate-300/20">
                    Suite breakdown
                  </summary>
                  <div className="mt-3 grid gap-2 sm:grid-cols-2">
                    {suites.map((suite) => (
                      <div key={suite.key} className="min-w-0 rounded-md bg-surface-inset p-3">
                        <button
                          type="button"
                          onClick={() =>
                            navigate(`/explorer/${suite.suiteId}?scale=${suite.suiteScaleFactor}`)
                          }
                          className="block max-w-full truncate rounded-sm text-left text-xs font-medium text-slate-300 transition-colors outline-none hover:text-accent-300 focus-visible:ring-2 focus-visible:ring-slate-300/30"
                          title={`Open ${suite.title} explorer`}
                        >
                          {suite.title}
                          {suite.publicRole === "smoke" ? (
                            <span className="ml-1 text-[9px] font-semibold text-slate-500 uppercase">
                              smoke
                            </span>
                          ) : null}
                        </button>
                        <ScoreValue
                          score={overview.scoresByDbAndSuite.get(database.key)?.get(suite.key)}
                          rank={
                            overview.suites
                              .find((entry) => entry.key === suite.key)
                              ?.scores.findIndex((entry) => entry.dbKey === database.key) ?? -1
                          }
                          align="left"
                        />
                      </div>
                    ))}
                  </div>
                </details>
              </section>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

interface ScoreCellProps {
  score: ReturnType<typeof useHomeOverview>["suites"][number]["scores"][number] | undefined
  rank: number
}

function ScoreCell({ score, rank }: ScoreCellProps) {
  return (
    <td className="px-2 py-3 text-right align-middle last:pr-6">
      <ScoreValue score={score} rank={rank} align="right" />
    </td>
  )
}

function ScoreValue({ score, rank, align }: ScoreCellProps & { align: "left" | "right" }) {
  if (!score) {
    return (
      <div className={cn(align === "right" ? "text-right" : "mt-2 text-left")}>
        <NumericText className="text-slate-500">—</NumericText>
        <CaptionText className="mt-0.5 text-slate-500">not run</CaptionText>
      </div>
    )
  }

  const isLeader = rank === 0 && Number.isFinite(score.score)
  const coverageParts = [
    score.queryCount > 0 ? `${score.wins}/${score.queryCount} fastest` : "0 completed",
    score.failed > 0 ? `${score.failed} failed` : null,
    score.neverCompleted > 0 ? `${score.neverCompleted} not run` : null,
    score.latestSelectFailed && score.failed === 0 ? "latest failed" : null,
  ].filter(Boolean)

  return (
    <div className={cn(align === "left" && "mt-2")}>
      <div className={cn("flex items-center gap-2", align === "right" && "justify-end")}>
        {isLeader ? <Trophy className="size-3.5 text-amber-300" aria-label="Leader" /> : null}
        <NumericText className={cn(isLeader && "font-semibold text-amber-200")}>
          {formatScore(score.score)}
        </NumericText>
      </div>
      <LatencyCaption
        latencyS={geometricMean(score.latenciesS)}
        className={cn(align === "right" && "text-right")}
      />
      <CaptionText className={cn("mt-0.5", align === "right" && "text-right")}>
        {coverageParts.join(" · ")}
        {score.missing > 0 && score.failed > 0 && score.neverCompleted === 0 ? " penalized" : ""}
      </CaptionText>
    </div>
  )
}

function LatencyCaption({ latencyS, className }: { latencyS: number | null; className?: string }) {
  if (latencyS === null) return null

  return (
    <CaptionText
      className={cn("mt-0.5 font-mono whitespace-nowrap text-slate-300 tabular-nums", className)}
      as="p"
    >
      <span title="Geometric mean of the per-query median measured latency over the completed queries">
        {formatLatencyMs(latencyS * 1000)}
      </span>
    </CaptionText>
  )
}

function LoadingRows({ suiteCount }: { suiteCount: number }) {
  return (
    <>
      {Array.from({ length: 5 }, (_, rowIdx) => (
        <tr key={rowIdx} className="border-t border-border-subtle">
          <td className="px-5 py-3">
            <div className="flex items-center gap-2.5">
              <Skeleton className="h-3 w-4 rounded" />
              <Skeleton className="size-2.5 rounded-full" />
              <Skeleton className="h-4 w-24 rounded" />
            </div>
          </td>
          <td className="px-2 py-3">
            <Skeleton className="ml-auto h-4 w-12 rounded" />
          </td>
          {Array.from({ length: suiteCount }, (__, colIdx) => (
            <td key={colIdx} className="px-4 py-3 last:pr-6">
              <div className="flex flex-col items-end gap-1">
                <Skeleton className="h-4 w-12 rounded" />
                <Skeleton className="h-2.5 w-16 rounded" />
              </div>
            </td>
          ))}
        </tr>
      ))}
    </>
  )
}

function MobileLoadingRows() {
  return (
    <div className="divide-y divide-border-subtle">
      {Array.from({ length: 4 }, (_, rowIndex) => (
        <div key={rowIndex} className="flex items-center gap-2.5 p-4">
          <div className="flex items-center gap-2.5">
            <Skeleton className="h-3 w-4 rounded" />
            <Skeleton className="size-2.5 rounded-full" />
            <Skeleton className="h-4 w-28 rounded" />
          </div>
          <Skeleton className="ml-auto h-4 w-12 rounded" />
        </div>
      ))}
    </div>
  )
}
