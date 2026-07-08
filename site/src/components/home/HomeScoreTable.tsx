import { Trophy } from "lucide-react"
import { useNavigate } from "react-router-dom"

import { useHomeOverview } from "../../hooks/useHomeOverview"
import { benchmarkDefinitions } from "../../lib/benchmarks"
import { cn } from "../../lib/cn"
import { getDatabaseColors } from "../../lib/databaseColors"
import { formatScore } from "../../lib/score"
import { useAppStore } from "../../stores/useAppStore"
import { Skeleton } from "../Skeleton"
import { BodyText, SectionTitle } from "../Typography"

export function HomeScoreTable() {
  const navigate = useNavigate()
  const selectedSystem = useAppStore((s) => s.selectedSystem)
  const systemLoading = useAppStore((s) => s.systemLoading)
  const overview = useHomeOverview(selectedSystem)

  const databaseColors = getDatabaseColors(overview.databases.map((database) => database.label))
  const suites = benchmarkDefinitions
  const isLoading = systemLoading || overview.loading
  const isEmpty = !isLoading && overview.databases.length === 0

  return (
    <div className="overflow-hidden rounded-2xl border border-border-default bg-surface-raised">
      <div className="border-b border-border-default px-5 py-4">
        <SectionTitle as="h2">Suite scores</SectionTitle>
        <BodyText className="mt-1">
          Geometric mean of per-query latency vs the fastest database. Lower is better; 1.00× is the
          leader. Missing queries are penalized, and rows are ranked by geometric mean across suites
          at their default scale factor; a missing suite counts as its worst score.{" "}
          {selectedSystem ? (
            <>
              System: <span className="text-slate-200">{selectedSystem}</span>.
            </>
          ) : null}
        </BodyText>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full min-w-[40rem] border-collapse text-left text-sm">
          <thead className="bg-surface-inset/60 text-xs tracking-wide text-slate-300 uppercase">
            <tr>
              <th className="px-5 py-3 font-medium">Database</th>
              {suites.map((suite) => (
                <th key={suite.id} className="px-4 py-3 text-right font-medium whitespace-nowrap">
                  <button
                    type="button"
                    className="cursor-pointer rounded-md px-1 text-slate-200 transition-colors outline-none hover:text-accent-300 focus-visible:ring-2 focus-visible:ring-slate-300/30"
                    onClick={() => navigate(`/explorer/${suite.id}`)}
                    title={`Open ${suite.title} explorer`}
                  >
                    {suite.title}
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
                  colSpan={suites.length + 1}
                  className="px-5 py-10 text-center text-sm text-slate-300"
                >
                  {overview.error
                    ? `Failed to load results: ${overview.error}`
                    : `No completed runs found${selectedSystem ? ` for ${selectedSystem}` : ""}.`}
                </td>
              </tr>
            ) : (
              overview.databases.map((database, index) => (
                <tr key={database.key} className="border-t border-border-subtle">
                  <td className="px-5 py-3 align-middle">
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
                  {suites.map((suite) => (
                    <ScoreCell
                      key={suite.id}
                      score={overview.scoresByDbAndSuite.get(database.key)?.get(suite.id)}
                      rank={
                        overview.suites
                          .find((s) => s.suiteId === suite.id)
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
    </div>
  )
}

interface ScoreCellProps {
  score: ReturnType<typeof useHomeOverview>["suites"][number]["scores"][number] | undefined
  rank: number
}

function ScoreCell({ score, rank }: ScoreCellProps) {
  if (!score) {
    return (
      <td className="px-4 py-3 text-right align-middle">
        <span className="font-mono text-sm text-slate-500 tabular-nums">—</span>
        <p className="mt-0.5 text-right text-[10px] text-slate-500">not run</p>
      </td>
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
    <td className="px-4 py-3 text-right align-middle">
      <div className="flex items-center justify-end gap-2">
        {isLeader ? <Trophy className="size-3.5 text-amber-300" aria-label="Leader" /> : null}
        <span
          className={cn(
            "font-mono text-sm tabular-nums",
            isLeader ? "font-semibold text-amber-200" : "text-slate-100",
          )}
        >
          {formatScore(score.score)}
        </span>
      </div>
      <p className="mt-0.5 text-right text-[10px] text-slate-400">
        {coverageParts.join(" · ")}
        {score.missing > 0 && score.failed > 0 && score.neverCompleted === 0 ? " penalized" : ""}
      </p>
    </td>
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
          {Array.from({ length: suiteCount }, (__, colIdx) => (
            <td key={colIdx} className="px-4 py-3">
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
