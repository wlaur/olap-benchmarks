import { HelpCircle, Trophy } from "lucide-react"
import { useCallback, useMemo, useRef, useState } from "react"

import { cn } from "../../lib/cn"
import { computeDatabaseScores, formatScore, SCORE_EXPLAINER } from "../../lib/score"
import type { QuerySummary } from "../../lib/types"
import { PortalCard, useAnchoredPosition, useDismissable } from "../controls/Popover"
import { PanelCard } from "../layout/Panel"
import { Skeleton } from "../Skeleton"
import { MetaLabel, SectionTitle } from "../Typography"

interface SuiteScoreCardsProps {
  querySummaries: QuerySummary[]
  databaseColors: Record<string, string>
  isLoading: boolean
}

export function SuiteScoreCards({
  querySummaries,
  databaseColors,
  isLoading,
}: SuiteScoreCardsProps) {
  const scores = useMemo(() => computeDatabaseScores(querySummaries), [querySummaries])

  return (
    <PanelCard className="p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <SectionTitle as="h3">Suite score</SectionTitle>
          <ScoreInfoButton />
        </div>
        <MetaLabel className="tracking-normal text-slate-400 normal-case">
          Lower is better · 1.00× = fastest on every query
        </MetaLabel>
      </div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5">
        {isLoading
          ? Array.from({ length: 5 }, (_, i) => (
              <Skeleton key={i} className="h-[5.5rem] rounded-lg" />
            ))
          : scores.map((entry, index) => (
              <ScoreCard
                key={entry.db}
                rank={index + 1}
                db={entry.db}
                score={entry.score}
                wins={entry.wins}
                queryCount={entry.queryCount}
                missing={entry.missing}
                accent={databaseColors[entry.db] ?? "#94a3b8"}
              />
            ))}
      </div>
    </PanelCard>
  )
}

interface ScoreCardProps {
  rank: number
  db: string
  score: number
  wins: number
  queryCount: number
  missing: number
  accent: string
}

function ScoreCard({ rank, db, score, wins, queryCount, missing, accent }: ScoreCardProps) {
  const isLeader = rank === 1
  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-lg border bg-surface-inset px-3 py-2.5",
        isLeader ? "border-amber-300/40" : "border-border-default",
      )}
    >
      <span
        aria-hidden
        className="absolute inset-y-0 left-0 w-1"
        style={{ backgroundColor: accent }}
      />
      <div className="ml-2">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5">
            <span className="text-[10px] font-semibold tracking-wider text-slate-400 uppercase">
              #{rank}
            </span>
            {isLeader ? <Trophy className="size-3 text-amber-300" /> : null}
          </div>
          <span className="font-mono text-base font-semibold text-slate-50 tabular-nums">
            {formatScore(score)}
          </span>
        </div>
        <p className="mt-0.5 truncate text-sm font-medium text-slate-100">{db}</p>
        <p className="mt-0.5 text-[11px] text-slate-400">
          {wins}/{queryCount} fastest{missing > 0 ? ` · ${missing} missing penalized` : ""}
        </p>
      </div>
    </div>
  )
}

function ScoreInfoButton() {
  const [isOpen, setIsOpen] = useState(false)
  const triggerRef = useRef<HTMLButtonElement | null>(null)
  const popoverRef = useRef<HTMLDivElement | null>(null)
  const position = useAnchoredPosition(isOpen, triggerRef, { align: "start", gap: 8 })
  const close = useCallback(() => setIsOpen(false), [])
  useDismissable(isOpen, close, triggerRef, popoverRef)

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        aria-label="Explain score"
        aria-expanded={isOpen}
        onClick={() => setIsOpen((v) => !v)}
        className="rounded-full p-0.5 text-slate-400 transition-colors outline-none hover:bg-surface-elevated hover:text-slate-100 focus-visible:ring-2 focus-visible:ring-slate-300/30"
      >
        <HelpCircle className="size-4" />
      </button>
      {isOpen && position ? (
        <PortalCard
          ref={popoverRef}
          className="w-80 max-w-[calc(100vw-1.5rem)] bg-surface-elevated p-4 text-sm"
          style={{ left: position.left, top: position.top }}
        >
          <p className="text-sm font-semibold text-slate-50">{SCORE_EXPLAINER.title}</p>
          <div className="mt-2 space-y-2 text-[13px] leading-relaxed text-slate-300">
            {SCORE_EXPLAINER.body.map((paragraph, index) => (
              <p key={index}>{paragraph}</p>
            ))}
          </div>
        </PortalCard>
      ) : null}
    </>
  )
}
