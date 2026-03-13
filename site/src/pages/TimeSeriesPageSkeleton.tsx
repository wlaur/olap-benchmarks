import { Skeleton } from "../components/Skeleton"
import {
  TIME_SERIES_BOTTOM_GRID_CLASS,
  TIME_SERIES_DETAIL_SECTION_CLASS,
  TIME_SERIES_QUERY_SECTION_CLASS,
  TIME_SERIES_QUERY_TABLE_CONTAINER_CLASS,
  TIME_SERIES_QUERY_TABLE_WRAPPER_CLASS,
  TIME_SERIES_TOP_GRID_CLASS,
} from "./timeSeriesLayout"
import {
  TimeSeriesOverviewChartFrame,
  TimeSeriesOverviewHeader,
  TimeSeriesTopCard,
} from "./timeSeriesShell"

export function TimeSeriesPageSkeleton() {
  return (
    <section className="flex min-h-full w-full flex-col gap-4 pb-4">
      <div className={TIME_SERIES_TOP_GRID_CLASS}>
        <TimeSeriesTopCard>
          <Skeleton className="h-3 w-24 rounded-full" />
          <Skeleton className="mt-4 h-10 w-64" />
          <Skeleton className="mt-3 h-4 w-full max-w-xl" />
          <Skeleton className="mt-2 h-4 w-full max-w-lg" />

          <div className="mt-5">
            <div className="flex flex-wrap gap-2">
              <Skeleton className="h-11 w-28 rounded-full" />
              <Skeleton className="h-11 w-32 rounded-full" />
              <Skeleton className="h-11 w-24 rounded-full" />
              <Skeleton className="h-11 w-32 rounded-full" />
            </div>
          </div>
        </TimeSeriesTopCard>

        <TimeSeriesTopCard>
          <TimeSeriesOverviewHeader>
            <div>
              <Skeleton className="h-6 w-44" />
              <Skeleton className="mt-2 h-4 w-80 max-w-full" />
              <Skeleton className="mt-2 h-4 w-full max-w-2xl" />
              <Skeleton className="mt-2 h-4 w-full max-w-xl" />
            </div>
            <div className="flex flex-wrap justify-end gap-2">
              <Skeleton className="h-8 w-24 rounded-full" />
              <Skeleton className="h-8 w-20 rounded-full" />
              <Skeleton className="h-8 w-28 rounded-full" />
            </div>
          </TimeSeriesOverviewHeader>

          <TimeSeriesOverviewChartFrame>
            <div className="grid h-full grid-cols-[4rem_minmax(0,1fr)] gap-4">
              <div className="flex flex-col justify-around py-3">
                <Skeleton className="h-3 w-10 rounded-full" />
                <Skeleton className="h-3 w-9 rounded-full" />
                <Skeleton className="h-3 w-11 rounded-full" />
                <Skeleton className="h-3 w-8 rounded-full" />
              </div>
              <div className="relative min-h-0 rounded-xl">
                <div className="absolute inset-x-0 bottom-0 border-t border-slate-800/80" />
                <div className="absolute inset-y-0 left-0 border-l border-slate-800/80" />
                <div className="absolute inset-x-0 top-[20%] border-t border-slate-800/40" />
                <div className="absolute inset-x-0 top-[45%] border-t border-slate-800/40" />
                <div className="absolute inset-x-0 top-[70%] border-t border-slate-800/40" />
                <div className="absolute inset-0 flex items-end gap-4 px-4 pt-4 pb-6">
                  <Skeleton className="h-[72%] flex-1 rounded-xl" />
                  <Skeleton className="h-[48%] flex-1 rounded-xl" />
                  <Skeleton className="h-[28%] flex-1 rounded-xl" />
                  <Skeleton className="h-[62%] flex-1 rounded-xl" />
                </div>
              </div>
            </div>
          </TimeSeriesOverviewChartFrame>
        </TimeSeriesTopCard>
      </div>

      <section className="rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <Skeleton className="h-6 w-40" />
            <Skeleton className="mt-2 h-4 w-full max-w-3xl" />
            <Skeleton className="mt-2 h-4 w-full max-w-2xl" />
          </div>
          <Skeleton className="h-11 w-24 rounded-full" />
        </div>
      </section>

      <div className={TIME_SERIES_BOTTOM_GRID_CLASS}>
        <section className={TIME_SERIES_QUERY_SECTION_CLASS}>
          <div className="flex shrink-0 items-start justify-between gap-4 border-b border-slate-800 px-5 py-4">
            <div>
              <Skeleton className="h-6 w-56" />
              <Skeleton className="mt-2 h-4 w-80 max-w-full" />
            </div>
            <div className="flex gap-2">
              <Skeleton className="h-6 w-16 rounded-full" />
              <Skeleton className="h-6 w-20 rounded-full" />
              <Skeleton className="h-6 w-20 rounded-full" />
            </div>
          </div>

          <div className={TIME_SERIES_QUERY_TABLE_WRAPPER_CLASS}>
            <div
              className={`flex min-h-0 flex-col overflow-hidden rounded-2xl border border-slate-800/40 bg-slate-950/35 ${TIME_SERIES_QUERY_TABLE_CONTAINER_CLASS}`}
            >
              <div className="grid shrink-0 grid-cols-[32%_34%_12%_10%_12%] gap-0 border-b border-slate-800/40 bg-slate-900/80 px-4 py-3">
                <Skeleton className="h-4 w-20" />
                <Skeleton className="h-4 w-28" />
                <Skeleton className="h-4 w-12" />
                <Skeleton className="h-4 w-12" />
                <Skeleton className="h-4 w-20" />
              </div>
              <div className="space-y-3 p-4">
                <Skeleton className="h-16 w-full rounded-2xl" />
                <Skeleton className="h-16 w-full rounded-2xl" />
                <Skeleton className="h-16 w-full rounded-2xl" />
                <Skeleton className="h-16 w-full rounded-2xl" />
                <Skeleton className="h-16 w-full rounded-2xl" />
              </div>
            </div>
          </div>
        </section>

        <section className={TIME_SERIES_DETAIL_SECTION_CLASS}>
          <div className="flex h-full min-h-0 flex-col gap-4 rounded-3xl border border-slate-800 bg-slate-900/70 p-5">
            <div className="flex items-start justify-between">
              <div>
                <Skeleton className="h-6 w-40" />
                <Skeleton className="mt-2 h-4 w-64" />
              </div>
              <Skeleton className="h-8 w-16 rounded-lg" />
            </div>

            <div className="rounded-2xl border border-slate-800/40 bg-slate-950/40 p-4">
              <Skeleton className="h-[168px] w-full rounded-2xl" />
            </div>

            <div className="flex min-h-0 flex-1 flex-col rounded-2xl border border-slate-700/50 bg-slate-950/75">
              <div className="flex gap-1 border-b border-slate-800/40 px-4 py-2">
                <Skeleton className="h-6 w-14 rounded-md" />
                <Skeleton className="h-6 w-20 rounded-md" />
              </div>
              <div className="min-h-0 flex-1 p-4">
                <Skeleton className="h-full w-full rounded-xl" />
              </div>
            </div>
          </div>
        </section>
      </div>
    </section>
  )
}
