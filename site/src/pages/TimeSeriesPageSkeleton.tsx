import { Skeleton } from "../components/Skeleton"

function SkeletonCard() {
  return (
    <div className="rounded-2xl border border-slate-800/45 bg-slate-900/55 p-5">
      <Skeleton className="h-3 w-24 rounded-full" />
      <Skeleton className="mt-4 h-8 w-36" />
      <Skeleton className="mt-3 h-4 w-48" />
    </div>
  )
}

export function TimeSeriesPageSkeleton() {
  return (
    <section className="flex h-full min-h-0 w-full flex-1 flex-col gap-4 overflow-hidden">
      <div className="grid shrink-0 gap-4 xl:grid-cols-[minmax(24rem,0.95fr)_minmax(0,1.15fr)]">
        <div className="rounded-3xl border border-slate-800/45 bg-slate-900/55 p-5">
          <Skeleton className="h-3 w-24 rounded-full" />
          <Skeleton className="mt-4 h-10 w-64" />
          <Skeleton className="mt-3 h-4 w-full max-w-xl" />
          <Skeleton className="mt-2 h-4 w-full max-w-lg" />

          <div className="mt-6 space-y-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <Skeleton className="h-3 w-36 rounded-full" />
                <Skeleton className="mt-2 h-4 w-80 max-w-full" />
              </div>
              <Skeleton className="h-8 w-14 rounded-full" />
            </div>
            <div className="flex flex-wrap gap-2">
              <Skeleton className="h-9 w-28 rounded-full" />
              <Skeleton className="h-9 w-32 rounded-full" />
              <Skeleton className="h-9 w-24 rounded-full" />
              <Skeleton className="h-9 w-30 rounded-full" />
            </div>
          </div>
        </div>

        <div className="rounded-3xl border border-slate-800/45 bg-slate-900/55 p-5">
          <div className="flex items-start justify-between gap-4">
            <div>
              <Skeleton className="h-6 w-44" />
              <Skeleton className="mt-2 h-4 w-80 max-w-full" />
            </div>
            <Skeleton className="h-7 w-28 rounded-full" />
          </div>

          <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_15rem]">
            <div className="rounded-2xl border border-slate-800/40 bg-slate-950/40 p-4">
              <Skeleton className="h-[156px] w-full rounded-2xl" />
            </div>
            <div className="space-y-3">
              <Skeleton className="h-[84px] w-full rounded-2xl" />
              <Skeleton className="h-[84px] w-full rounded-2xl" />
              <Skeleton className="h-[84px] w-full rounded-2xl" />
            </div>
          </div>
        </div>
      </div>

      <div className="grid shrink-0 gap-3 md:grid-cols-2 xl:grid-cols-4">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>

      <div className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[minmax(0,1.25fr)_minmax(24rem,0.95fr)]">
        <section className="flex min-h-0 flex-col rounded-3xl border border-slate-800/45 bg-slate-900/55">
          <div className="flex shrink-0 items-start justify-between gap-4 border-b border-slate-800 px-5 py-4">
            <div>
              <Skeleton className="h-6 w-56" />
              <Skeleton className="mt-2 h-4 w-80 max-w-full" />
            </div>
            <div className="flex gap-2">
              <Skeleton className="h-6 w-16 rounded-full" />
              <Skeleton className="h-6 w-20 rounded-full" />
              <Skeleton className="h-6 w-18 rounded-full" />
            </div>
          </div>

          <div className="min-h-0 flex-1 p-5 pt-4">
            <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-2xl border border-slate-800/40 bg-slate-950/35">
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

        <section className="min-h-0">
          <div className="flex h-full min-h-0 flex-col gap-4 rounded-3xl border border-slate-800/45 bg-slate-900/55 p-5">
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
