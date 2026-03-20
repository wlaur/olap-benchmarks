import { QUERY_COMPARISON_TABLE_MIN_WIDTH_CLASS } from "../QueryComparisonTable"
import { Skeleton } from "../Skeleton"
import { BodyText, Eyebrow, FeatureTitle, MetaLabel } from "../Typography"

export function FilterChipsSkeleton() {
  return (
    <div className="flex flex-wrap gap-1.5">
      <Skeleton className="h-6 w-20 rounded-full" />
      <Skeleton className="h-6 w-24 rounded-full" />
      <Skeleton className="h-6 w-18 rounded-full" />
      <Skeleton className="h-6 w-24 rounded-full" />
    </div>
  )
}

export function OverviewControlsSkeleton() {
  return (
    <div className="flex flex-wrap items-center justify-end gap-1.5">
      <Skeleton className="h-6 w-20 rounded-full" />
      <Skeleton className="h-6 w-16 rounded-full" />
    </div>
  )
}

export function OverviewChartSkeleton() {
  return (
    <div className="grid h-full grid-cols-[4rem_minmax(0,1fr)] gap-4">
      <div className="flex flex-col justify-around py-3">
        <Skeleton className="h-3 w-10 rounded-full" />
        <Skeleton className="h-3 w-9 rounded-full" />
        <Skeleton className="h-3 w-11 rounded-full" />
        <Skeleton className="h-3 w-8 rounded-full" />
      </div>
      <div className="relative min-h-0 rounded-xl">
        <div className="absolute inset-x-0 bottom-0 border-t border-border-default" />
        <div className="absolute inset-y-0 left-0 border-l border-border-default" />
        <div className="absolute inset-x-0 top-[20%] border-t border-border-subtle" />
        <div className="absolute inset-x-0 top-[45%] border-t border-border-subtle" />
        <div className="absolute inset-x-0 top-[70%] border-t border-border-subtle" />
        <div className="absolute inset-0 flex items-end gap-4 px-4 pt-4 pb-6">
          <Skeleton className="h-[72%] flex-1 rounded-xl" />
          <Skeleton className="h-[48%] flex-1 rounded-xl" />
          <Skeleton className="h-[28%] flex-1 rounded-xl" />
          <Skeleton className="h-[62%] flex-1 rounded-xl" />
        </div>
      </div>
    </div>
  )
}

export function LegendSkeleton() {
  return (
    <div className="flex gap-2">
      <Skeleton className="h-6 w-16 rounded-full" />
      <Skeleton className="h-6 w-20 rounded-full" />
      <Skeleton className="h-6 w-20 rounded-full" />
    </div>
  )
}

const QUERY_TABLE_CONTAINER_CLASS = "h-[32rem]"

export function QueryTableSkeleton() {
  return (
    <div
      className={`flex min-h-0 flex-col rounded-2xl border border-border-default bg-surface-inset ${QUERY_TABLE_CONTAINER_CLASS}`}
    >
      <div className="panel-scrollbar min-h-0 overflow-x-scroll overflow-y-hidden">
        <div className={`h-full min-h-0 ${QUERY_COMPARISON_TABLE_MIN_WIDTH_CLASS}`}>
          <div className="panel-scrollbar h-full min-h-0 overflow-y-scroll">
            <div className="grid shrink-0 grid-cols-[30%_26%_12%_16%_16%] gap-0 border-b border-border-default bg-surface-raised/80 px-4 py-3">
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-4 w-12" />
              <Skeleton className="h-4 w-12" />
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
      </div>
    </div>
  )
}

export function InspectorSkeleton() {
  return (
    <div className="flex h-full min-h-0 flex-col justify-between rounded-2xl bg-surface-raised p-5">
      <div>
        <Eyebrow>Inspector</Eyebrow>
        <FeatureTitle as="h3" className="mt-3">
          Pick a query row
        </FeatureTitle>
        <BodyText className="mt-3 max-w-md leading-6">
          The detail pane stays pinned on the right. Select any query to inspect latency by database
          and compare the SQL variants for only the databases currently included.
        </BodyText>
      </div>

      <div className="grid gap-3">
        <div className="rounded-xl border border-border-default bg-surface-inset px-4 py-3">
          <MetaLabel>Rows available</MetaLabel>
          <Skeleton className="mt-2 h-7 w-16" />
        </div>
        <div className="rounded-xl border border-border-default bg-surface-inset px-4 py-3">
          <MetaLabel>Active databases</MetaLabel>
          <Skeleton className="mt-2 h-7 w-14" />
        </div>
      </div>
    </div>
  )
}

export function TabsSkeleton() {
  return (
    <div className="flex gap-2">
      <Skeleton className="h-9 w-20 rounded-full" />
      <Skeleton className="h-9 w-20 rounded-full" />
    </div>
  )
}
