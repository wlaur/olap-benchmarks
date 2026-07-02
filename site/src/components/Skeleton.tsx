import { cn } from "../lib/cn"

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("animate-skeleton rounded-xl bg-slate-800/40", className)} />
}

export function SkeletonChips({ widths, className }: { widths: string[]; className?: string }) {
  return (
    <div className={cn("flex flex-wrap gap-1.5", className)}>
      {widths.map((width, index) => (
        <Skeleton key={index} className={cn("h-6 rounded-full", width)} />
      ))}
    </div>
  )
}
