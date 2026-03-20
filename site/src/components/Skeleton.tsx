import { cn } from "../lib/cn"

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("animate-skeleton rounded-xl bg-slate-800/40", className)} />
}
