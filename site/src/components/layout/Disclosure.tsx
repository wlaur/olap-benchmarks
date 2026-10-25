import { ChevronDown } from "lucide-react"
import { useState, type ReactNode } from "react"

import { cn } from "../../lib/cn"

export function Disclosure({
  title,
  meta,
  children,
  className,
  contentClassName,
  lazy = false,
}: {
  title: ReactNode
  meta?: ReactNode
  children: ReactNode
  className?: string
  contentClassName?: string
  lazy?: boolean
}) {
  const [open, setOpen] = useState(false)
  return (
    <details
      className={cn("group/disclosure min-w-0", className)}
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary className="flex min-h-10 cursor-pointer list-none items-center gap-2 rounded-md py-1.5 text-sm font-medium text-slate-200 outline-none hover:text-slate-50 focus-visible:ring-2 focus-visible:ring-accent-300/60 [&::-webkit-details-marker]:hidden">
        <ChevronDown
          aria-hidden="true"
          className="size-4 shrink-0 -rotate-90 text-slate-400 transition-transform group-open/disclosure:rotate-0"
        />
        <span className="min-w-0 flex-1">{title}</span>
        {meta ? <span className="shrink-0 text-sm text-slate-400">{meta}</span> : null}
      </summary>
      <div className={cn("min-w-0 space-y-3 pt-2 pb-1 pl-6", contentClassName)}>
        {!lazy || open ? children : null}
      </div>
    </details>
  )
}
