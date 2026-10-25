import * as Select from "@radix-ui/react-select"
import { Check, ChevronDown, ChevronUp } from "lucide-react"
import { useRef, useState, type ReactNode } from "react"

import { cn } from "../../lib/cn"
import { Skeleton } from "../Skeleton"

interface ControlSelectOption {
  value: string
  label: string
}

interface ControlSelectProps {
  ariaLabel: string
  label: string
  value: string
  onChange: (value: string) => void
  options: ControlSelectOption[]
  icon: ReactNode
  disabled?: boolean
  className?: string
  menuClassName?: string
  labelMode?: "responsive" | "always" | "hidden"
}

const TRIGGER_CLASS =
  "inline-flex min-h-10 min-w-0 items-center justify-between gap-2 rounded-md border border-border-default bg-surface-primary/80 py-1 pr-2.5 pl-1.5 text-left text-slate-100 transition-colors outline-none hover:border-slate-500 focus-visible:border-accent-300 focus-visible:ring-2 focus-visible:ring-accent-300/60 disabled:cursor-not-allowed disabled:opacity-60"

const CONTENT_CLASS =
  "z-50 max-h-80 w-[var(--radix-select-trigger-width)] max-w-[var(--radix-select-content-available-width)] overflow-hidden rounded-lg border border-border-strong bg-surface-primary p-1.5 text-slate-100 shadow-2xl shadow-black/60 data-[side=bottom]:translate-y-1 data-[side=top]:-translate-y-1"

const ITEM_CLASS =
  "relative flex cursor-default items-center rounded-md min-h-10 py-2 pr-8 pl-3 text-sm font-medium text-slate-200 transition outline-none data-[highlighted]:bg-surface-raised data-[highlighted]:text-slate-100 data-[state=checked]:bg-accent-400/10 data-[state=checked]:text-slate-50"

export function ControlSelect({
  ariaLabel,
  label,
  value,
  onChange,
  options,
  icon,
  disabled = false,
  className,
  menuClassName,
  labelMode = "responsive",
}: ControlSelectProps) {
  const triggerRef = useRef<HTMLButtonElement>(null)
  const [portalContainer, setPortalContainer] = useState<HTMLElement | undefined>(undefined)
  const labelClass =
    labelMode === "always" ? "inline" : labelMode === "responsive" ? "hidden md:inline" : "hidden"
  const dividerClass = labelClass

  return (
    <Select.Root
      value={value}
      onValueChange={onChange}
      disabled={disabled}
      onOpenChange={(open) => {
        if (open) setPortalContainer(triggerRef.current?.closest("dialog") ?? undefined)
      }}
    >
      <Select.Trigger
        ref={triggerRef}
        aria-label={ariaLabel}
        className={cn(TRIGGER_CLASS, className)}
      >
        <span className="flex min-w-0 flex-1 items-center gap-2">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded border border-border-subtle bg-surface-raised text-slate-300">
            {icon}
          </span>
          <span className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1">
            <span className={cn("shrink-0 text-sm font-medium text-slate-400", labelClass)}>
              {label}
            </span>
            <span className={cn("h-3 w-px shrink-0 bg-slate-700/50", dividerClass)} />
            <Select.Value className="block min-w-0 flex-1 text-sm font-medium [overflow-wrap:anywhere] whitespace-normal text-slate-100" />
          </span>
        </span>
        <Select.Icon className="shrink-0 text-slate-400">
          <ChevronDown className="h-3.5 w-3.5" strokeWidth={1.8} />
        </Select.Icon>
      </Select.Trigger>

      <Select.Portal container={portalContainer}>
        <Select.Content
          position="popper"
          sideOffset={10}
          className={cn(CONTENT_CLASS, menuClassName)}
        >
          <Select.ScrollUpButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronUp className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollUpButton>
          <Select.Viewport className="space-y-1">
            {options.map((option) => (
              <Select.Item key={option.value} value={option.value} className={ITEM_CLASS}>
                <span className="min-w-0 [overflow-wrap:anywhere] whitespace-normal">
                  <Select.ItemText>{option.label}</Select.ItemText>
                </span>
                <Select.ItemIndicator className="absolute right-3 inline-flex items-center text-slate-300">
                  <Check className="h-4 w-4" strokeWidth={2} />
                </Select.ItemIndicator>
              </Select.Item>
            ))}
          </Select.Viewport>
          <Select.ScrollDownButton className="flex h-8 items-center justify-center text-slate-500">
            <ChevronDown className="h-4 w-4" strokeWidth={1.8} />
          </Select.ScrollDownButton>
        </Select.Content>
      </Select.Portal>
    </Select.Root>
  )
}

interface ControlSelectSkeletonProps {
  label: string
  className?: string
  labelMode?: "responsive" | "always" | "hidden"
}

export function ControlSelectSkeleton({
  label,
  className,
  labelMode = "responsive",
}: ControlSelectSkeletonProps) {
  const labelClass =
    labelMode === "always" ? "inline" : labelMode === "responsive" ? "hidden md:inline" : "hidden"
  const dividerClass = labelClass

  return (
    <div className={cn(TRIGGER_CLASS, className)} aria-hidden="true">
      <span className="flex min-w-0 flex-1 items-center gap-2">
        <Skeleton className="h-6 w-6 shrink-0 rounded-full" />
        <span className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1">
          <span className={cn("shrink-0 text-sm font-medium text-slate-400", labelClass)}>
            {label}
          </span>
          <span className={cn("h-3 w-px shrink-0 bg-slate-700/50", dividerClass)} />
          <Skeleton className="h-3 w-24 rounded-full sm:w-28" />
        </span>
      </span>
      <Skeleton className="h-3.5 w-3.5 shrink-0 rounded-full" />
    </div>
  )
}
