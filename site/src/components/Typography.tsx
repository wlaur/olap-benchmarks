import type { ElementType, ReactNode } from "react"

import { cn } from "../lib/cn"

function Text({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  const Tag = as ?? "p"

  return <Tag className={className}>{children}</Tag>
}

export function Eyebrow({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  return (
    <Text
      as={as}
      className={cn("text-xs font-semibold tracking-[0.14em] text-accent-300 uppercase", className)}
    >
      {children}
    </Text>
  )
}

export function MetaLabel({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  return (
    <Text
      as={as}
      className={cn(
        "text-[0.6875rem] font-semibold tracking-[0.13em] text-slate-400 uppercase",
        className,
      )}
    >
      {children}
    </Text>
  )
}

export function DisplayTitle({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  return (
    <Text as={as} className={cn("text-3xl font-semibold tracking-tight text-slate-50", className)}>
      {children}
    </Text>
  )
}

export function SectionTitle({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  return (
    <Text as={as} className={cn("text-lg font-semibold tracking-tight text-slate-50", className)}>
      {children}
    </Text>
  )
}

export function FeatureTitle({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  return (
    <Text as={as} className={cn("text-2xl font-semibold tracking-tight text-slate-50", className)}>
      {children}
    </Text>
  )
}

export function SectionDivider({
  title,
  description,
  trailing,
  className,
}: {
  title: string
  description?: string
  trailing?: ReactNode
  className?: string
}) {
  return (
    <div className={cn("flex flex-wrap items-baseline justify-between gap-3", className)}>
      <div className="flex flex-wrap items-baseline gap-3">
        <h2 className="text-[11px] font-semibold tracking-[0.18em] text-slate-300 uppercase">
          {title}
        </h2>
        {description ? <p className="font-sans text-xs text-slate-400">{description}</p> : null}
      </div>
      {trailing ? <div className="flex items-center gap-2">{trailing}</div> : null}
    </div>
  )
}

export function BodyText({
  as,
  children,
  className,
}: {
  as?: ElementType
  children: ReactNode
  className?: string
}) {
  return (
    <Text as={as} className={cn("font-sans text-sm text-slate-300", className)}>
      {children}
    </Text>
  )
}
