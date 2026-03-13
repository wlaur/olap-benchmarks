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
      className={cn("text-sm font-medium tracking-[0.18em] text-cyan-300 uppercase", className)}
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
      className={cn("text-xs font-medium tracking-[0.18em] text-slate-500 uppercase", className)}
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
    <Text as={as} className={cn("text-lg font-semibold text-slate-50", className)}>
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
    <Text as={as} className={cn("text-2xl font-semibold text-slate-50", className)}>
      {children}
    </Text>
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
    <Text as={as} className={cn("text-sm text-slate-400", className)}>
      {children}
    </Text>
  )
}
