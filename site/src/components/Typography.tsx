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
