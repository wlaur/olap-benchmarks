import type { ElementType, ReactNode } from "react"

import { cn } from "../lib/cn"

interface TextProps {
  as?: ElementType
  children: ReactNode
  className?: string
  id?: string
}

function Text({ as, children, className, id }: TextProps) {
  const Tag = as ?? "p"

  return (
    <Tag className={className} id={id}>
      {children}
    </Tag>
  )
}

export function MetaLabel({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as}
      className={cn("text-sm leading-5 font-medium text-slate-400", className)}
      {...props}
    />
  )
}

export function DisplayTitle({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as}
      className={cn("text-3xl font-semibold tracking-tight text-balance text-slate-50", className)}
      {...props}
    />
  )
}

export function SectionTitle({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as}
      className={cn("text-xl leading-7 font-semibold tracking-tight text-slate-50", className)}
      {...props}
    />
  )
}

export function ItemTitle({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as ?? "span"}
      className={cn("text-base leading-6 font-semibold text-slate-50", className)}
      {...props}
    />
  )
}

export function LeadText({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as}
      className={cn("max-w-[70ch] text-lg leading-7 text-pretty text-slate-50", className)}
      {...props}
    />
  )
}

export function BodyText({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as}
      className={cn("max-w-[75ch] text-base leading-7 text-slate-200", className)}
      {...props}
    />
  )
}

export function SecondaryText({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as}
      className={cn("max-w-[75ch] text-sm leading-6 text-slate-300", className)}
      {...props}
    />
  )
}

export function NumericText({ as, className, ...props }: TextProps) {
  return (
    <Text
      as={as ?? "span"}
      className={cn("font-mono text-sm text-slate-100 tabular-nums", className)}
      {...props}
    />
  )
}

export function CaptionText({ as, className, ...props }: TextProps) {
  return <Text as={as} className={cn("text-sm leading-5 text-slate-400", className)} {...props} />
}
