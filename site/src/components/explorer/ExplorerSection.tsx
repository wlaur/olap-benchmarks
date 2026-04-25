import type { ReactNode } from "react"

import { cn } from "../../lib/cn"
import { SectionDivider } from "../Typography"

interface ExplorerSectionProps {
  title: string
  description?: string
  trailing?: ReactNode
  children: ReactNode
  className?: string
}

export function ExplorerSection({
  title,
  description,
  trailing,
  children,
  className,
}: ExplorerSectionProps) {
  return (
    <section className={cn("space-y-3", className)}>
      <SectionDivider title={title} description={description} trailing={trailing} />
      {children}
    </section>
  )
}
