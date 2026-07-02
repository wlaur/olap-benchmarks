import {
  useEffect,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
  type RefObject,
} from "react"
import { createPortal } from "react-dom"

import { cn } from "../../lib/cn"
import { clamp } from "../../lib/format"

interface PortalCardProps {
  className?: string
  style?: CSSProperties
  ref?: RefObject<HTMLDivElement | null>
  children: ReactNode
}

export function PortalCard({ className, style, ref, children }: PortalCardProps) {
  return createPortal(
    <div
      ref={ref}
      className={cn(
        "fixed z-[80] rounded-xl border border-border-default bg-surface-primary/98 text-slate-200 shadow-2xl",
        className,
      )}
      style={style}
    >
      {children}
    </div>,
    document.body,
  )
}

export interface AnchoredPosition {
  left: number
  top: number
  placement: "top" | "bottom"
}

interface AnchoredPositionOptions {
  align: "center" | "start"
  width?: number
  gap?: number
}

export function useAnchoredPosition(
  isOpen: boolean,
  triggerRef: RefObject<HTMLElement | null>,
  { align, width = 288, gap = 10 }: AnchoredPositionOptions,
): AnchoredPosition | null {
  const [position, setPosition] = useState<AnchoredPosition | null>(null)

  useEffect(() => {
    if (!isOpen) {
      setPosition(null)
      return
    }

    function updatePosition() {
      const trigger = triggerRef.current
      if (!trigger) return

      const rect = trigger.getBoundingClientRect()

      if (align === "start") {
        setPosition({ left: rect.left, top: rect.bottom + gap, placement: "bottom" })
        return
      }

      const margin = 12
      const left = clamp(
        rect.left + rect.width / 2 - width / 2,
        margin,
        window.innerWidth - width - margin,
      )
      const placement = rect.top > 120 ? "top" : "bottom"

      setPosition({
        left,
        top: placement === "top" ? rect.top - gap : rect.bottom + gap,
        placement,
      })
    }

    updatePosition()
    window.addEventListener("resize", updatePosition)
    window.addEventListener("scroll", updatePosition, true)

    return () => {
      window.removeEventListener("resize", updatePosition)
      window.removeEventListener("scroll", updatePosition, true)
    }
  }, [isOpen, triggerRef, align, width, gap])

  return position
}

export function useDismissable(
  isOpen: boolean,
  onDismiss: () => void,
  triggerRef: RefObject<HTMLElement | null>,
  popoverRef: RefObject<HTMLElement | null>,
): void {
  useEffect(() => {
    if (!isOpen) return

    function handleMouseDown(event: MouseEvent) {
      const target = event.target as Node
      if (triggerRef.current?.contains(target) || popoverRef.current?.contains(target)) return
      onDismiss()
    }
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") onDismiss()
    }

    window.addEventListener("mousedown", handleMouseDown)
    window.addEventListener("keydown", handleKeyDown)

    return () => {
      window.removeEventListener("mousedown", handleMouseDown)
      window.removeEventListener("keydown", handleKeyDown)
    }
  }, [isOpen, onDismiss, triggerRef, popoverRef])
}

interface InfoTooltipProps {
  label: string
  content: ReactNode
  width?: number
  children: ReactNode
}

export function InfoTooltip({ label, content, width = 288, children }: InfoTooltipProps) {
  const triggerRef = useRef<HTMLSpanElement | null>(null)
  const [isOpen, setIsOpen] = useState(false)
  const position = useAnchoredPosition(isOpen, triggerRef, { align: "center", width })

  return (
    <>
      <span
        ref={triggerRef}
        aria-label={label}
        onMouseEnter={() => setIsOpen(true)}
        onMouseLeave={() => setIsOpen(false)}
        className="transition-colors hover:text-slate-100"
      >
        {children}
      </span>
      {isOpen && position ? (
        <PortalCard
          className={cn(
            "pointer-events-none w-72 max-w-[calc(100vw-1.5rem)] px-3 py-2 text-xs leading-5 shadow-[0_20px_50px_rgba(0,0,0,0.4)]",
            position.placement === "top" ? "-translate-y-full" : undefined,
          )}
          style={{ left: position.left, top: position.top }}
        >
          {content}
        </PortalCard>
      ) : null}
    </>
  )
}
