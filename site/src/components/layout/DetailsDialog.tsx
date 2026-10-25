import { X } from "lucide-react"
import { useEffect, useId, useRef, useState, type ReactNode } from "react"

import { cn } from "../../lib/cn"
import { QuietButton } from "../controls/Control"
import { SectionTitle } from "../Typography"

export function DetailsDialog({
  title,
  trigger,
  triggerLabel,
  children,
  className,
  triggerClassName,
  open: controlledOpen,
  onOpenChange,
}: {
  title: string
  trigger: ReactNode
  triggerLabel?: string
  children: ReactNode
  className?: string
  triggerClassName?: string
  open?: boolean
  onOpenChange?: (open: boolean) => void
}) {
  const titleId = useId()
  const dialogRef = useRef<HTMLDialogElement>(null)
  const triggerRef = useRef<HTMLButtonElement>(null)
  const [internalOpen, setInternalOpen] = useState(false)
  const open = controlledOpen ?? internalOpen
  const restoreFocusRef = useRef<HTMLElement | null>(null)

  function setOpen(next: boolean) {
    setInternalOpen(next)
    onOpenChange?.(next)
  }

  useEffect(() => {
    const dialog = dialogRef.current
    if (!dialog) return
    if (open && !dialog.open) {
      restoreFocusRef.current =
        document.activeElement instanceof HTMLElement ? document.activeElement : null
      dialog.showModal()
    } else if (!open && dialog.open) dialog.close()
  }, [open])

  function show() {
    setOpen(true)
  }

  function close() {
    dialogRef.current?.close()
  }

  return (
    <>
      <QuietButton
        ref={triggerRef}
        size="xs"
        aria-haspopup="dialog"
        aria-label={triggerLabel}
        onClick={show}
        className={triggerClassName}
      >
        {trigger}
      </QuietButton>
      <dialog
        ref={dialogRef}
        aria-labelledby={titleId}
        onClose={() => {
          setOpen(false)
          const active = document.activeElement
          if (
            active === document.body ||
            active === restoreFocusRef.current ||
            (active && dialogRef.current?.contains(active))
          )
            (restoreFocusRef.current ?? triggerRef.current)?.focus()
        }}
        onClick={(event) => {
          if (event.target === event.currentTarget) close()
        }}
        className={cn(
          "m-auto max-h-[min(88dvh,60rem)] w-[min(calc(100%-1.5rem),56rem)] overflow-hidden rounded-lg border border-border-strong bg-surface-raised p-0 text-slate-100 shadow-2xl shadow-black/70 backdrop:bg-black/70",
          className,
        )}
      >
        <div className="flex max-h-[inherit] flex-col">
          <header className="flex shrink-0 items-start justify-between gap-3 border-b border-border-subtle px-4 py-3 sm:px-5">
            <SectionTitle as="h2" id={titleId} className="text-lg">
              {title}
            </SectionTitle>
            <QuietButton
              size="xs"
              aria-label="Close"
              onClick={close}
              className="size-9 shrink-0 px-0"
              autoFocus
            >
              <X aria-hidden="true" className="size-4" />
            </QuietButton>
          </header>
          <div className="panel-scrollbar min-h-0 flex-1 space-y-4 overflow-y-auto px-4 py-4 sm:px-5">
            {open ? children : null}
          </div>
        </div>
      </dialog>
    </>
  )
}
