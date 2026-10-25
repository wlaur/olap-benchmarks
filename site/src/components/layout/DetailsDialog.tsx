import { X } from "lucide-react"
import { useEffect, useId, useRef, useState, type KeyboardEvent, type ReactNode } from "react"

import { cn } from "../../lib/cn"
import { QuietButton } from "../controls/Control"
import { SectionTitle } from "../Typography"

const TABBABLE =
  'a[href], area[href], button, input, select, textarea, summary, iframe, [contenteditable]:not([contenteditable="false"]), [tabindex]'

function tabbablesIn(container: HTMLElement) {
  return [...container.querySelectorAll<HTMLElement>(TABBABLE)].filter(
    (element) =>
      element.tabIndex >= 0 &&
      !element.matches(":disabled") &&
      !element.closest("[inert]") &&
      element.checkVisibility({ visibilityProperty: true }),
  )
}

function popupAnchor(dialog: HTMLElement, active: HTMLElement) {
  const listId = active.getAttribute("aria-controls")
  if (!listId) return null
  const combobox = dialog.querySelector<HTMLElement>(
    `[role="combobox"][aria-controls="${CSS.escape(listId)}"]`,
  )
  if (!combobox || combobox === active) return null
  const list = document.getElementById(listId)
  let popup: HTMLElement | null = active
  while (popup && list && !popup.contains(list)) popup = popup.parentElement
  return popup && popup !== dialog ? { combobox, popup } : null
}

function containTab(event: KeyboardEvent<HTMLDialogElement>) {
  if (event.key !== "Tab" || event.defaultPrevented || event.altKey || event.ctrlKey) return
  const dialog = event.currentTarget
  const active = document.activeElement
  if (!(active instanceof HTMLElement) || !dialog.contains(active)) return
  const anchor = popupAnchor(dialog, active)
  const tabbables = tabbablesIn(dialog).filter((element) => !anchor?.popup.contains(element))
  if (tabbables.length === 0) {
    event.preventDefault()
    return
  }
  const reference = anchor?.combobox ?? active
  if (anchor && event.shiftKey) {
    event.preventDefault()
    reference.focus()
    return
  }
  const following = event.shiftKey
    ? tabbables.filter(
        (element) => reference.compareDocumentPosition(element) & Node.DOCUMENT_POSITION_PRECEDING,
      )
    : tabbables.filter(
        (element) => reference.compareDocumentPosition(element) & Node.DOCUMENT_POSITION_FOLLOWING,
      )
  if (following.length > 0 && !anchor) return
  event.preventDefault()
  ;(following[0] ?? (event.shiftKey ? tabbables.at(-1) : tabbables[0]))?.focus()
}

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
        onKeyDown={containTab}
        className={cn(
          "m-auto max-h-[min(88dvh,60rem)] w-[min(calc(100%-1.5rem),56rem)] overflow-hidden rounded-lg border border-border-strong bg-surface-raised p-0 text-left text-slate-100 shadow-2xl shadow-black/70 backdrop:bg-black/70",
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
