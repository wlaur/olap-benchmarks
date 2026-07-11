import { Check, ChevronDown, Code2, Search, X } from "lucide-react"
import {
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent,
  type ReactNode,
} from "react"
import { createPortal } from "react-dom"

import { cn } from "../../lib/cn"

export interface VirtualizedSelectOption {
  value: string
  label: string
}

interface VirtualizedSelectProps {
  ariaLabel: string
  label: string
  value: string
  onChange: (value: string) => void
  options: readonly VirtualizedSelectOption[]
  icon?: ReactNode
  filterPlaceholder?: string
  disabled?: boolean
  className?: string
}

const ITEM_HEIGHT = 42
const MAX_LIST_HEIGHT = ITEM_HEIGHT * 7
const OVERSCAN = 3

export function VirtualizedSelect({
  ariaLabel,
  label,
  value,
  onChange,
  options,
  icon,
  filterPlaceholder = "Filter options",
  disabled = false,
  className,
}: VirtualizedSelectProps) {
  const listId = useId()
  const triggerRef = useRef<HTMLButtonElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)
  const [open, setOpen] = useState(false)
  const [filter, setFilter] = useState("")
  const [activeIndex, setActiveIndex] = useState(0)
  const [scrollTop, setScrollTop] = useState(0)
  const [menuStyle, setMenuStyle] = useState<CSSProperties | null>(null)
  const selectedOption = options.find((option) => option.value === value)
  const filteredOptions = useMemo(() => {
    const normalizedFilter = filter.trim().toLocaleLowerCase()
    if (!normalizedFilter) return options
    return options.filter((option) =>
      `${option.label} ${option.value}`.toLocaleLowerCase().includes(normalizedFilter),
    )
  }, [filter, options])
  const listHeight = Math.min(MAX_LIST_HEIGHT, Math.max(ITEM_HEIGHT * filteredOptions.length, 88))
  const visibleStart = Math.max(0, Math.floor(scrollTop / ITEM_HEIGHT) - OVERSCAN)
  const visibleEnd = Math.min(
    filteredOptions.length,
    Math.ceil((scrollTop + listHeight) / ITEM_HEIGHT) + OVERSCAN,
  )
  const visibleOptions = filteredOptions.slice(visibleStart, visibleEnd)

  useEffect(() => {
    if (!open) return
    const selectedIndex = filteredOptions.findIndex((option) => option.value === value)
    setActiveIndex(Math.max(0, selectedIndex))
    setScrollTop(0)
    if (listRef.current) listRef.current.scrollTop = 0
  }, [filter, open, value, filteredOptions])

  useEffect(() => {
    if (!open) return

    function updatePosition() {
      const trigger = triggerRef.current
      if (!trigger) return
      const rect = trigger.getBoundingClientRect()
      const viewportPadding = 8
      const menuWidth = Math.min(Math.max(rect.width, 320), window.innerWidth - viewportPadding * 2)
      const menuHeight = 58 + listHeight + 34
      const spaceBelow = window.innerHeight - rect.bottom - viewportPadding
      const spaceAbove = rect.top - viewportPadding
      const opensBelow = spaceBelow >= menuHeight || spaceBelow >= spaceAbove
      const left = Math.min(
        Math.max(viewportPadding, rect.left),
        window.innerWidth - menuWidth - viewportPadding,
      )
      const top = opensBelow
        ? rect.bottom + 8
        : Math.max(viewportPadding, rect.top - menuHeight - 8)
      setMenuStyle({ left, top, width: menuWidth })
    }

    updatePosition()
    window.addEventListener("resize", updatePosition)
    window.addEventListener("scroll", updatePosition, true)
    return () => {
      window.removeEventListener("resize", updatePosition)
      window.removeEventListener("scroll", updatePosition, true)
    }
  }, [listHeight, open])

  useEffect(() => {
    if (!open) return
    const frame = window.requestAnimationFrame(() => searchRef.current?.focus())

    function handlePointerDown(event: PointerEvent) {
      const target = event.target as Node
      if (!triggerRef.current?.contains(target) && !menuRef.current?.contains(target)) {
        setOpen(false)
      }
    }

    document.addEventListener("pointerdown", handlePointerDown)
    return () => {
      window.cancelAnimationFrame(frame)
      document.removeEventListener("pointerdown", handlePointerDown)
    }
  }, [open])

  useEffect(() => {
    const list = listRef.current
    if (!open || !list || filteredOptions.length === 0) return
    const itemTop = activeIndex * ITEM_HEIGHT
    const itemBottom = itemTop + ITEM_HEIGHT
    if (itemTop < list.scrollTop) list.scrollTop = itemTop
    else if (itemBottom > list.scrollTop + list.clientHeight) {
      list.scrollTop = itemBottom - list.clientHeight
    }
  }, [activeIndex, filteredOptions.length, open])

  function openMenu() {
    if (disabled) return
    setFilter("")
    setOpen(true)
  }

  function closeMenu({ restoreFocus = true } = {}) {
    setOpen(false)
    if (restoreFocus) window.requestAnimationFrame(() => triggerRef.current?.focus())
  }

  function selectOption(option: VirtualizedSelectOption) {
    onChange(option.value)
    closeMenu()
  }

  function handleTriggerKeyDown(event: KeyboardEvent<HTMLButtonElement>) {
    if (event.key === "ArrowDown" || event.key === "Enter" || event.key === " ") {
      event.preventDefault()
      openMenu()
    }
  }

  function handleSearchKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault()
      setActiveIndex((index) => Math.min(filteredOptions.length - 1, index + 1))
    } else if (event.key === "ArrowUp") {
      event.preventDefault()
      setActiveIndex((index) => Math.max(0, index - 1))
    } else if (event.key === "Enter") {
      event.preventDefault()
      const option = filteredOptions[activeIndex]
      if (option) selectOption(option)
    } else if (event.key === "Escape") {
      event.preventDefault()
      closeMenu()
    } else if (event.key === "Tab") {
      closeMenu({ restoreFocus: false })
    }
  }

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        role="combobox"
        aria-label={ariaLabel}
        aria-expanded={open}
        aria-controls={listId}
        aria-haspopup="listbox"
        disabled={disabled}
        onClick={() => (open ? closeMenu() : openMenu())}
        onKeyDown={handleTriggerKeyDown}
        className={cn(
          "group flex min-h-11 min-w-0 items-center gap-3 rounded-md border border-border-strong bg-surface-primary px-3 text-left transition-colors outline-none",
          "hover:border-slate-500 hover:bg-surface-inset focus-visible:border-accent-400/70 focus-visible:ring-2 focus-visible:ring-accent-400/15 disabled:cursor-not-allowed disabled:opacity-50",
          open && "border-accent-400/60 ring-2 ring-accent-400/10",
          className,
        )}
      >
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded border border-border-default bg-surface-raised text-accent-300">
          {icon ?? <Code2 className="h-3.5 w-3.5" strokeWidth={1.8} />}
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[0.625rem] font-semibold tracking-[0.16em] text-slate-500 uppercase">
            {label}
          </span>
          <span
            className="block truncate text-sm font-medium text-slate-100"
            title={selectedOption?.label}
          >
            {selectedOption?.label ?? "Select an option"}
          </span>
        </span>
        <span className="hidden shrink-0 text-[0.6875rem] text-slate-500 sm:inline">
          {options.length} options
        </span>
        <ChevronDown
          className={cn(
            "h-4 w-4 shrink-0 text-slate-500 transition-transform",
            open && "rotate-180",
          )}
          strokeWidth={1.8}
        />
      </button>

      {open && menuStyle
        ? createPortal(
            <div
              ref={menuRef}
              style={menuStyle}
              className="animate-panel-enter fixed z-50 overflow-hidden rounded-lg border border-border-strong bg-surface-primary shadow-[0_20px_60px_rgba(0,0,0,0.65)]"
            >
              <label className="flex h-[58px] items-center gap-2 border-b border-border-default px-3 text-slate-400 focus-within:text-slate-200">
                <Search className="h-4 w-4 shrink-0" strokeWidth={1.8} />
                <span className="sr-only">{filterPlaceholder}</span>
                <input
                  ref={searchRef}
                  type="text"
                  role="searchbox"
                  aria-label={filterPlaceholder}
                  aria-controls={listId}
                  aria-activedescendant={
                    filteredOptions[activeIndex] ? `${listId}-option-${activeIndex}` : undefined
                  }
                  value={filter}
                  onChange={(event) => setFilter(event.target.value)}
                  onKeyDown={handleSearchKeyDown}
                  placeholder={filterPlaceholder}
                  className="min-w-0 flex-1 bg-transparent text-sm text-slate-100 outline-none placeholder:text-slate-600"
                />
                {filter ? (
                  <button
                    type="button"
                    aria-label="Clear filter"
                    onClick={() => setFilter("")}
                    className="flex h-7 w-7 shrink-0 items-center justify-center rounded text-slate-500 hover:bg-surface-raised hover:text-slate-200"
                  >
                    <X className="h-3.5 w-3.5" strokeWidth={1.8} />
                  </button>
                ) : null}
              </label>

              <div
                ref={listRef}
                id={listId}
                role="listbox"
                aria-label={ariaLabel}
                className="panel-scrollbar overflow-y-auto overscroll-contain"
                style={{ height: listHeight }}
                onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
              >
                {filteredOptions.length > 0 ? (
                  <div
                    className="relative"
                    style={{ height: filteredOptions.length * ITEM_HEIGHT }}
                  >
                    {visibleOptions.map((option, visibleIndex) => {
                      const optionIndex = visibleStart + visibleIndex
                      const selected = option.value === value
                      const active = optionIndex === activeIndex
                      return (
                        <button
                          key={option.value}
                          id={`${listId}-option-${optionIndex}`}
                          type="button"
                          role="option"
                          aria-selected={selected}
                          title={option.label}
                          onPointerMove={() => setActiveIndex(optionIndex)}
                          onClick={() => selectOption(option)}
                          className={cn(
                            "absolute left-0 flex w-full min-w-0 items-center gap-2 border-l-2 px-3 text-left text-sm outline-none",
                            selected
                              ? "border-accent-400 bg-accent-400/10 text-slate-50"
                              : active
                                ? "border-transparent bg-surface-raised text-slate-100"
                                : "border-transparent text-slate-400",
                          )}
                          style={{ top: optionIndex * ITEM_HEIGHT, height: ITEM_HEIGHT }}
                        >
                          <Code2
                            className={cn(
                              "h-3.5 w-3.5 shrink-0",
                              selected ? "text-accent-300" : "text-slate-600",
                            )}
                            strokeWidth={1.8}
                          />
                          <span className="min-w-0 flex-1 truncate">
                            <HighlightedMatch text={option.label} query={filter} />
                          </span>
                          {selected ? (
                            <Check
                              className="h-3.5 w-3.5 shrink-0 text-accent-300"
                              strokeWidth={2}
                            />
                          ) : null}
                        </button>
                      )
                    })}
                  </div>
                ) : (
                  <div className="flex h-full items-center justify-center px-4 text-sm text-slate-500">
                    No matching options
                  </div>
                )}
              </div>

              <div className="flex h-[34px] items-center justify-between border-t border-border-default px-3 text-[0.6875rem] text-slate-500">
                <span>
                  {filteredOptions.length}/{options.length} matches
                </span>
                <span className="hidden sm:inline">↑↓ navigate · enter select · esc close</span>
              </div>
            </div>,
            document.body,
          )
        : null}
    </>
  )
}

function HighlightedMatch({ text, query }: { text: string; query: string }) {
  const normalizedQuery = query.trim()
  const matchIndex = text.toLocaleLowerCase().indexOf(normalizedQuery.toLocaleLowerCase())
  if (!normalizedQuery || matchIndex < 0) return text

  const matchEnd = matchIndex + normalizedQuery.length
  return (
    <>
      {text.slice(0, matchIndex)}
      <mark className="rounded-sm bg-accent-400/20 px-px text-accent-200">
        {text.slice(matchIndex, matchEnd)}
      </mark>
      {text.slice(matchEnd)}
    </>
  )
}
