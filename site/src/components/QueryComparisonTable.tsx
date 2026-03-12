import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingFn,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table"
import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react"
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react"
import { createPortal } from "react-dom"

import type { SelectionState } from "../hooks/useSelectionState"
import { cn } from "../lib/cn"
import { formatDurationSeconds, formatMultiplier } from "../lib/format"
import { InlineDurationBars } from "./InlineDurationBars"

export interface QueryComparisonRow {
  query_name: string
  query_label: string
  category: string
  scale: string
  fastest_db: string
  spread_ratio: number
  by_database: Record<string, number | null>
}

interface QueryComparisonTableProps {
  rows: QueryComparisonRow[]
  databases: string[]
  databaseColors: Record<string, string>
  selection: SelectionState
  maxDuration: number
  containerClassName?: string
}

interface HeaderMeta {
  tooltip?: ReactNode
  tooltipLabel?: string
}

const columnHelper = createColumnHelper<QueryComparisonRow>()

const querySort: SortingFn<QueryComparisonRow> = (left, right) => {
  const labelDelta = left.original.query_label.localeCompare(right.original.query_label)
  if (labelDelta !== 0) return labelDelta

  const categoryDelta = left.original.category.localeCompare(right.original.category)
  if (categoryDelta !== 0) return categoryDelta

  return left.original.scale.localeCompare(right.original.scale)
}

const bestMedianSort: SortingFn<QueryComparisonRow> = (left, right) =>
  compareNullableNumbers(getBestMedian(left.original), getBestMedian(right.original))

export function QueryComparisonTable({
  rows,
  databases,
  databaseColors,
  selection,
  maxDuration,
  containerClassName,
}: QueryComparisonTableProps) {
  const [sorting, setSorting] = useState<SortingState>([{ id: "query", desc: false }])

  const columns = useMemo(
    () => [
      columnHelper.accessor("query_label", {
        id: "query",
        header: "Query",
        sortingFn: querySort,
        cell: (info) => (
          <div className="min-w-0">
            <p className="font-medium text-slate-100">{info.row.original.query_label}</p>
            <p className="mt-1 text-xs text-slate-500">
              {info.row.original.category} · {info.row.original.scale}
            </p>
          </div>
        ),
      }),
      columnHelper.display({
        id: "duration_bars",
        header: "Latency (log scale)",
        enableSorting: false,
        cell: (info) => (
          <InlineDurationBars
            byDatabase={info.row.original.by_database}
            databases={databases}
            databaseColors={databaseColors}
            maxDuration={maxDuration}
          />
        ),
      }),
      columnHelper.accessor("fastest_db", { header: "Fastest" }),
      columnHelper.accessor("spread_ratio", {
        header: "Gap",
        meta: {
          tooltip:
            "Slowest median divided by fastest median for this query across the selected databases. 1.0x means a tie. Higher values mean a wider latency gap.",
          tooltipLabel: "Explain gap",
        } satisfies HeaderMeta,
        cell: (info) => formatMultiplier(info.getValue()),
      }),
      columnHelper.accessor((row) => getBestMedian(row), {
        id: "best_duration",
        header: "Best",
        meta: {
          tooltip:
            "Lowest median query time across the selected databases. For each database we take the median of its recorded runs for this query, then keep the fastest median.",
          tooltipLabel: "Explain best median",
        } satisfies HeaderMeta,
        sortingFn: bestMedianSort,
        cell: (info) => {
          const bestMedian = info.getValue()
          if (bestMedian === null) return "—"
          return formatDurationSeconds(bestMedian)
        },
      }),
    ],
    [databases, databaseColors, maxDuration],
  )

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  })

  return (
    <div
      className={cn(
        "panel-scrollbar overflow-x-auto overflow-y-scroll rounded-2xl border border-slate-800 bg-slate-950/40",
        containerClassName,
      )}
    >
      <table className="w-full table-fixed text-left text-sm">
        <colgroup>
          <col className="w-[32%]" />
          <col className="w-[34%]" />
          <col className="w-[12%]" />
          <col className="w-[10%]" />
          <col className="w-[12%]" />
        </colgroup>
        <thead className="sticky top-0 z-10 bg-slate-900/95 text-slate-400 backdrop-blur">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const meta = header.column.columnDef.meta as HeaderMeta | undefined
                const headerContent = flexRender(
                  header.column.columnDef.header,
                  header.getContext(),
                )

                return (
                  <th key={header.id} className="px-4 py-3 font-medium">
                    {header.isPlaceholder ? null : header.column.getCanSort() ? (
                      <div className="flex min-w-0 items-center gap-1.5">
                        <button
                          type="button"
                          className="flex min-w-0 items-center gap-1.5 text-left transition-colors hover:text-slate-200"
                          onClick={header.column.getToggleSortingHandler()}
                        >
                          {meta?.tooltip ? (
                            <HeaderInfoTooltip
                              label={meta.tooltipLabel ?? `Explain ${header.id}`}
                              tooltip={meta.tooltip}
                            >
                              {headerContent}
                            </HeaderInfoTooltip>
                          ) : (
                            headerContent
                          )}
                          <SortIcon direction={header.column.getIsSorted()} />
                        </button>
                      </div>
                    ) : (
                      headerContent
                    )}
                  </th>
                )
              })}
            </tr>
          ))}
        </thead>
        <tbody className="divide-y divide-slate-800/60">
          {table.getRowModel().rows.map((row) => {
            const queryName = row.original.query_name
            const isSelected = selection.selectedQuery === queryName
            const highlighted = selection.isHighlighted(queryName)

            return (
              <tr
                key={row.id}
                className={cn(
                  "cursor-pointer transition-opacity duration-150",
                  highlighted ? "opacity-100" : "opacity-20",
                  isSelected
                    ? "bg-cyan-950/30 ring-1 ring-cyan-500/40 ring-inset"
                    : "hover:bg-slate-900/50",
                )}
                onMouseEnter={() => selection.setHoveredQuery(queryName)}
                onMouseLeave={() => selection.setHoveredQuery(null)}
                onClick={() => selection.toggleSelectedQuery(queryName)}
              >
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-4 py-3 align-top">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function getBestMedian(row: QueryComparisonRow): number | null {
  const values = Object.values(row.by_database).filter((value): value is number => value !== null)
  if (values.length === 0) return null
  return Math.min(...values)
}

function compareNullableNumbers(left: number | null, right: number | null): number {
  if (left === null && right === null) return 0
  if (left === null) return 1
  if (right === null) return -1
  return left - right
}

function SortIcon({ direction }: { direction: false | "asc" | "desc" }) {
  if (direction === "asc") {
    return <ArrowUp className="size-4 shrink-0 text-cyan-300" />
  }

  if (direction === "desc") {
    return <ArrowDown className="size-4 shrink-0 text-cyan-300" />
  }

  return <ArrowUpDown className="size-4 shrink-0 text-slate-500" />
}

interface HeaderWithTooltipProps {
  label: string
  tooltip: ReactNode
  children: ReactNode
}

interface TooltipPosition {
  left: number
  top: number
  placement: "top" | "bottom"
}

function HeaderInfoTooltip({ label, tooltip, children }: HeaderWithTooltipProps) {
  const triggerRef = useRef<HTMLSpanElement | null>(null)
  const [isOpen, setIsOpen] = useState(false)
  const [position, setPosition] = useState<TooltipPosition | null>(null)

  useEffect(() => {
    if (!isOpen) return

    function updatePosition() {
      const trigger = triggerRef.current
      if (!trigger) return

      const rect = trigger.getBoundingClientRect()
      const tooltipWidth = 288
      const margin = 12
      const gap = 10
      const left = clamp(
        rect.left + rect.width / 2 - tooltipWidth / 2,
        margin,
        window.innerWidth - tooltipWidth - margin,
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
  }, [isOpen])

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
      {isOpen && position
        ? createPortal(
            <div
              className={cn(
                "pointer-events-none fixed z-[80] w-72 max-w-[calc(100vw-1.5rem)] rounded-2xl border border-slate-700 bg-slate-950/98 px-3 py-2 text-xs leading-5 text-slate-200 shadow-[0_20px_50px_rgba(2,6,23,0.55)]",
                position.placement === "top" ? "-translate-y-full" : undefined,
              )}
              style={{ left: position.left, top: position.top }}
            >
              {tooltip}
            </div>,
            document.body,
          )
        : null}
    </>
  )
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}
