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
import { useMemo, useState } from "react"

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
        header: "Spread",
        cell: (info) => formatMultiplier(info.getValue()),
      }),
      columnHelper.accessor((row) => getBestMedian(row), {
        id: "best_duration",
        header: "Best Median",
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
              {headerGroup.headers.map((header) => (
                <th key={header.id} className="px-4 py-3 font-medium">
                  {header.isPlaceholder ? null : header.column.getCanSort() ? (
                    <button
                      type="button"
                      className="inline-flex items-center gap-1.5 text-left transition-colors hover:text-slate-200"
                      onClick={header.column.getToggleSortingHandler()}
                    >
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      <SortIcon direction={header.column.getIsSorted()} />
                    </button>
                  ) : (
                    flexRender(header.column.columnDef.header, header.getContext())
                  )}
                </th>
              ))}
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
    return <ArrowUp className="size-3.5 text-cyan-300" />
  }

  if (direction === "desc") {
    return <ArrowDown className="size-3.5 text-cyan-300" />
  }

  return <ArrowUpDown className="size-3.5 text-slate-500" />
}
