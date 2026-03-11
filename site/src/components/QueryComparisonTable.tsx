import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table"
import { useMemo } from "react"

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

export function QueryComparisonTable({
  rows,
  databases,
  databaseColors,
  selection,
  maxDuration,
  containerClassName,
}: QueryComparisonTableProps) {
  const columns = useMemo(
    () => [
      columnHelper.display({
        id: "query",
        header: "Query",
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
      columnHelper.display({
        id: "best_duration",
        header: "Best Median",
        cell: (info) => {
          const values = Object.values(info.row.original.by_database).filter(
            (value): value is number => value !== null,
          )

          if (values.length === 0) return "—"
          return formatDurationSeconds(Math.min(...values))
        },
      }),
    ],
    [databases, databaseColors, maxDuration],
  )

  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <div
      className={cn(
        "overflow-x-hidden overflow-y-auto rounded-2xl border border-slate-800 bg-slate-950/40",
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
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
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
