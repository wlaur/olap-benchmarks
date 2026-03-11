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
}

const columnHelper = createColumnHelper<QueryComparisonRow>()

export function QueryComparisonTable({
  rows,
  databases,
  databaseColors,
  selection,
  maxDuration,
}: QueryComparisonTableProps) {
  const columns = useMemo(
    () => [
      columnHelper.accessor("query_label", { header: "Query" }),
      columnHelper.accessor("category", { header: "Category" }),
      columnHelper.accessor("scale", { header: "Scale" }),
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
      ...databases.map((database) =>
        columnHelper.accessor(
          (row) => (Object.hasOwn(row.by_database, database) ? row.by_database[database] : null),
          {
            id: `${database}-median`,
            header: database,
            cell: (info) => {
              const value = info.getValue()
              return value === null || value === undefined ? "—" : formatDurationSeconds(value)
            },
          },
        ),
      ),
    ],
    [databases, databaseColors, maxDuration],
  )

  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <div className="overflow-x-auto rounded-2xl border border-slate-800">
      <table className="w-full text-left text-sm">
        <thead className="bg-slate-900/80 text-slate-400">
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
                  <td key={cell.id} className="px-4 py-2.5 whitespace-nowrap">
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
