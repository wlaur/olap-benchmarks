import { Info } from "lucide-react"

import { cn } from "../../lib/cn"
import type { OperationSummary, RunMetadata } from "../../lib/types"
import { PanelCard, PanelHeader } from "../layout/Panel"
import { MetaLabel, SectionTitle } from "../Typography"
import { OverviewChartSkeleton } from "./ExplorerSkeletons"

interface RunMethodologyPanelProps {
  operationSummaries: OperationSummary[]
  includedDatabases: string[]
  databaseColors: Record<string, string>
  isLoading: boolean
}

const OPERATION_PREFERENCE = ["select", "concurrent", "mutate", "populate"] as const

export function RunMethodologyPanel({
  operationSummaries,
  includedDatabases,
  databaseColors,
  isLoading,
}: RunMethodologyPanelProps) {
  const rows = includedDatabases.map((database) => {
    const summaries = operationSummaries.filter((summary) => summary.db === database)
    const summary = OPERATION_PREFERENCE.map((operation) =>
      summaries.find((candidate) => candidate.operation === operation),
    ).find(Boolean)
    return {
      database,
      summary,
      metadata: normalizeMetadata(summary?.metadata),
    }
  })
  const missingCount = rows.filter((row) => !row.metadata).length

  return (
    <PanelCard className="p-3">
      <PanelHeader className="flex-col items-start gap-2 sm:flex-row">
        <div>
          <SectionTitle as="h3">Methodology metadata</SectionTitle>
          <MetaLabel className="mt-1 tracking-normal text-slate-400 normal-case">
            Latest available operation run for each selected database.
          </MetaLabel>
        </div>
        {!isLoading ? (
          <div
            className={cn(
              "inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs",
              missingCount > 0
                ? "border-amber-300/25 bg-amber-950/20 text-amber-200"
                : "border-emerald-300/20 bg-emerald-950/20 text-emerald-200",
            )}
          >
            <Info className="size-3.5" strokeWidth={1.8} />
            {missingCount > 0 ? `${missingCount} missing` : "Complete"}
          </div>
        ) : null}
      </PanelHeader>

      {isLoading ? (
        <div className="mt-3">
          <OverviewChartSkeleton />
        </div>
      ) : rows.length === 0 ? (
        <p className="mt-3 text-sm text-slate-500">No run metadata is available.</p>
      ) : (
        <div className="mt-3 overflow-x-auto">
          <table className="w-full min-w-[42rem] border-collapse text-left text-xs">
            <thead className="text-[0.65rem] tracking-wider text-slate-400 uppercase">
              <tr>
                <th className="border-b border-border-subtle px-3 py-2 font-medium">Database</th>
                <th className="border-b border-border-subtle px-3 py-2 font-medium">Execution</th>
                <th className="border-b border-border-subtle px-3 py-2 font-medium">Host</th>
                <th className="border-b border-border-subtle px-3 py-2 font-medium">Docker</th>
                <th className="border-b border-border-subtle px-3 py-2 font-medium">Image</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.database} className="border-b border-border-subtle/70 last:border-0">
                  <td className="px-3 py-2 align-top">
                    <div className="flex items-center gap-2">
                      <span
                        className="size-2 rounded-full"
                        style={{ backgroundColor: databaseColors[row.database] ?? "#94a3b8" }}
                      />
                      <span className="font-medium text-slate-100">{row.database}</span>
                    </div>
                  </td>
                  {row.metadata ? (
                    <>
                      <MetadataCell value={formatExecution(row.metadata)} />
                      <MetadataCell value={formatHost(row.metadata)} />
                      <MetadataCell value={formatDocker(row.metadata)} />
                      <MetadataCell
                        value={formatImage(row.metadata)}
                        title={formatImageTitle(row.metadata)}
                      />
                    </>
                  ) : (
                    <td colSpan={4} className="px-3 py-2 text-amber-200/90">
                      Missing metadata. This run likely predates methodology metadata recording.
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </PanelCard>
  )
}

function MetadataCell({ value, title }: { value: string; title?: string }) {
  return (
    <td className="max-w-[18rem] px-3 py-2 align-top text-slate-300" title={title ?? value}>
      <span className="line-clamp-2">{value}</span>
    </td>
  )
}

function normalizeMetadata(value: unknown): RunMetadata | null {
  if (!value) return null
  if (typeof value === "string") {
    try {
      return normalizeMetadata(JSON.parse(value))
    } catch {
      return null
    }
  }
  return typeof value === "object" ? (value as RunMetadata) : null
}

function formatExecution(metadata: RunMetadata): string {
  const mode = metadata.execution?.mode ?? "unknown"
  return mode === "in_process" ? "In process" : mode === "container" ? "Container" : String(mode)
}

function formatHost(metadata: RunMetadata): string {
  const host = metadata.host
  if (!host) return "Unknown"

  const memory = host.memory_total_mb ? `${Math.round(host.memory_total_mb / 1024)} GiB` : null
  return compactJoin([
    compactJoin([host.os, host.os_release], " "),
    host.machine,
    host.cpu_count_logical ? `${host.cpu_count_logical} CPU` : null,
    memory,
  ])
}

function formatDocker(metadata: RunMetadata): string {
  const docker = metadata.docker
  const execution = metadata.execution
  if (!docker && !execution?.container_platform) return "None"

  return compactJoin([
    execution?.container_platform ?? docker?.server_platform,
    docker?.version ? `Docker ${docker.version}` : null,
    docker?.context ? `context ${docker.context}` : null,
  ])
}

function formatImage(metadata: RunMetadata): string {
  const image = metadata.execution?.container_image
  if (!image) return metadata.execution?.mode === "in_process" ? "Python package" : "Unknown"

  const digest = metadata.execution?.container_image_digest
  if (!digest) return image

  const shortDigest = digest
    .split("@")
    .at(-1)
    ?.replace(/^sha256:/, "")
    .slice(0, 12)
  return shortDigest ? `${image} @ ${shortDigest}` : image
}

function formatImageTitle(metadata: RunMetadata): string {
  const image = metadata.execution?.container_image
  const digest = metadata.execution?.container_image_digest
  return compactJoin([image, digest], " @ ")
}

function compactJoin(values: Array<number | string | null | undefined>, separator = " · "): string {
  const parts = values.filter(
    (value): value is number | string => value !== null && value !== undefined && value !== "",
  )
  return parts.length > 0 ? parts.join(separator) : "Unknown"
}
