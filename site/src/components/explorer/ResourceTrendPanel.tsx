import { useEffect, useMemo, useState } from "react"

import { getDatabaseColors } from "../../lib/databaseColors"
import { toTitleCase } from "../../lib/format"
import {
  buildTrendChartData,
  getAvailableStepWindows,
  getStepOptions,
  METRIC_CONFIGS,
  type StepTimeWindow,
} from "../../lib/resourceTrendTransforms"
import { METRIC_SAMPLE_RATE_S, type SuiteConfig } from "../../lib/suiteConfig"
import type { BenchmarkOperation, InsertStep, MetricSample, QueryStep } from "../../lib/types"
import { ControlChip, QuietButton, SegmentedButton } from "../controls/Control"
import { DatabaseLegend } from "../DatabaseLegend"
import { PanelCard } from "../layout/Panel"
import { MetricTrendChart } from "../MetricTrendChart"
import { MetaLabel, SectionTitle } from "../Typography"

interface ResourceTrendPanelProps {
  selectedOperation?: "select" | "mutate"
  suiteConfig: SuiteConfig
  metricSamples: MetricSample[]
  insertSteps: InsertStep[]
  querySteps: QueryStep[]
  mutateSteps: QueryStep[]
  databases: string[]
  isLoading?: boolean
}

const OPERATION_LABEL: Record<BenchmarkOperation, string> = {
  populate: "Populate",
  mutate: "Mutate",
  select: "Select",
}

const MAX_COLLAPSED_STEP_OPTIONS = 12
const MAX_COLLAPSED_UNAVAILABLE_STEP_OPTIONS = 8

export function ResourceTrendPanel({
  selectedOperation,
  suiteConfig,
  metricSamples,
  insertSteps,
  querySteps,
  mutateSteps,
  databases,
  isLoading = false,
}: ResourceTrendPanelProps) {
  const [selectedStep, setSelectedStep] = useState<string | null>(null)
  const [showAllSteps, setShowAllSteps] = useState(false)
  const [showAllUnavailableSteps, setShowAllUnavailableSteps] = useState(false)

  const availableOperations = suiteConfig.operations
  const parentOperation: BenchmarkOperation =
    selectedOperation === "mutate" && availableOperations.includes("mutate") ? "mutate" : "select"

  const [panelOperation, setPanelOperation] = useState<BenchmarkOperation>(parentOperation)
  // Track whether the user has manually picked an operation inside the panel.
  // Once they have, we stop syncing to the parent's selectedOperation so a
  // populate selection here isn't clobbered when the parent switches between
  // select/mutate.
  const [hasOverride, setHasOverride] = useState(false)

  useEffect(() => {
    if (!hasOverride) setPanelOperation(parentOperation)
  }, [parentOperation, hasOverride])

  const resolvedOperation: BenchmarkOperation = availableOperations.includes(panelOperation)
    ? panelOperation
    : (availableOperations[0] ?? "select")

  const databaseColors = getDatabaseColors(databases)

  useEffect(() => {
    setSelectedStep(null)
    setShowAllSteps(false)
    setShowAllUnavailableSteps(false)
  }, [resolvedOperation])

  const allStepOptions = useMemo(
    () =>
      getStepOptions(
        resolvedOperation,
        insertSteps,
        querySteps,
        mutateSteps,
        databases,
        suiteConfig.compareQueryNames,
      ),
    [resolvedOperation, insertSteps, querySteps, mutateSteps, databases, suiteConfig],
  )

  const availableStepWindows = useMemo(
    () =>
      getAvailableStepWindows(
        resolvedOperation,
        metricSamples,
        insertSteps,
        querySteps,
        mutateSteps,
        databases,
      ),
    [resolvedOperation, metricSamples, insertSteps, querySteps, mutateSteps, databases],
  )

  const stepOptions = useMemo(
    () => allStepOptions.filter((step) => availableStepWindows.has(step.value)),
    [allStepOptions, availableStepWindows],
  )

  const unavailableStepOptions = useMemo(
    () => allStepOptions.filter((step) => !availableStepWindows.has(step.value)),
    [allStepOptions, availableStepWindows],
  )

  const visibleStepOptions = showAllSteps
    ? stepOptions
    : stepOptions.slice(0, MAX_COLLAPSED_STEP_OPTIONS)
  const visibleUnavailableStepOptions = showAllUnavailableSteps
    ? unavailableStepOptions
    : unavailableStepOptions.slice(0, MAX_COLLAPSED_UNAVAILABLE_STEP_OPTIONS)

  const resolvedStep =
    selectedStep && stepOptions.some((s) => s.value === selectedStep)
      ? selectedStep
      : (stepOptions[0]?.value ?? null)

  const stepTimeWindows = useMemo(
    () =>
      resolvedStep
        ? (availableStepWindows.get(resolvedStep) ?? new Map<string, StepTimeWindow>())
        : new Map(),
    [resolvedStep, availableStepWindows],
  )

  const stepDurations = useMemo(() => {
    const durations = new Map<string, number>()
    for (const [db, window] of stepTimeWindows) {
      durations.set(db, window.duration_s)
    }
    return durations
  }, [stepTimeWindows])

  const maxDuration = Math.max(0, ...Array.from(stepDurations.values()))
  const isTooFast = maxDuration < METRIC_SAMPLE_RATE_S

  const chartData = useMemo(() => {
    if (isTooFast || !resolvedStep) return { cpu_percent: [], mem_mb: [], disk_mb: [] }
    return buildTrendChartData(metricSamples, resolvedOperation, stepTimeWindows)
  }, [metricSamples, resolvedOperation, stepTimeWindows, isTooFast, resolvedStep])

  const maxElapsed = useMemo(
    () =>
      Math.max(
        0,
        ...METRIC_CONFIGS.flatMap((metric) => chartData[metric.key].map((row) => row.elapsed_s)),
      ),
    [chartData],
  )

  const hasSufficientData =
    !isTooFast &&
    Math.max(chartData.cpu_percent.length, chartData.mem_mb.length, chartData.disk_mb.length) > 1

  const hasData = metricSamples.length > 0

  return (
    <PanelCard className="flex h-full min-h-0 min-w-0 flex-col p-3">
      <SectionTitle as="h3" className="shrink-0">
        Resource trends
      </SectionTitle>

      {isLoading ? (
        <div className="mt-2 rounded-lg border border-border-default bg-surface-inset px-4 py-6 text-xs text-slate-500">
          Loading resource metrics…
        </div>
      ) : !hasData ? (
        <div className="mt-2 rounded-lg border border-dashed border-border-default bg-surface-inset px-4 py-6 text-xs text-slate-500">
          No resource metrics were recorded for the selected databases.
        </div>
      ) : (
        <div className="mt-2 flex min-h-0 flex-1 flex-col rounded-lg border border-border-default bg-surface-inset p-3">
          <div className="flex shrink-0 flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <MetaLabel>{toTitleCase(resolvedOperation)} metrics</MetaLabel>
            </div>

            <div className="min-w-0">
              <DatabaseLegend databases={databases} databaseColors={databaseColors} />
            </div>
          </div>

          {availableOperations.length > 1 ? (
            <div className="mt-3 shrink-0 space-y-2">
              <MetaLabel>Operation</MetaLabel>
              <div className="flex flex-wrap gap-1.5">
                {availableOperations.map((op) => (
                  <SegmentedButton
                    key={op}
                    selected={resolvedOperation === op}
                    onClick={() => {
                      setPanelOperation(op)
                      setHasOverride(true)
                    }}
                  >
                    {OPERATION_LABEL[op]}
                  </SegmentedButton>
                ))}
              </div>
            </div>
          ) : null}

          {stepOptions.length > 0 ? (
            <div className="mt-3 shrink-0">
              <div className="space-y-2">
                <MetaLabel>Tracked Step</MetaLabel>
                <div
                  className={`panel-scrollbar overflow-y-auto pr-1 ${
                    showAllSteps ? "max-h-56" : "max-h-28"
                  }`}
                >
                  <div className="flex flex-wrap gap-1.5">
                    {visibleStepOptions.map((step) => (
                      <ControlChip
                        key={step.value}
                        onClick={() => setSelectedStep(step.value)}
                        selected={resolvedStep === step.value}
                      >
                        {step.label}
                      </ControlChip>
                    ))}
                  </div>
                </div>
                {stepOptions.length > MAX_COLLAPSED_STEP_OPTIONS ? (
                  <QuietButton onClick={() => setShowAllSteps((current) => !current)}>
                    {showAllSteps
                      ? "Show fewer steps"
                      : `Show ${stepOptions.length - visibleStepOptions.length} more steps`}
                  </QuietButton>
                ) : null}
              </div>
            </div>
          ) : (
            <p className="mt-3 text-xs text-slate-500">No steps found for this operation.</p>
          )}

          {unavailableStepOptions.length > 0 ? (
            <div className="mt-3 shrink-0">
              <div className="space-y-2">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <MetaLabel>No Metrics</MetaLabel>
                  {unavailableStepOptions.length > MAX_COLLAPSED_UNAVAILABLE_STEP_OPTIONS ? (
                    <QuietButton onClick={() => setShowAllUnavailableSteps((current) => !current)}>
                      {showAllUnavailableSteps
                        ? "Show fewer unavailable"
                        : `Show ${unavailableStepOptions.length - visibleUnavailableStepOptions.length} more unavailable`}
                    </QuietButton>
                  ) : null}
                </div>
                <div
                  className={`panel-scrollbar overflow-y-auto pr-1 ${
                    showAllUnavailableSteps ? "max-h-52" : "max-h-28"
                  }`}
                >
                  <div className="flex flex-wrap gap-1.5">
                    {visibleUnavailableStepOptions.map((step) => (
                      <span
                        key={step.value}
                        className="rounded-full border border-dashed border-border-default bg-surface-primary/45 px-2.5 py-1 text-xs font-medium text-slate-500"
                        aria-disabled="true"
                      >
                        {step.label}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          ) : null}

          {resolvedStep === null ? (
            <div className="mt-3 rounded-lg border border-dashed border-border-default bg-surface-inset px-4 py-5 text-xs text-slate-500">
              No steps available for the selected operation.
            </div>
          ) : isTooFast || !hasSufficientData ? (
            <div className="mt-3 rounded-lg border border-dashed border-border-default bg-surface-inset px-4 py-5 text-xs text-slate-500">
              Step too fast for resource sampling (under {METRIC_SAMPLE_RATE_S}s).
            </div>
          ) : (
            <div className="panel-scrollbar mt-3 min-h-0 flex-1 overflow-auto pr-1">
              <div className="grid gap-2">
                {METRIC_CONFIGS.map((metric) => (
                  <div
                    key={metric.key}
                    className="rounded-lg border border-border-default bg-surface-primary/60 p-2.5"
                  >
                    <MetricTrendChart
                      label={metric.label}
                      data={chartData[metric.key]}
                      databases={databases}
                      databaseColors={databaseColors}
                      maxElapsed={maxElapsed}
                      formatter={metric.formatter}
                      scaleBuilder={metric.scaleBuilder}
                      syncId="resource-trends"
                      size="md"
                    />
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </PanelCard>
  )
}
