import {
  ArrowDown,
  ArrowUp,
  ChevronDown,
  ChevronsUpDown,
  Database,
  FlaskConical,
  GitBranch,
  Scale,
  Search,
  Server,
  type LucideIcon,
} from "lucide-react"
import { useEffect, useMemo, useState, type ReactNode } from "react"
import { useNavigate, useSearchParams } from "react-router-dom"

import { ControlChip, SegmentedButton } from "../components/controls/Control"
import { ControlSelect } from "../components/controls/ControlSelect"
import { VirtualizedSelect } from "../components/controls/VirtualizedSelect"
import { DetailsDialog } from "../components/layout/DetailsDialog"
import { Disclosure } from "../components/layout/Disclosure"
import { ChartFrame, PanelCard, PanelDisclosure, PanelHeader } from "../components/layout/Panel"
import {
  CampaignNotice,
  ProfileScopeNotice,
  SnapshotSpanWarning,
} from "../components/methodology/ComparisonScopeNotices"
import { EngineComparison } from "../components/methodology/EngineComparison"
import { QueryEvidence, RunContext } from "../components/methodology/ObservationContext"
import { Skeleton } from "../components/Skeleton"
import { SqlCodeView } from "../components/SqlCodeView"
import { BodyText, MetaLabel, SectionTitle } from "../components/Typography"
import { useExplorerData } from "../hooks/useExplorerData"
import type { BenchmarkDefinition, BenchmarkSuiteId } from "../lib/benchmarks"
import { cn } from "../lib/cn"
import type { ComparisonFamily, ComparisonManifest } from "../lib/comparison"
import { getDatabaseColors } from "../lib/databaseColors"
import { formatDatabaseName } from "../lib/databaseNames"
import { formatLatencyMs } from "../lib/format"
import {
  compareFamily,
  observeQuery,
  runPerformanceProblems,
  matchingAnnotations,
  type RunObservation,
} from "../lib/observations"
import { describeQueryOutcome, queryOutcomeSummary, type QueryOutcome } from "../lib/queryOutcomes"
import { suiteDescriptions } from "../lib/suiteDescriptions"
import type { ExplorerQueryMetric, QuerySqlEntry } from "../lib/types"

type ComparisonMode = "database" | "scale" | "version" | "system"
type SingleDatabaseMode = Exclude<ComparisonMode, "database">

const COMPARISON_MODES: {
  id: ComparisonMode
  label: string
  icon: LucideIcon
}[] = [
  { id: "database", label: "Databases", icon: Database },
  { id: "scale", label: "Scale factors", icon: Scale },
  { id: "version", label: "Versions", icon: GitBranch },
  { id: "system", label: "Systems", icon: Server },
]

const SINGLE_DATABASE_MODES = COMPARISON_MODES.filter(
  (
    comparisonMode,
  ): comparisonMode is (typeof COMPARISON_MODES)[number] & {
    id: SingleDatabaseMode
  } => comparisonMode.id !== "database",
)

const SERIES_COLORS = ["#5eead4", "#93c5fd", "#fbbf24", "#fb7185", "#c4b5fd"] as const

interface ExplorerPageProps {
  suiteId: BenchmarkSuiteId
  suiteDefinition: BenchmarkDefinition
  benchmarkDefinitions: readonly BenchmarkDefinition[]
  preferredSystem: string | null
}

interface RawSelection {
  system: string
  systems: string[]
  database: string
  databases: string[]
  scale: number | null
  scales: number[]
  version: string
  versions: string[]
}

interface ResolvedSelection {
  system: string
  systems: string[]
  systemOptions: string[]
  database: string
  databases: string[]
  databaseOptions: string[]
  scale: number
  scales: number[]
  scaleOptions: number[]
  version: string
  versions: string[]
  versionOptions: string[]
  latestVersions: Map<string, string>
  ready: boolean
}

interface RunVariant {
  runId: number
  system: string
  scale: number
  database: string
  version: string
  systemSnapshotId: number | null
  finishedAt: string
}

interface ComparisonRow {
  id: string
  label: string
  detail: string
  color: string
  database: string
  systemSnapshotId: number | null
  queryValues: Map<string, number>
  queryOutcomes: Map<string, QueryOutcome>
}

interface QueryRow {
  query: string
  values: (number | null)[]
  outcomes: QueryOutcome[]
}

export function ExplorerPage({
  suiteId,
  suiteDefinition,
  benchmarkDefinitions,
  preferredSystem,
}: ExplorerPageProps) {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [inspectorOpen, setInspectorOpen] = useState(false)
  const [lastAnalyzedDatabase, setLastAnalyzedDatabase] = useState<string | null>(() =>
    searchParams.get("mode") !== "database" ? searchParams.get("database") : null,
  )
  const mode = parseComparisonMode(searchParams.get("mode"))
  const [singleDatabaseMode, setSingleDatabaseMode] = useState<SingleDatabaseMode>(() =>
    mode === "database" ? "scale" : mode,
  )
  const rawSelection = parseUrlState(searchParams, preferredSystem)
  const requestedQuery = searchParams.get("query") ?? ""
  const requestedSqlDatabase = searchParams.get("sql_database") ?? ""
  const { metrics, runs, comparison, queriesManifest, loading, error } = useExplorerData(suiteId)
  const requestedFamily = searchParams.get("family") ?? ""
  const families = comparison?.families.filter((family) => family.suite === suiteId) ?? []
  const family =
    families.find((entry) => entry.id === requestedFamily) ??
    families.find((entry) => entry.track === "raw") ??
    families[0]
  const variants = makeRunVariants(runs)
  const selection = useMemo(
    () =>
      resolveSelection({
        mode,
        variants,
        raw: rawSelection,
        preferredSystem,
        defaultScale: suiteDefinition.defaultScaleFactor,
      }),
    [mode, preferredSystem, rawSelection, suiteDefinition.defaultScaleFactor, variants],
  )
  const singleDatabaseSelections = useMemo(
    () =>
      new Map(
        SINGLE_DATABASE_MODES.map((comparisonMode) => [
          comparisonMode.id,
          resolveSelection({
            mode: comparisonMode.id,
            variants,
            raw: rawSelection,
            preferredSystem,
            defaultScale: suiteDefinition.defaultScaleFactor,
          }),
        ]),
      ),
    [preferredSystem, rawSelection, suiteDefinition.defaultScaleFactor, variants],
  )
  const databaseColors = useMemo(
    () => getDatabaseColors(unique(variants.map((variant) => variant.database))),
    [variants],
  )
  const comparisonRows = buildComparisonRows({
    mode,
    metrics,
    variants,
    selection,
    databaseColors,
    runs,
    comparison,
    family,
  })
  const manifestEntries = Object.fromEntries(
    Object.entries(queriesManifest[suiteDefinition.queriesKey] ?? {}).filter(
      ([name]) => !family || family.coverage_query_names.includes(name),
    ),
  )
  const publicationRows =
    family && comparison
      ? compareFamily(family, selection.scale, selection.system, runs, comparison)
      : []
  const displayRows = comparisonRows
  const queryNames = family?.coverage_query_names ?? Object.keys(manifestEntries)
  const queryOptions = queryNames.map((query) => ({ value: query, label: query }))
  const selectedQuery = queryNames.includes(requestedQuery) ? requestedQuery : (queryNames[0] ?? "")
  const queryRows = makeQueryRows(queryNames, displayRows)
  const sqlDatabaseOptions = unique(
    mode === "database"
      ? displayRows.map((row) => row.database)
      : selection.database
        ? [selection.database]
        : [],
  )
  const sqlDatabase = sqlDatabaseOptions.includes(requestedSqlDatabase)
    ? requestedSqlDatabase
    : (sqlDatabaseOptions[0] ?? "")
  const selectedSqlEntry = manifestEntries[selectedQuery]
  const selectedSql = resolveSql(selectedSqlEntry, sqlDatabase)
  const sqlVariants = new Map(
    sqlDatabaseOptions.map((database) => [
      database,
      sqlDiffersFromDefault(selectedSqlEntry, database),
    ]),
  )
  const hasDifferentSql = [...sqlVariants.values()].some(Boolean)
  const selectedQueryRows = displayRows.map((row) => ({
    ...row,
    valueMs: row.queryValues.get(selectedQuery) ?? null,
    outcome:
      row.queryOutcomes.get(selectedQuery) ?? describeQueryOutcome(undefined, undefined, false, []),
  }))
  if (mode === "database")
    selectedQueryRows.sort((a, b) => (a.valueMs ?? Infinity) - (b.valueMs ?? Infinity))
  const queryFinding = queryOutcomeSummary(selectedQueryRows)
  const queryDetailHeight = Math.min(380, Math.max(280, 80 + selectedQueryRows.length * 80))
  const nextSearchParams = (() => {
    if (!selection.ready || !selectedQuery) return ""
    const params = new URLSearchParams()
    params.set("mode", mode)
    if (family) params.set("family", family.id)
    params.set("system", selection.system)
    params.set("systems", selection.systems.join(","))
    params.set("database", selection.database)
    params.set(
      "databases",
      (mode === "database"
        ? selection.databases
        : rawSelection.databases.length
          ? rawSelection.databases
          : selection.databaseOptions
      ).join(","),
    )
    params.set("scale", String(selection.scale))
    params.set("scales", selection.scales.join(","))
    params.set("version", selection.version)
    params.set("versions", selection.versions.join(","))
    params.set("query", selectedQuery)
    if (sqlDatabase) params.set("sql_database", sqlDatabase)
    return params.toString()
  })()

  useEffect(() => {
    if (nextSearchParams && searchParams.toString() !== nextSearchParams)
      setSearchParams(nextSearchParams, { replace: true })
  }, [nextSearchParams, searchParams, setSearchParams])

  function updateUrl(changes: Record<string, string | null>) {
    const params = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(changes)) {
      if (value === null) params.delete(key)
      else params.set(key, value)
    }
    setSearchParams(params, { replace: true })
  }
  function setRequestedQuery(query: string) {
    updateUrl({ query })
  }
  function setRequestedSqlDatabase(database: string) {
    updateUrl({ sql_database: database })
  }
  function setRequestedFamily(familyId: string) {
    updateUrl({ family: familyId })
  }
  function updateRawSelection(update: Partial<RawSelection>, nextMode: ComparisonMode = mode) {
    const changes: Record<string, string | null> = { mode: nextMode }
    if (nextMode !== "database" && update.database) setLastAnalyzedDatabase(update.database)
    for (const [key, value] of Object.entries(update))
      changes[key] = value === null ? null : Array.isArray(value) ? value.join(",") : String(value)
    updateUrl(changes)
  }

  function handleSuiteChange(nextSuiteId: string) {
    navigate(`/explorer/${nextSuiteId}?${nextSearchParams || `mode=${mode}`}`)
  }

  function handleSingleDatabaseModeChange(nextMode: SingleDatabaseMode) {
    const target = singleDatabaseSelections.get(nextMode)
    updateRawSelection(
      {
        databases: rawSelection.databases.length ? rawSelection.databases : selection.databases,
        scales:
          mode === "database" && nextMode === "scale"
            ? (target?.scaleOptions ?? [])
            : rawSelection.scales,
        versions:
          mode === "database" && nextMode === "version"
            ? (target?.versionOptions ?? [])
            : rawSelection.versions,
        systems:
          mode === "database" && nextMode === "system"
            ? (target?.systemOptions ?? [])
            : rawSelection.systems,
      },
      nextMode,
    )
    setSingleDatabaseMode(nextMode)
  }
  function handleCompareDatabases() {
    updateRawSelection(
      { databases: rawSelection.databases.length ? rawSelection.databases : selection.databases },
      "database",
    )
  }
  function handleAnalyzeOneDatabase() {
    const database = selection.databaseOptions.includes(lastAnalyzedDatabase ?? "")
      ? lastAnalyzedDatabase!
      : (publicationRows
          .filter(
            (row) => row.eligible && row.score !== null && selection.databases.includes(row.db),
          )
          .sort((a, b) => a.score! - b.score!)[0]?.db ?? selection.database)
    const targets = SINGLE_DATABASE_MODES.map(({ id }) => ({
      mode: id,
      selection: resolveSelection({
        mode: id,
        variants,
        raw: { ...rawSelection, database },
        preferredSystem,
        defaultScale: suiteDefinition.defaultScaleFactor,
      }),
    }))
    const target =
      targets.find(
        (candidate) =>
          candidate.mode === singleDatabaseMode &&
          getVaryingOptionCount(candidate.mode, candidate.selection) > 1,
      ) ??
      targets.find((candidate) => getVaryingOptionCount(candidate.mode, candidate.selection) > 1) ??
      targets[0]!
    setSingleDatabaseMode(target.mode)
    updateRawSelection(
      {
        database,
        databases: rawSelection.databases.length ? rawSelection.databases : selection.databases,
        scales: target.mode === "scale" ? target.selection.scaleOptions : rawSelection.scales,
        versions:
          target.mode === "version" ? target.selection.versionOptions : rawSelection.versions,
        systems: target.mode === "system" ? target.selection.systemOptions : rawSelection.systems,
      },
      target.mode,
    )
  }

  const showLoading = loading
  const analysisSummary = selection.ready
    ? mode === "database"
      ? `${selection.databases.length} ${selection.databases.length === 1 ? "database" : "databases"} · ${selection.system} · SF ${selection.scale}`
      : mode === "scale"
        ? `${formatDatabaseName(selection.database)} ${selection.version} · ${selection.system} · ${selection.scales.map((scale) => `SF ${scale}`).join(", ")}`
        : mode === "version"
          ? `${formatDatabaseName(selection.database)} · ${selection.system} · SF ${selection.scale} · ${selection.versions.join(", ")}`
          : `${formatDatabaseName(selection.database)} ${selection.version} · SF ${selection.scale} · ${selection.systems.join(", ")}`
    : "Configure the comparison"

  const queryInspector =
    !showLoading && !error && displayRows.length > 0 ? (
      <DetailsDialog
        title="Query performance and SQL"
        trigger="Inspect a query"
        open={inspectorOpen}
        onOpenChange={setInspectorOpen}
      >
        <VirtualizedSelect
          ariaLabel="Query to inspect"
          label="Query"
          value={selectedQuery}
          options={queryOptions}
          onChange={setRequestedQuery}
          icon={<Search className="size-4" />}
          filterPlaceholder="Filter queries"
          className="w-full"
        />
        <div className="grid min-w-0 gap-4 lg:grid-cols-2">
          <ChartFrame
            className="flex flex-col overflow-hidden p-4"
            style={{ height: queryDetailHeight }}
          >
            <SectionTitle as="h3" className="mb-1 text-lg">
              Recorded latency
            </SectionTitle>
            <BodyText className="mb-4 text-sm">
              Median fetch time, including transfer and conversion. Shorter bars are faster.
            </BodyText>
            <div
              data-query-runtime-scroll
              className="panel-scrollbar min-h-0 flex-1 overflow-y-auto pr-1"
            >
              <RuntimeRankingBars rows={selectedQueryRows} />
            </div>
          </ChartFrame>
          <ChartFrame
            className="flex flex-col overflow-hidden p-4"
            style={{ height: queryDetailHeight }}
          >
            <SectionTitle as="h3" className="mb-3 text-lg">
              SQL
            </SectionTitle>
            {sqlDatabaseOptions.length > 0 ? (
              <div className="panel-scrollbar mb-3 flex gap-1 overflow-x-auto pb-1">
                {sqlDatabaseOptions.map((database) => {
                  const differsFromDefault = sqlVariants.get(database) ?? false
                  const databaseName = formatDatabaseName(database)
                  return (
                    <SegmentedButton
                      key={database}
                      selected={sqlDatabase === database}
                      aria-pressed={sqlDatabase === database}
                      aria-label={`${databaseName} SQL`}
                      onClick={() => setRequestedSqlDatabase(database)}
                      className="shrink-0 gap-1.5 rounded-md"
                      data-sql-variant={differsFromDefault ? "different" : "default"}
                    >
                      {databaseName}
                      {differsFromDefault ? (
                        <span className="sr-only"> · differs from default SQL</span>
                      ) : null}
                    </SegmentedButton>
                  )
                })}
              </div>
            ) : null}
            {hasDifferentSql ? (
              <BodyText className="mb-2 text-sm">
                SQL varies by database; select a dialect above.
              </BodyText>
            ) : null}
            <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border-subtle bg-surface-primary/40">
              <SqlCodeView code={selectedSql} wrapLines className="text-sm leading-relaxed" />
            </div>
          </ChartFrame>
        </div>
        {family && comparison ? (
          <Disclosure title="Execution and validation" lazy>
            <BodyText className="text-sm">
              Successful iterations may be available for incomplete or blocked runs. These medians
              are descriptive. Hash agreement is not independent correctness validation.
            </BodyText>
            {displayRows.map((row) => {
              const run = runs.find((entry) => String(entry.runId) === row.id)
              if (!run) return null
              const outcome = row.queryOutcomes.get(selectedQuery)
              return (
                <Disclosure
                  key={row.id}
                  title={`${row.label} · ${run.version} · run ${run.runId} · ${outcome?.label ?? "Not run"}`}
                  lazy
                >
                  <QueryEvidence
                    query={observeQuery(run, selectedQuery, runs, comparison)}
                    comparable={family.query_names.includes(selectedQuery)}
                  />
                  <RunContext
                    run={run}
                    annotations={matchingAnnotations(run, run.db, run.suite, run.scale, comparison)}
                  />
                </Disclosure>
              )
            })}
          </Disclosure>
        ) : null}
      </DetailsDialog>
    ) : null

  return (
    <div className="flex min-h-full w-full max-w-full min-w-0 shrink-0 flex-col gap-4 overflow-x-clip pb-8">
      <header>
        <div className="min-w-0">
          <MetaLabel>Explorer</MetaLabel>
          <h1 className="mt-1 text-2xl font-semibold text-slate-50 sm:text-3xl">
            {suiteDefinition.title}
          </h1>
          <p className="mt-2 max-w-3xl font-sans text-sm leading-6 text-slate-400">
            {analysisSummary}
          </p>
        </div>
      </header>

      <div className="flex min-w-0 flex-wrap gap-2">
        <SegmentedButton
          selected={mode === "database"}
          aria-pressed={mode === "database"}
          onClick={handleCompareDatabases}
          size="sm"
          className="min-h-9 min-w-0 gap-2"
        >
          <Database className="h-3.5 w-3.5 shrink-0" strokeWidth={1.8} />
          Compare databases
        </SegmentedButton>
        <SegmentedButton
          selected={mode !== "database"}
          aria-pressed={mode !== "database"}
          aria-label="Analyze one database"
          onClick={handleAnalyzeOneDatabase}
          size="sm"
          className="min-h-9 min-w-0 gap-2"
        >
          <Search className="h-3.5 w-3.5 shrink-0" strokeWidth={1.8} />
          One database
        </SegmentedButton>
      </div>

      <details className="group overflow-hidden rounded-lg border border-border-default bg-surface-inset/60">
        <summary className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-3 px-4 py-2 text-sm font-medium text-slate-200 outline-none focus-visible:ring-2 focus-visible:ring-accent-300 [&::-webkit-details-marker]:hidden">
          <span>Change comparison</span>
          <ChevronDown aria-hidden="true" className="size-4 group-open:rotate-180" />
        </summary>

        <div className="border-t border-border-subtle px-3 py-3 sm:px-4 sm:py-4">
          <div className="min-w-0">
            <MetaLabel className="mb-2 block">Benchmark suite</MetaLabel>
            <ControlSelect
              ariaLabel="Benchmark suite"
              label="Suite"
              value={suiteId}
              onChange={handleSuiteChange}
              options={benchmarkDefinitions.map((definition) => ({
                value: definition.id,
                label: definition.title,
                description: suiteDescriptions[definition.id]?.purpose,
              }))}
              icon={<FlaskConical className="h-3 w-3" strokeWidth={1.8} />}
              labelMode="hidden"
              className="min-h-10 w-full"
              menuClassName="min-w-56"
            />
          </div>
          <div className="mt-3 border-t border-border-subtle pt-3">
            {showLoading ? (
              <SetupSkeleton />
            ) : error ? (
              <div className="border-y border-red-500/30 bg-red-950/20 px-3 py-4 text-sm text-red-300">
                Failed to load explorer data: {error}
              </div>
            ) : !selection.ready ? (
              <div className="border-y border-border-subtle px-3 py-4 text-sm text-slate-500">
                No recorded SELECT runs are available for this suite.
              </div>
            ) : mode === "database" ? (
              <div className="grid min-w-0 gap-3 xl:grid-cols-[minmax(0,1.3fr)_minmax(24rem,0.7fr)]">
                <AnalysisField
                  icon={Database}
                  label="Databases to compare"
                  helper="Compare databases on the same system and scale."
                >
                  <ChoiceChips
                    options={selection.databaseOptions.map((database) => ({
                      id: database,
                      label: formatDatabaseName(database),
                      color: databaseColors[database] ?? getSeriesColor(0),
                    }))}
                    selectedValues={selection.databases}
                    onToggle={(database) =>
                      updateRawSelection({
                        databases: toggleSelection(selection.databases, database),
                      })
                    }
                  />
                </AnalysisField>
                <AnalysisField
                  icon={Server}
                  label="Fixed environment"
                  helper="Every database runs on the same system and scale; latest completed versions are used."
                  className="xl:border-l xl:border-border-subtle xl:pl-4"
                >
                  <div className="flex min-w-0 flex-wrap gap-2">
                    <DimensionSelect
                      ariaLabel="System"
                      label="System"
                      icon={Server}
                      value={selection.system}
                      options={selection.systemOptions.map((system) => ({
                        id: system,
                        label: system,
                      }))}
                      onChange={(system) => updateRawSelection({ system })}
                      className="sm:w-72"
                    />
                    <DimensionSelect
                      ariaLabel="Scale factor"
                      label="Scale"
                      icon={Scale}
                      value={String(selection.scale)}
                      options={selection.scaleOptions.map((scale) => ({
                        id: String(scale),
                        label: `SF ${scale}`,
                      }))}
                      onChange={(scale) => updateRawSelection({ scale: Number(scale) })}
                      className="sm:w-40"
                    />
                  </div>
                </AnalysisField>
              </div>
            ) : (
              <div className="space-y-3">
                <div className="grid min-w-0 gap-3 lg:grid-cols-[minmax(15rem,0.6fr)_minmax(0,1.4fr)]">
                  <AnalysisField
                    icon={Database}
                    label="Database to analyze"
                    helper="Keep one engine in focus while another dimension changes."
                  >
                    <DimensionSelect
                      ariaLabel="Database"
                      label="Database"
                      icon={Database}
                      value={selection.database}
                      options={selection.databaseOptions.map((database) => ({
                        id: database,
                        label: formatDatabaseName(database),
                      }))}
                      onChange={(database) => updateRawSelection({ database })}
                    />
                  </AnalysisField>
                  <AnalysisField
                    icon={Scale}
                    label="Compare it across"
                    helper="Choose the dimension you want to vary."
                  >
                    <div className="grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-3">
                      {SINGLE_DATABASE_MODES.map((comparisonMode) => {
                        const Icon = comparisonMode.icon
                        const optionCount = getVaryingOptionCount(
                          comparisonMode.id,
                          singleDatabaseSelections.get(comparisonMode.id),
                        )
                        return (
                          <SegmentedButton
                            key={comparisonMode.id}
                            disabled={optionCount < 2}
                            title={
                              optionCount < 2
                                ? "Only one recorded value is available for this dimension."
                                : undefined
                            }
                            selected={mode === comparisonMode.id}
                            aria-pressed={mode === comparisonMode.id}
                            onClick={() => handleSingleDatabaseModeChange(comparisonMode.id)}
                            size="sm"
                            className="min-h-9 min-w-0 gap-1.5 px-2"
                          >
                            <Icon className="h-3.5 w-3.5 shrink-0" strokeWidth={1.8} />
                            <span className="truncate">{comparisonMode.label}</span>
                            <span className="ml-auto shrink-0 text-sm text-slate-400">
                              {optionCount}
                            </span>
                          </SegmentedButton>
                        )
                      })}
                    </div>
                  </AnalysisField>
                </div>

                <div className="grid min-w-0 gap-3 xl:grid-cols-[minmax(0,1fr)_auto]">
                  <AnalysisField
                    icon={getModeIcon(mode)}
                    label={`${getModePluralLabel(mode)} to compare`}
                    helper={
                      getVaryingOptionCount(mode, selection) < 2
                        ? `Only one recorded ${getModePluralLabel(mode).toLowerCase().replace(/s$/, "")} is available. Try another dimension or database for a meaningful comparison.`
                        : "Select the values that should appear together in the comparison and query views."
                    }
                  >
                    {mode === "scale" ? (
                      <ChoiceChips
                        options={selection.scaleOptions.map((scale, index) => ({
                          id: scale,
                          label: `SF ${scale}`,
                          color: getSeriesColor(index),
                        }))}
                        selectedValues={selection.scales}
                        onToggle={(scale) =>
                          updateRawSelection({ scales: toggleSelection(selection.scales, scale) })
                        }
                      />
                    ) : mode === "version" ? (
                      <ChoiceChips
                        options={selection.versionOptions.map((version, index) => ({
                          id: version,
                          label: version,
                          color: getSeriesColor(index),
                        }))}
                        selectedValues={selection.versions}
                        onToggle={(version) =>
                          updateRawSelection({
                            versions: toggleSelection(selection.versions, version),
                          })
                        }
                      />
                    ) : (
                      <ChoiceChips
                        options={selection.systemOptions.map((system, index) => ({
                          id: system,
                          label: system,
                          color: getSeriesColor(index),
                        }))}
                        selectedValues={selection.systems}
                        onToggle={(system) =>
                          updateRawSelection({
                            systems: toggleSelection(selection.systems, system),
                          })
                        }
                      />
                    )}
                  </AnalysisField>

                  <AnalysisField
                    icon={Server}
                    label="Fixed context"
                    helper="These values stay constant while the selected dimension changes."
                    className="w-fit max-w-full"
                  >
                    <div className="flex min-w-0 flex-wrap gap-2">
                      {mode !== "system" ? (
                        <DimensionSelect
                          ariaLabel="System"
                          label="System"
                          icon={Server}
                          value={selection.system}
                          options={selection.systemOptions.map((system) => ({
                            id: system,
                            label: system,
                          }))}
                          onChange={(system) => updateRawSelection({ system })}
                          className="sm:w-72"
                        />
                      ) : null}
                      {mode !== "scale" ? (
                        <DimensionSelect
                          ariaLabel="Scale factor"
                          label="Scale"
                          icon={Scale}
                          value={String(selection.scale)}
                          options={selection.scaleOptions.map((scale) => ({
                            id: String(scale),
                            label: `SF ${scale}`,
                          }))}
                          onChange={(scale) => updateRawSelection({ scale: Number(scale) })}
                          className="sm:w-40"
                        />
                      ) : null}
                      {mode !== "version" ? (
                        <DimensionSelect
                          ariaLabel="Database version"
                          label="Version"
                          icon={GitBranch}
                          value={selection.version}
                          options={selection.versionOptions.map((version) => ({
                            id: version,
                            label: version,
                          }))}
                          onChange={(version) => updateRawSelection({ version })}
                          className="sm:w-64"
                        />
                      ) : null}
                    </div>
                  </AnalysisField>
                </div>
              </div>
            )}
          </div>
        </div>
      </details>

      <PanelCard className="min-w-0 space-y-4 border-border-strong p-4 sm:p-6">
        <PanelHeader className="flex-wrap items-center">
          <SectionTitle as="h2">{getModeLabel(mode, selection)}</SectionTitle>
          <div className="flex flex-wrap gap-2">
            {queryInspector}
            <DetailsDialog title="Measurement context" trigger="Measurement context">
              <CampaignNotice />
              <ProfileScopeNotice />
              {mode !== "system" ? (
                <SnapshotSpanWarning
                  snapshotCount={new Set(displayRows.map((row) => row.systemSnapshotId)).size}
                />
              ) : null}
              <BodyText className="max-w-prose">
                Geometric mean query-latency ratios compare complete, eligible runs against the
                fixed publication cohort. Lower is better. Visibility never changes these ratios.
                Query medians describe recorded iterations and are not an aggregate ranking across
                scales, versions or systems.
              </BodyText>
            </DetailsDialog>
          </div>
        </PanelHeader>

        {families.length > 1 ? (
          <div role="group" aria-label="Workload track" className="flex flex-wrap gap-2">
            {families.map((entry) => (
              <SegmentedButton
                key={entry.id}
                selected={entry.id === family?.id}
                aria-pressed={entry.id === family?.id}
                onClick={() => setRequestedFamily(entry.id)}
              >
                {entry.title} · {entry.track}
              </SegmentedButton>
            ))}
          </div>
        ) : family ? (
          <BodyText>
            {family.title} · {family.track} track
          </BodyText>
        ) : null}
        {showLoading ? (
          <ResultsSkeleton />
        ) : error ? (
          <p role="alert" className="text-sm text-red-300">
            Failed to load explorer data: {error}
          </p>
        ) : !selection.ready ? (
          <p role="status" className="text-base text-slate-300">
            No recorded SELECT runs are available for this suite.
          </p>
        ) : family && mode === "database" ? (
          <>
            <EngineComparison
              rows={publicationRows.filter((row) => selection.databases.includes(row.db))}
              family={family}
              eligibleCount={publicationRows.filter((row) => row.eligible).length}
              cohortRows={publicationRows}
            />
            {publicationRows.some((row) => !row.run) ? (
              <Disclosure
                title={`${publicationRows.filter((row) => !row.run).length} without recorded results · ${publicationRows
                  .filter((row) => !row.run)
                  .map((row) => formatDatabaseName(row.db))
                  .join(", ")}`}
                lazy
              >
                {publicationRows
                  .filter((row) => !row.run)
                  .map((row) => (
                    <BodyText key={row.key} className="text-sm">
                      {formatDatabaseName(row.db)}:{" "}
                      {row.registration === "unimplemented"
                        ? "adapter not implemented"
                        : "no recorded run; execution unknown"}
                      .
                    </BodyText>
                  ))}
              </Disclosure>
            ) : null}
          </>
        ) : (
          <div className="space-y-4">
            <VirtualizedSelect
              ariaLabel="Query"
              label="Query to compare"
              value={selectedQuery}
              options={queryOptions}
              onChange={setRequestedQuery}
              icon={<Search className="size-4" />}
              filterPlaceholder="Filter queries"
              className="w-full sm:max-w-xl"
            />
            <BodyText className="max-w-prose text-xl font-medium text-slate-100">
              {queryFinding}
            </BodyText>
            <RuntimeRankingBars rows={selectedQueryRows} />
          </div>
        )}
      </PanelCard>

      <PanelDisclosure title="All query results">
        {showLoading ? (
          <ResultsSkeleton />
        ) : error ? (
          <BodyText>Query results unavailable: {error}</BodyText>
        ) : displayRows.length > 0 ? (
          <>
            <BodyText>
              Recorded median latencies. Lighter blue means shorter latency on a fixed 1 ms–100 s
              scale; status labels distinguish failures, exclusions and absent measurements. Select
              a query to inspect it.
            </BodyText>
            <QueryBreakdown
              rows={displayRows}
              queryRows={queryRows}
              selectedQuery={selectedQuery}
              onSelectQuery={(query) => {
                setRequestedQuery(query)
                setInspectorOpen(true)
              }}
            />
          </>
        ) : (
          <BodyText>No selected query results are available.</BodyText>
        )}
      </PanelDisclosure>
    </div>
  )
}

function AnalysisField({
  icon: Icon,
  label,
  helper,
  children,
  className,
}: {
  icon: LucideIcon
  label: string
  helper: string
  children: ReactNode
  className?: string
}) {
  return (
    <section className={cn("min-w-0 py-1", className)}>
      <div className="flex min-w-0 items-start gap-2.5">
        <Icon className="mt-0.5 h-4 w-4 shrink-0 text-slate-500" strokeWidth={1.8} />
        <div className="min-w-0 flex-1">
          <p className="text-sm font-semibold text-slate-100">{label}</p>
          <p className="mt-0.5 font-sans text-sm leading-5 text-slate-400">{helper}</p>
        </div>
      </div>
      <div className="mt-2.5 min-w-0 sm:pl-6">{children}</div>
    </section>
  )
}

function DimensionSelect<T extends string>({
  ariaLabel,
  label,
  icon: Icon,
  value,
  options,
  onChange,
  className,
}: {
  ariaLabel: string
  label: string
  icon: LucideIcon
  value: string
  options: readonly { id: T; label: string }[]
  onChange: (value: T) => void
  className?: string
}) {
  return (
    <ControlSelect
      ariaLabel={ariaLabel}
      label={label}
      value={value}
      onChange={(nextValue) => onChange(nextValue as T)}
      options={options.map((option) => ({ value: option.id, label: option.label }))}
      icon={<Icon className="h-3 w-3" strokeWidth={1.8} />}
      labelMode="always"
      className={cn("w-full max-w-full", className)}
      menuClassName="min-w-[11rem] sm:min-w-[13rem]"
    />
  )
}

function ChoiceChips<T extends string | number>({
  options,
  selectedValues,
  onToggle,
}: {
  options: readonly { id: T; label: string; color: string }[]
  selectedValues: readonly T[]
  onToggle: (value: T) => void
}) {
  return (
    <div className="flex min-w-0 flex-wrap gap-2">
      {options.map((option) => {
        const selected = selectedValues.includes(option.id)
        return (
          <ControlChip
            key={String(option.id)}
            selected={selected}
            aria-pressed={selected}
            onClick={() => onToggle(option.id)}
            size="sm"
            className="min-h-8 max-w-full min-w-0 gap-1.5 rounded-lg"
          >
            <span
              className="h-2 w-2 shrink-0 rounded-full"
              style={{ backgroundColor: option.color }}
            />
            <span className="min-w-0 truncate">{option.label}</span>
          </ControlChip>
        )
      })}
    </div>
  )
}

type QueryComparisonRow = ComparisonRow & { valueMs: number | null; outcome: QueryOutcome }

function RuntimeRankingBars({ rows }: { rows: readonly QueryComparisonRow[] }) {
  const maximum = Math.max(0, ...rows.map((row) => row.valueMs ?? 0))
  if (rows.length === 0) return <BodyText>No selected measurements are available.</BodyText>
  return (
    <div className="space-y-5" role="list" aria-label="Recorded query outcomes">
      {rows.map((row) => (
        <div
          role="listitem"
          key={row.id}
          data-query-outcome={row.outcome.kind}
          data-query-engine={row.database}
          className="space-y-2"
        >
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <span className="text-base font-medium text-slate-100">{row.label}</span>
            <span className="font-mono text-base text-slate-100 tabular-nums">
              {row.valueMs === null ? row.outcome.label : formatLatencyMs(row.valueMs)}
            </span>
          </div>
          {row.valueMs !== null ? (
            <div
              role="meter"
              aria-label={row.label + " recorded latency"}
              aria-valuemin={0}
              aria-valuemax={maximum}
              aria-valuenow={row.valueMs}
              aria-valuetext={formatLatencyMs(row.valueMs)}
              className="h-2 overflow-hidden rounded-full bg-surface-primary"
            >
              <div
                className="h-full rounded-full"
                style={{ width: (row.valueMs / maximum) * 100 + "%", backgroundColor: row.color }}
              />
            </div>
          ) : null}
          <p className="text-sm leading-6 text-slate-400">
            {row.detail} · {row.outcome.label}
          </p>
        </div>
      ))}
    </div>
  )
}

interface HeatmapSort {
  seriesId: string
  direction: "best" | "worst"
}

function QueryBreakdown({
  rows,
  queryRows,
  selectedQuery,
  onSelectQuery,
}: {
  rows: readonly ComparisonRow[]
  queryRows: readonly QueryRow[]
  selectedQuery: string
  onSelectQuery: (query: string) => void
}) {
  const [sort, setSort] = useState<HeatmapSort | null>(null)
  const columnCount = useHeatmapColumnCount(rows.length, queryRows.length)
  const sorted = useMemo(() => {
    if (!sort) return queryRows
    const index = rows.findIndex((row) => row.id === sort.seriesId)
    if (index < 0) return queryRows
    return queryRows
      .map((row, order) => ({ row, order, value: getRelativeQueryScore(row, index) }))
      .sort((a, b) => {
        if (a.value === null && b.value === null) return a.order - b.order
        if (a.value === null) return 1
        if (b.value === null) return -1
        return (
          (sort.direction === "best" ? a.value - b.value : b.value - a.value) || a.order - b.order
        )
      })
      .map((entry) => entry.row)
  }, [sort, queryRows, rows])
  const size = Math.ceil(sorted.length / columnCount)
  const blocks = Array.from({ length: columnCount }, (_, index) =>
    sorted.slice(index * size, (index + 1) * size),
  )
  function toggleSort(seriesId: string) {
    setSort((current) =>
      current?.seriesId !== seriesId
        ? { seriesId, direction: "best" }
        : current.direction === "best"
          ? { seriesId, direction: "worst" }
          : null,
    )
  }
  return (
    <div className={cn("grid min-w-0 gap-4", columnCount === 2 && "xl:grid-cols-2")}>
      {blocks.map((block, index) => (
        <div
          key={index}
          data-heatmap-scroll
          className="panel-scrollbar max-h-[32rem] min-w-0 overflow-auto rounded-md border border-border-default"
        >
          <table
            aria-label={"Query latencies" + (columnCount > 1 ? " part " + (index + 1) : "")}
            className="w-full min-w-max border-collapse text-sm"
          >
            <thead
              data-heatmap-header
              data-heatmap-header-block
              className="sticky top-0 z-10 bg-surface-inset"
            >
              <tr>
                <th
                  scope="col"
                  className="w-56 min-w-56 border-b border-border-default px-3 py-3 text-left font-medium text-slate-300"
                >
                  Query
                </th>
                {rows.map((row) => {
                  const direction = sort?.seriesId === row.id ? sort.direction : null
                  return (
                    <th
                      key={row.id}
                      scope="col"
                      aria-sort={
                        direction === "best"
                          ? "ascending"
                          : direction === "worst"
                            ? "descending"
                            : "none"
                      }
                      className="min-w-32 border-b border-border-default p-1 text-left"
                    >
                      <button
                        type="button"
                        data-heatmap-series-header={row.id}
                        data-sort-series={row.id}
                        data-sort-direction={direction ?? "none"}
                        aria-label={
                          row.label +
                          ": sort by " +
                          (direction === "best"
                            ? "worst"
                            : direction === "worst"
                              ? "original"
                              : "best") +
                          " relative performance"
                        }
                        onClick={() => toggleSort(row.id)}
                        className="flex min-h-11 w-full items-center justify-between gap-2 rounded px-2 font-medium text-slate-200 outline-none hover:bg-surface-raised focus-visible:ring-2 focus-visible:ring-accent-300"
                      >
                        <span>{row.label}</span>
                        {direction === "best" ? (
                          <ArrowUp className="size-4" />
                        ) : direction === "worst" ? (
                          <ArrowDown className="size-4" />
                        ) : (
                          <ChevronsUpDown className="size-4 text-slate-400" />
                        )}
                      </button>
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody data-heatmap-block>
              {block.map((query) => (
                <tr
                  key={query.query}
                  className={selectedQuery === query.query ? "bg-surface-elevated" : ""}
                >
                  <th
                    scope="row"
                    className="max-w-56 border-t border-border-subtle p-1 text-left align-middle"
                  >
                    <button
                      type="button"
                      data-query-name={query.query}
                      onClick={() => onSelectQuery(query.query)}
                      aria-pressed={selectedQuery === query.query}
                      className="min-h-11 w-full rounded px-2 py-2 text-left font-medium break-words whitespace-normal text-slate-200 outline-none hover:bg-surface-raised focus-visible:ring-2 focus-visible:ring-accent-300"
                    >
                      {query.query}
                    </button>
                  </th>
                  {rows.map((row, seriesIndex) => {
                    const value = query.values[seriesIndex] ?? null
                    const outcome = query.outcomes[seriesIndex]!
                    return (
                      <td
                        key={row.id}
                        data-query={query.query}
                        data-series={row.id}
                        data-query-state={outcome.kind}
                        aria-label={
                          query.query +
                          ", " +
                          row.label +
                          ": " +
                          (value === null
                            ? outcome.label
                            : formatLatencyMs(value) + "; " + outcome.label)
                        }
                        className="border-t border-border-subtle px-2 py-3 text-center align-middle text-slate-100"
                        style={outcome.kind === "measured" ? getHeatCellStyle(value) : undefined}
                      >
                        {value === null ? (
                          outcome.label
                        ) : (
                          <>
                            <span className="font-mono tabular-nums">{formatLatencyMs(value)}</span>
                            {outcome.kind !== "measured" ? (
                              <span className="mt-1 block text-sm text-slate-300">
                                {outcome.label}
                              </span>
                            ) : null}
                          </>
                        )}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}

function getRelativeQueryScore(row: QueryRow, index: number) {
  const value = row.values[index]
  if (value === null || value === undefined || row.outcomes[index]?.kind !== "measured") return null
  const others = row.values.filter(
    (candidate, i): candidate is number =>
      i !== index && candidate !== null && row.outcomes[i]?.kind === "measured",
  )
  return others.length ? value / Math.min(...others) : null
}

function useHeatmapColumnCount(seriesCount: number, queryCount: number): 1 | 2 {
  const [width, setWidth] = useState(() => window.innerWidth)
  useEffect(() => {
    const update = () => setWidth(window.innerWidth)
    window.addEventListener("resize", update)
    return () => window.removeEventListener("resize", update)
  }, [])
  return seriesCount > 0 && seriesCount <= 3 && queryCount > 12 && width >= 1280 ? 2 : 1
}

function getHeatCellStyle(value: number | null) {
  if (value === null) return {}
  const position = Math.max(0, Math.min(1, Math.log10(Math.max(1, value)) / 5))
  return { backgroundColor: "hsl(215 55% " + (30 - position * 16) + "%)" }
}

function SetupSkeleton() {
  return (
    <div className="grid gap-3 md:grid-cols-2 2xl:grid-cols-5">
      {Array.from({ length: 5 }, (_, index) => (
        <div key={index} className="space-y-3 rounded-lg bg-surface-inset p-3">
          <div className="flex items-center gap-3">
            <Skeleton className="h-8 w-8 rounded-lg" />
            <div className="flex-1 space-y-2">
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-3 w-40" />
            </div>
          </div>
          <Skeleton className="h-9 w-full rounded-full" />
        </div>
      ))}
    </div>
  )
}

function ResultsSkeleton() {
  return (
    <>
      <div className="grid gap-2 sm:grid-cols-3">
        {Array.from({ length: 3 }, (_, index) => (
          <ChartFrame key={index}>
            <Skeleton className="h-24 w-full rounded-lg" />
          </ChartFrame>
        ))}
      </div>
      <ChartFrame>
        <Skeleton className="h-80 w-full rounded-lg" />
      </ChartFrame>
    </>
  )
}

function compareVersions(a: string, b: string): number {
  return a.localeCompare(b, undefined, { numeric: true })
}

function resolveSelection({
  mode,
  variants,
  raw,
  preferredSystem,
  defaultScale,
}: {
  mode: ComparisonMode
  variants: readonly RunVariant[]
  raw: RawSelection
  preferredSystem: string | null
  defaultScale: number
}): ResolvedSelection {
  if (variants.length === 0) return emptySelection()

  const allSystems = unique(variants.map((variant) => variant.system)).sort()
  const allDatabases = unique(variants.map((variant) => variant.database)).sort()

  if (mode === "system") {
    const database = choose(raw.database, allDatabases)
    const versionOptions = unique(
      variants.filter((variant) => variant.database === database).map((variant) => variant.version),
    ).sort(compareVersions)
    const version = choose(raw.version, versionOptions)
    const scaleOptions = unique(
      variants
        .filter((variant) => variant.database === database && variant.version === version)
        .map((variant) => variant.scale),
    ).sort((a, b) => a - b)
    const scale = chooseNumber(raw.scale, scaleOptions, defaultScale)
    const systemOptions = unique(
      variants
        .filter(
          (variant) =>
            variant.database === database && variant.version === version && variant.scale === scale,
        )
        .map((variant) => variant.system),
    ).sort()
    const systems = normalizeSubset(raw.systems, systemOptions, systemOptions)

    return makeResolvedSelection({
      system: choose(raw.system, systemOptions, preferredSystem),
      systems,
      systemOptions,
      database,
      databases: [database],
      databaseOptions: allDatabases,
      scale,
      scales: [scale],
      scaleOptions,
      version,
      versions: [version],
      versionOptions,
      variants,
    })
  }

  const system = choose(raw.system, allSystems, preferredSystem)
  const systemVariants = variants.filter((variant) => variant.system === system)
  const databaseOptions = unique(systemVariants.map((variant) => variant.database)).sort()

  if (mode === "database") {
    const scaleOptions = unique(systemVariants.map((variant) => variant.scale)).sort(
      (a, b) => a - b,
    )
    const scale = chooseNumber(raw.scale, scaleOptions, defaultScale)
    const availableVariants = systemVariants.filter((variant) => variant.scale === scale)
    const availableDatabases = unique(availableVariants.map((variant) => variant.database)).sort()
    const databases = normalizeSubset(raw.databases, availableDatabases, availableDatabases)
    const latestVersions = latestVersionsByDatabase(availableVariants, databases)
    const database = choose(raw.database, databases)
    const version = latestVersions.get(database) ?? ""

    return {
      system,
      systems: [system],
      systemOptions: allSystems,
      database,
      databases,
      databaseOptions: availableDatabases,
      scale,
      scales: [scale],
      scaleOptions,
      version,
      versions: version ? [version] : [],
      versionOptions: version ? [version] : [],
      latestVersions,
      ready: databases.length > 0,
    }
  }

  const database = choose(raw.database, databaseOptions)
  const databaseVariants = systemVariants.filter((variant) => variant.database === database)

  if (mode === "scale") {
    const versionOptions = unique(databaseVariants.map((variant) => variant.version)).sort(
      compareVersions,
    )
    const version = choose(raw.version, versionOptions)
    const scaleOptions = unique(
      databaseVariants
        .filter((variant) => variant.version === version)
        .map((variant) => variant.scale),
    ).sort((a, b) => a - b)
    const scales = normalizeSubset(raw.scales, scaleOptions, scaleOptions)
    const scale = chooseNumber(raw.scale, scales, defaultScale)

    return makeResolvedSelection({
      system,
      systems: [system],
      systemOptions: allSystems,
      database,
      databases: [database],
      databaseOptions,
      scale,
      scales,
      scaleOptions,
      version,
      versions: [version],
      versionOptions,
      variants,
    })
  }

  const scaleOptions = unique(databaseVariants.map((variant) => variant.scale)).sort(
    (a, b) => a - b,
  )
  const scale = chooseNumber(raw.scale, scaleOptions, defaultScale)
  const versionOptions = unique(
    databaseVariants.filter((variant) => variant.scale === scale).map((variant) => variant.version),
  ).sort(compareVersions)
  const versions = normalizeSubset(raw.versions, versionOptions, versionOptions)
  const version = choose(raw.version, versions)

  return makeResolvedSelection({
    system,
    systems: [system],
    systemOptions: allSystems,
    database,
    databases: [database],
    databaseOptions,
    scale,
    scales: [scale],
    scaleOptions,
    version,
    versions,
    versionOptions,
    variants,
  })
}

function makeResolvedSelection(
  selection: Omit<ResolvedSelection, "latestVersions" | "ready"> & {
    variants: readonly RunVariant[]
  },
): ResolvedSelection {
  const { variants, ...resolved } = selection
  return {
    ...resolved,
    latestVersions: latestVersionsByDatabase(variants, resolved.databases),
    ready: Boolean(resolved.system && resolved.database && resolved.version),
  }
}

function emptySelection(): ResolvedSelection {
  return {
    system: "",
    systems: [],
    systemOptions: [],
    database: "",
    databases: [],
    databaseOptions: [],
    scale: 0,
    scales: [],
    scaleOptions: [],
    version: "",
    versions: [],
    versionOptions: [],
    latestVersions: new Map(),
    ready: false,
  }
}

function buildComparisonRows({
  mode,
  metrics,
  variants,
  selection,
  databaseColors,
  runs,
  comparison,
  family,
}: {
  mode: ComparisonMode
  metrics: readonly ExplorerQueryMetric[]
  variants: readonly RunVariant[]
  selection: ResolvedSelection
  databaseColors: Readonly<Record<string, string>>
  runs: readonly RunObservation[]
  comparison: ComparisonManifest | null
  family: ComparisonFamily | undefined
}): ComparisonRow[] {
  if (!selection.ready) return []

  const selectedVariants: { variant: RunVariant; label: string; detail: string; color: string }[] =
    []

  if (mode === "database") {
    for (const database of selection.databases) {
      const variant = findLatestVariant(
        variants.filter(
          (candidate) =>
            candidate.system === selection.system &&
            candidate.database === database &&
            candidate.scale === selection.scale,
        ),
      )
      if (variant) {
        selectedVariants.push({
          variant,
          label: formatDatabaseName(database),
          detail: `${variant.version} / SF ${variant.scale}`,
          color: databaseColors[database] ?? getSeriesColor(selectedVariants.length),
        })
      }
    }
  } else if (mode === "scale") {
    for (const scale of selection.scales) {
      const variant = findExactVariant(variants, {
        system: selection.system,
        database: selection.database,
        version: selection.version,
        scale,
      })
      if (variant) {
        selectedVariants.push({
          variant,
          label: `SF ${scale}`,
          detail: `${formatDatabaseName(variant.database)} ${variant.version}`,
          color: getSeriesColor(selectedVariants.length),
        })
      }
    }
  } else if (mode === "version") {
    for (const version of selection.versions) {
      const variant = findExactVariant(variants, {
        system: selection.system,
        database: selection.database,
        version,
        scale: selection.scale,
      })
      if (variant) {
        selectedVariants.push({
          variant,
          label: version,
          detail: `${formatDatabaseName(variant.database)} / SF ${variant.scale}`,
          color: getSeriesColor(selectedVariants.length),
        })
      }
    }
  } else {
    for (const system of selection.systems) {
      const variant = findExactVariant(variants, {
        system,
        database: selection.database,
        version: selection.version,
        scale: selection.scale,
      })
      if (variant) {
        selectedVariants.push({
          variant,
          label: system,
          detail: `${formatDatabaseName(variant.database)} ${variant.version} / SF ${variant.scale}`,
          color: getSeriesColor(selectedVariants.length),
        })
      }
    }
  }

  return selectedVariants.map(({ variant, label, detail, color }) => {
    const queryValues = new Map(
      metrics
        .filter((metric) => metric.run_id === variant.runId)
        .map((metric) => [metric.query_name, metric.median_duration_s * 1000] as const),
    )
    const run = runs.find((entry) => entry.runId === variant.runId)
    const problems = run
      ? runPerformanceProblems(
          run,
          comparison ? matchingAnnotations(run, run.db, run.suite, run.scale, comparison) : [],
        )
      : []
    const queryOutcomes = new Map(
      (family?.coverage_query_names ?? [...queryValues.keys()]).map((name) => {
        const query = run && comparison ? observeQuery(run, name, runs, comparison) : undefined
        return [
          name,
          describeQueryOutcome(query, run, family?.query_names.includes(name) ?? false, problems),
        ]
      }),
    )
    return {
      id: String(variant.runId),
      label,
      detail,
      color,
      database: variant.database,
      systemSnapshotId: variant.systemSnapshotId,
      queryValues,
      queryOutcomes,
    }
  })
}

function makeRunVariants(runs: readonly RunObservation[]): RunVariant[] {
  const variants = new Map<number, RunVariant>()
  for (const run of runs) {
    variants.set(run.runId, {
      runId: run.runId,
      system: run.system,
      scale: run.scale,
      database: run.db,
      version: run.driver === null ? run.version : run.version + " · " + run.driver,
      systemSnapshotId: run.snapshotId,
      finishedAt: run.finishedAt,
    })
  }
  return [...variants.values()]
}

function makeQueryRows(queryNames: readonly string[], rows: readonly ComparisonRow[]): QueryRow[] {
  return queryNames.map((query) => ({
    query,
    values: rows.map((row) => row.queryValues.get(query) ?? null),
    outcomes: rows.map(
      (row) =>
        row.queryOutcomes.get(query) ?? describeQueryOutcome(undefined, undefined, false, []),
    ),
  }))
}

function resolveSql(entry: QuerySqlEntry | undefined, database: string) {
  if (!entry) return "-- SQL is not available for this query."
  return entry.db_overrides[database] ?? entry.sql ?? "-- SQL is not available for this database."
}

function sqlDiffersFromDefault(entry: QuerySqlEntry | undefined, database: string): boolean {
  if (!entry) return false
  const databaseSql = entry.db_overrides[database] ?? entry.sql
  if (!databaseSql) return false
  const defaultSql = getDefaultSql(entry)
  return defaultSql === null || normalizeSql(databaseSql) !== defaultSql
}

function getDefaultSql(entry: QuerySqlEntry): string | null {
  if (entry.sql) return normalizeSql(entry.sql)

  const counts = new Map<string, number>()
  for (const sql of Object.values(entry.db_overrides)) {
    const normalizedSql = normalizeSql(sql)
    counts.set(normalizedSql, (counts.get(normalizedSql) ?? 0) + 1)
  }
  let defaultSql: string | null = null
  let defaultCount = 1
  for (const [sql, count] of counts) {
    if (count > defaultCount) {
      defaultSql = sql
      defaultCount = count
    }
  }
  return defaultSql
}

function normalizeSql(sql: string): string {
  return sql.replace(/\r\n?/g, "\n").trim()
}

function parseUrlState(
  searchParams: URLSearchParams,
  preferredSystem: string | null,
): RawSelection {
  return {
    system: searchParams.get("system") ?? preferredSystem ?? "",
    systems: splitParam(searchParams.get("systems")),
    database: searchParams.get("database") ?? "",
    databases: splitParam(searchParams.get("databases")),
    scale: parseNumber(searchParams.get("scale")),
    scales: splitParam(searchParams.get("scales")).map(Number).filter(Number.isFinite),
    version: searchParams.get("version") ?? "",
    versions: splitParam(searchParams.get("versions")),
  }
}

function parseComparisonMode(value: string | null): ComparisonMode {
  return COMPARISON_MODES.some((comparisonMode) => comparisonMode.id === value)
    ? (value as ComparisonMode)
    : "database"
}

function parseNumber(value: string | null) {
  if (value === null) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function splitParam(value: string | null) {
  return (
    value
      ?.split(",")
      .map((item) => item.trim())
      .filter(Boolean) ?? []
  )
}

function choose(value: string, options: readonly string[], alternate?: string | null) {
  if (options.includes(value)) return value
  if (alternate && options.includes(alternate)) return alternate
  return options[0] ?? ""
}

function chooseNumber(value: number | null, options: readonly number[], alternate?: number) {
  if (value !== null && options.includes(value)) return value
  if (alternate !== undefined && options.includes(alternate)) return alternate
  return options[0] ?? 0
}

function normalizeSubset<T>(values: readonly T[], options: readonly T[], fallback: readonly T[]) {
  const selected = options.filter((option) => values.includes(option))
  return selected.length > 0 ? selected : [...fallback]
}

function latestVersionsByDatabase(variants: readonly RunVariant[], databases: readonly string[]) {
  return new Map(
    databases.flatMap((database) => {
      const latest = findLatestVariant(variants.filter((variant) => variant.database === database))
      return latest ? [[database, latest.version] as const] : []
    }),
  )
}

function findLatestVariant(variants: readonly RunVariant[]) {
  return [...variants].sort((a, b) => b.finishedAt.localeCompare(a.finishedAt))[0]
}

function findExactVariant(
  variants: readonly RunVariant[],
  selection: Pick<RunVariant, "system" | "database" | "version" | "scale">,
) {
  return findLatestVariant(
    variants.filter(
      (variant) =>
        variant.system === selection.system &&
        variant.database === selection.database &&
        variant.version === selection.version &&
        variant.scale === selection.scale,
    ),
  )
}

function toggleSelection<T>(values: readonly T[], value: T): T[] {
  if (values.includes(value)) {
    return values.length <= 1 ? [...values] : values.filter((candidate) => candidate !== value)
  }
  return [...values, value]
}

function unique<T>(values: readonly T[]) {
  return [...new Set(values)]
}

function getModeLabel(mode: ComparisonMode, selection: ResolvedSelection) {
  if (mode === "database") return "Database comparison"
  const database = formatDatabaseName(selection.database)
  if (mode === "scale") return `${database} across scale factors`
  if (mode === "version") return `${database} version comparison`
  return `${database} across systems`
}

function getModeIcon(mode: ComparisonMode): LucideIcon {
  return COMPARISON_MODES.find((comparisonMode) => comparisonMode.id === mode)?.icon ?? Database
}

function getModePluralLabel(mode: ComparisonMode) {
  return COMPARISON_MODES.find((comparisonMode) => comparisonMode.id === mode)?.label ?? "Databases"
}

function getVaryingOptionCount(mode: ComparisonMode, selection: ResolvedSelection | undefined) {
  if (!selection) return 0
  if (mode === "database") return selection.databaseOptions.length
  if (mode === "scale") return selection.scaleOptions.length
  if (mode === "version") return selection.versionOptions.length
  return selection.systemOptions.length
}

function getSeriesColor(index: number) {
  return SERIES_COLORS[index % SERIES_COLORS.length] ?? SERIES_COLORS[0]
}
