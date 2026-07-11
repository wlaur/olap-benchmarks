import {
  ArrowDown,
  ArrowUp,
  ChevronDown,
  ChevronUp,
  ChevronsUpDown,
  Database,
  FlaskConical,
  Gauge,
  GitBranch,
  Scale,
  Search,
  Server,
  Trophy,
  type LucideIcon,
} from "lucide-react"
import { Fragment, useEffect, useMemo, useRef, useState, type ReactNode } from "react"
import { useNavigate, useSearchParams } from "react-router-dom"

import { ControlChip, QuietButton, SegmentedButton } from "../components/controls/Control"
import { ControlSelect } from "../components/controls/ControlSelect"
import { VirtualizedSelect } from "../components/controls/VirtualizedSelect"
import { ChartFrame, PanelCard, PanelHeader } from "../components/layout/Panel"
import { Skeleton } from "../components/Skeleton"
import { SqlCodeView } from "../components/SqlCodeView"
import { MetaLabel, SectionTitle } from "../components/Typography"
import { useExplorerData } from "../hooks/useExplorerData"
import type { BenchmarkDefinition, BenchmarkSuiteId } from "../lib/benchmarks"
import { cn } from "../lib/cn"
import { getDatabaseColors } from "../lib/databaseColors"
import { computeRelativeSeriesScores, formatScore } from "../lib/score"
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
  finishedAt: string
}

interface ComparisonRow {
  id: string
  label: string
  detail: string
  color: string
  valueMs: number
  database: string
  queryValues: Map<string, number>
}

interface ScoredComparisonRow extends ComparisonRow {
  score: number
  queryCount: number
  wins: number
  missing: number
}

interface QueryRow {
  query: string
  values: (number | null)[]
}

export function ExplorerPage({
  suiteId,
  suiteDefinition,
  benchmarkDefinitions,
  preferredSystem,
}: ExplorerPageProps) {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [initialState] = useState(() => parseUrlState(searchParams, preferredSystem))
  const [mode, setMode] = useState<ComparisonMode>(() =>
    parseComparisonMode(searchParams.get("mode")),
  )
  const [singleDatabaseMode, setSingleDatabaseMode] = useState<SingleDatabaseMode>(() => {
    const initialMode = parseComparisonMode(searchParams.get("mode"))
    return initialMode === "database" ? "scale" : initialMode
  })
  const [rawSelection, setRawSelection] = useState<RawSelection>(initialState)
  const [requestedQuery, setRequestedQuery] = useState(searchParams.get("query") ?? "")
  const [requestedSqlDatabase, setRequestedSqlDatabase] = useState(
    searchParams.get("sql_database") ?? "",
  )
  const [showAllQueries, setShowAllQueries] = useState(false)
  const { metrics, queriesManifest, loading, error } = useExplorerData(suiteId)
  const variants = useMemo(() => makeRunVariants(metrics), [metrics])
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
  const comparisonRows = useMemo(
    () => buildComparisonRows({ mode, metrics, variants, selection, databaseColors }),
    [databaseColors, metrics, mode, selection, variants],
  )
  const manifestEntries = useMemo(
    () => queriesManifest[suiteDefinition.queriesKey] ?? {},
    [queriesManifest, suiteDefinition.queriesKey],
  )
  const rankedRows = useMemo(() => {
    const scores = new Map(
      computeRelativeSeriesScores(
        comparisonRows.map((row) => ({ key: row.id, queryValues: row.queryValues })),
        Object.keys(manifestEntries),
        10,
      ).map((score) => [score.key, score]),
    )
    return comparisonRows
      .map((row): ScoredComparisonRow => ({ ...row, ...scores.get(row.id)! }))
      .sort((left, right) => left.score - right.score || left.label.localeCompare(right.label))
  }, [comparisonRows, manifestEntries])
  const scoredQueryCount = rankedRows[0]
    ? rankedRows[0].queryCount + rankedRows[0].missing
    : Object.keys(manifestEntries).length
  const queryNames = useMemo(
    () => orderQueryNames(Object.keys(manifestEntries), rankedRows),
    [manifestEntries, rankedRows],
  )
  const queryOptions = useMemo(
    () => queryNames.map((query) => ({ value: query, label: query })),
    [queryNames],
  )
  const selectedQuery = queryNames.includes(requestedQuery) ? requestedQuery : (queryNames[0] ?? "")
  const queryRows = useMemo(() => makeQueryRows(queryNames, rankedRows), [queryNames, rankedRows])
  const sqlDatabaseOptions = useMemo(
    () =>
      unique(
        mode === "database"
          ? rankedRows.map((row) => row.database)
          : selection.database
            ? [selection.database]
            : [],
      ),
    [mode, rankedRows, selection.database],
  )
  const sqlDatabase = sqlDatabaseOptions.includes(requestedSqlDatabase)
    ? requestedSqlDatabase
    : (sqlDatabaseOptions[0] ?? "")
  const selectedSqlEntry = manifestEntries[selectedQuery]
  const selectedSql = resolveSql(selectedSqlEntry, sqlDatabase)
  const sqlVariants = useMemo(
    () =>
      new Map(
        sqlDatabaseOptions.map((database) => [
          database,
          sqlDiffersFromDefault(selectedSqlEntry, database),
        ]),
      ),
    [selectedSqlEntry, sqlDatabaseOptions],
  )
  const hasDifferentSql = [...sqlVariants.values()].some(Boolean)
  const selectedQueryRows = useMemo(
    () =>
      rankedRows
        .flatMap((row) => {
          const valueMs = row.queryValues.get(selectedQuery)
          return valueMs === undefined ? [] : [{ ...row, valueMs }]
        })
        .sort((a, b) => a.valueMs - b.valueMs),
    [rankedRows, selectedQuery],
  )
  const queryDetailHeight = Math.min(480, Math.max(384, 112 + selectedQueryRows.length * 60))
  const nextSearchParams = useMemo(() => {
    if (!selection.ready || !selectedQuery) return ""
    const params = new URLSearchParams()
    params.set("mode", mode)
    params.set("system", selection.system)
    params.set("systems", selection.systems.join(","))
    params.set("database", selection.database)
    params.set("databases", selection.databases.join(","))
    params.set("scale", String(selection.scale))
    params.set("scales", selection.scales.join(","))
    params.set("version", selection.version)
    params.set("versions", selection.versions.join(","))
    params.set("query", selectedQuery)
    if (sqlDatabase) params.set("sql_database", sqlDatabase)
    return params.toString()
  }, [mode, selectedQuery, selection, sqlDatabase])

  useEffect(() => {
    if (nextSearchParams && searchParams.toString() !== nextSearchParams) {
      setSearchParams(nextSearchParams, { replace: true })
    }
  }, [nextSearchParams, searchParams, setSearchParams])

  function updateRawSelection(update: Partial<RawSelection>) {
    setRawSelection((current) => ({ ...current, ...update }))
  }

  function handleSuiteChange(nextSuiteId: string) {
    navigate(`/explorer/${nextSuiteId}?${nextSearchParams || `mode=${mode}`}`)
  }

  function handleSingleDatabaseModeChange(nextMode: SingleDatabaseMode) {
    setRawSelection((current) => ({
      ...current,
      databases: current.databases.length > 0 ? current.databases : selection.databases,
      scales: current.scales.length > 0 ? current.scales : selection.scales,
      versions: current.versions.length > 0 ? current.versions : selection.versions,
      systems: current.systems.length > 0 ? current.systems : selection.systems,
    }))
    setSingleDatabaseMode(nextMode)
    setMode(nextMode)
  }

  function handleCompareDatabases() {
    setRawSelection((current) => ({
      ...current,
      databases: current.databases.length > 0 ? current.databases : selection.databases,
      scales: current.scales.length > 0 ? current.scales : selection.scales,
      versions: current.versions.length > 0 ? current.versions : selection.versions,
      systems: current.systems.length > 0 ? current.systems : selection.systems,
    }))
    setMode("database")
  }

  function handleAnalyzeOneDatabase() {
    const nextMode =
      getVaryingOptionCount(singleDatabaseMode, singleDatabaseSelections.get(singleDatabaseMode)) >
      1
        ? singleDatabaseMode
        : (SINGLE_DATABASE_MODES.find(
            (candidate) =>
              getVaryingOptionCount(candidate.id, singleDatabaseSelections.get(candidate.id)) > 1,
          )?.id ?? singleDatabaseMode)
    handleSingleDatabaseModeChange(nextMode)
  }

  const showLoading = loading

  return (
    <div className="flex min-h-full w-full max-w-full min-w-0 shrink-0 flex-col gap-4 overflow-x-clip pb-8">
      <header className="grid gap-4 md:grid-cols-[minmax(20rem,1fr)_16rem] md:items-end">
        <div className="min-w-0">
          <MetaLabel>Explorer</MetaLabel>
          <h1 className="mt-1 text-2xl font-semibold text-slate-50 sm:text-3xl">
            Benchmark explorer
          </h1>
          <p className="mt-2 max-w-3xl font-sans text-sm leading-6 text-slate-400">
            Start with a comparison question, then move from the suite result to individual queries
            and SQL.
          </p>
        </div>
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
            }))}
            icon={<FlaskConical className="h-3 w-3" strokeWidth={1.8} />}
            labelMode="hidden"
            className="min-h-10 w-full"
            menuClassName="min-w-56"
          />
        </div>
      </header>

      <PanelCard className="p-3 sm:p-4">
        <PanelHeader className="flex-wrap sm:items-center">
          <div className="min-w-0">
            <MetaLabel>Analysis</MetaLabel>
            <SectionTitle as="h2" className="mt-1 text-lg">
              {mode === "database" ? "Compare databases" : "Analyze one database"}
            </SectionTitle>
            <p className="mt-1.5 font-sans text-sm leading-5 text-slate-400">
              {mode === "database"
                ? "Rank engines under one shared environment."
                : "Hold one database constant and vary scale, version, or system."}
            </p>
          </div>
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
              onClick={handleAnalyzeOneDatabase}
              size="sm"
              className="min-h-9 min-w-0 gap-2"
            >
              <Search className="h-3.5 w-3.5 shrink-0" strokeWidth={1.8} />
              Analyze one database
            </SegmentedButton>
          </div>
        </PanelHeader>

        <div className="mt-4 border-t border-border-subtle pt-4">
          {showLoading ? (
            <SetupSkeleton />
          ) : error ? (
            <div className="border-y border-red-500/30 bg-red-950/20 px-3 py-4 text-sm text-red-300">
              Failed to load explorer data: {error}
            </div>
          ) : !selection.ready ? (
            <div className="border-y border-border-subtle px-3 py-4 text-sm text-slate-500">
              No completed query results are available for this suite.
            </div>
          ) : mode === "database" ? (
            <div className="grid min-w-0 gap-3 xl:grid-cols-[minmax(0,1.3fr)_minmax(24rem,0.7fr)]">
              <AnalysisField
                icon={Database}
                label="Databases to compare"
                helper="Select the engines that should compete in the suite ranking."
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
                          selected={mode === comparisonMode.id}
                          aria-pressed={mode === comparisonMode.id}
                          onClick={() => handleSingleDatabaseModeChange(comparisonMode.id)}
                          size="sm"
                          className="min-h-9 min-w-0 gap-1.5 px-2"
                        >
                          <Icon className="h-3.5 w-3.5 shrink-0" strokeWidth={1.8} />
                          <span className="truncate">{comparisonMode.label}</span>
                          <span className="ml-auto shrink-0 text-[0.65rem] text-slate-500">
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
                      : "Select the values that should appear together in the ranking and query views."
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
      </PanelCard>

      <PanelCard className="min-w-0 space-y-4 p-3 sm:p-4">
        <PanelHeader className="flex-wrap">
          <div className="min-w-0">
            <MetaLabel>Suite result</MetaLabel>
            <SectionTitle as="h2" className="mt-1 text-lg sm:text-xl">
              {getModeLabel(mode, selection)}
            </SectionTitle>
            {!showLoading && selection.ready ? (
              <p className="mt-2 max-w-4xl font-sans text-sm leading-6 text-slate-400">
                {getComparisonDescription(
                  mode,
                  suiteDefinition.title,
                  selection,
                  rankedRows.length,
                )}
              </p>
            ) : null}
          </div>
        </PanelHeader>

        {showLoading ? (
          <ResultsSkeleton />
        ) : error ? (
          <div className="flex min-h-72 items-center justify-center border-y border-red-500/30 bg-red-950/20 px-4 text-sm text-red-300">
            Failed to load explorer data: {error}
          </div>
        ) : rankedRows.length === 0 ? (
          <div className="flex min-h-72 items-center justify-center border-y border-border-subtle text-sm text-slate-500">
            No completed query results match this comparison.
          </div>
        ) : rankedRows.length < 2 ? (
          <div className="rounded-md border border-border-subtle bg-surface-inset px-4 py-5">
            <p className="text-sm font-semibold text-slate-200">No comparison available</p>
            <p className="mt-1 max-w-3xl font-sans text-sm leading-6 text-slate-400">
              Only one recorded value matches this setup. Choose another database, dimension, or
              environment to compare suite scores. Query SQL and runtime remain available below.
            </p>
          </div>
        ) : (
          <>
            <OverviewStats rows={rankedRows} />

            <div className="border-t border-border-subtle pt-4">
              <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
                <div>
                  <MetaLabel>Overall ranking</MetaLabel>
                  <p className="mt-1 text-sm font-medium text-slate-200">
                    Normalized suite score across {scoredQueryCount} suite queries
                  </p>
                  <p className="mt-1 max-w-3xl font-sans text-xs leading-5 text-slate-500">
                    Geometric mean versus the fastest result per query, with 10ms smoothing and
                    missing-query penalties.
                  </p>
                </div>
                <div className="flex items-center gap-2 text-xs font-medium text-slate-400">
                  <Gauge className="h-3.5 w-3.5 text-accent-300" strokeWidth={1.8} />
                  Lower is better · 1.0× is ideal
                </div>
              </div>
              <ScoreRanking rows={rankedRows} />
            </div>
          </>
        )}
      </PanelCard>

      {!showLoading && !error && rankedRows.length > 0 ? (
        <PanelCard className="min-w-0 space-y-4 overflow-visible p-3 sm:p-4">
          <PanelHeader className="flex-wrap sm:flex-nowrap sm:items-end">
            <div className="min-w-0 flex-1">
              <MetaLabel>Query detail</MetaLabel>
              <SectionTitle as="h2" className="mt-1 text-lg sm:text-xl">
                Query performance and SQL
              </SectionTitle>
              <p className="mt-2 max-w-2xl font-sans text-sm leading-6 text-slate-400">
                Choose a query to compare its runtime and inspect the SQL used by each database.
              </p>
            </div>
            <VirtualizedSelect
              ariaLabel="Query"
              label="Selected query"
              value={selectedQuery}
              options={queryOptions}
              onChange={setRequestedQuery}
              icon={<Search className="h-3.5 w-3.5" strokeWidth={1.8} />}
              filterPlaceholder="Filter queries"
              className="w-full sm:w-[28rem] sm:shrink-0"
            />
          </PanelHeader>

          <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(18rem,0.72fr)_minmax(0,1.28fr)]">
            <ChartFrame
              className="flex flex-col overflow-hidden p-3 sm:p-4"
              style={{ height: queryDetailHeight }}
            >
              <div className="mb-4 shrink-0">
                <MetaLabel>Runtime for this query</MetaLabel>
                <p
                  className="mt-1 truncate text-sm font-medium text-slate-200"
                  title={selectedQuery}
                >
                  {selectedQuery}
                </p>
                <p className="mt-1 text-xs text-slate-500">Shorter bars are faster</p>
              </div>
              <div
                data-query-runtime-scroll
                className="panel-scrollbar min-h-0 flex-1 overflow-y-auto pr-1"
              >
                <RuntimeRankingBars rows={selectedQueryRows} />
              </div>
            </ChartFrame>

            <ChartFrame
              className="flex flex-col overflow-hidden p-3 sm:p-4"
              style={{ height: queryDetailHeight }}
            >
              <div className="mb-3 shrink-0 space-y-3">
                <div>
                  <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                    <MetaLabel>SQL used</MetaLabel>
                    {hasDifferentSql ? (
                      <p className="flex items-center gap-1.5 text-[0.6875rem] text-slate-500">
                        <span
                          className="h-1.5 w-1.5 rounded-full bg-amber-300"
                          aria-hidden="true"
                        />
                        Differs from default SQL
                      </p>
                    ) : null}
                  </div>
                  <p className="mt-1 truncate text-sm font-medium text-slate-200">
                    {selectedQuery}
                  </p>
                </div>
                {sqlDatabaseOptions.length > 0 ? (
                  <div className="panel-scrollbar flex gap-1 overflow-x-auto pb-1">
                    {sqlDatabaseOptions.map((database) => {
                      const differsFromDefault = sqlVariants.get(database) ?? false
                      const databaseName = formatDatabaseName(database)
                      return (
                        <SegmentedButton
                          key={database}
                          selected={sqlDatabase === database}
                          aria-pressed={sqlDatabase === database}
                          onClick={() => setRequestedSqlDatabase(database)}
                          size="xs"
                          className="shrink-0 gap-1.5 rounded-md"
                          data-sql-variant={differsFromDefault ? "different" : "default"}
                          title={
                            differsFromDefault
                              ? `${databaseName} SQL differs from the default`
                              : `${databaseName} uses the default SQL`
                          }
                        >
                          {differsFromDefault ? (
                            <span
                              className="h-1.5 w-1.5 shrink-0 rounded-full bg-amber-300 shadow-[0_0_0_2px_rgba(252,211,77,0.12)]"
                              aria-hidden="true"
                            />
                          ) : null}
                          {databaseName}
                        </SegmentedButton>
                      )
                    })}
                  </div>
                ) : null}
              </div>
              <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border-subtle bg-surface-primary/40">
                <SqlCodeView code={selectedSql} wrapLines className="text-xs leading-relaxed" />
              </div>
            </ChartFrame>
          </div>

          <div className="border-t border-border-subtle pt-4">
            <QuietButton
              size="sm"
              onClick={() => setShowAllQueries((current) => !current)}
              aria-expanded={showAllQueries}
              className="gap-2 rounded-lg"
            >
              {showAllQueries ? (
                <ChevronUp className="h-3.5 w-3.5" strokeWidth={1.8} />
              ) : (
                <ChevronDown className="h-3.5 w-3.5" strokeWidth={1.8} />
              )}
              {showAllQueries ? "Hide" : "Show"} all {queryRows.length} query results
            </QuietButton>
            {showAllQueries ? (
              <ChartFrame className="mt-4 flex w-fit max-w-full flex-col overflow-visible p-3 sm:p-4">
                <div className="mb-3 flex shrink-0 flex-wrap items-end justify-between gap-3">
                  <div>
                    <MetaLabel>All queries</MetaLabel>
                    <p className="mt-1 text-sm font-medium text-slate-200">
                      Select a row for detail; click a database header to sort
                    </p>
                  </div>
                  <div className="flex items-center gap-2 text-[0.6875rem] text-slate-500">
                    <span>Faster</span>
                    <span
                      className="h-2 w-20 rounded-sm border border-border-subtle bg-[linear-gradient(90deg,hsl(155_58%_25%),hsl(30_70%_27%))]"
                      aria-hidden="true"
                    />
                    <span>Slower</span>
                  </div>
                </div>
                <QueryBreakdown
                  rows={rankedRows}
                  queryRows={queryRows}
                  selectedQuery={selectedQuery}
                  onSelectQuery={setRequestedQuery}
                />
              </ChartFrame>
            ) : null}
          </div>
        </PanelCard>
      ) : null}
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
          <p className="mt-0.5 font-sans text-xs leading-5 text-slate-400">{helper}</p>
        </div>
      </div>
      <div className="mt-2.5 min-w-0 sm:pl-6">{children}</div>
    </section>
  )
}

function OverviewStats({ rows }: { rows: readonly ScoredComparisonRow[] }) {
  const leader = rows[0]
  const last = rows.at(-1)
  if (!leader || !last) return null
  const scoredQueryCount = leader.queryCount + leader.missing
  const scoreRange =
    Number.isFinite(leader.score) && Number.isFinite(last.score) && leader.score > 0
      ? last.score / leader.score
      : null

  return (
    <div className="grid overflow-hidden rounded-md border border-border-subtle bg-surface-inset sm:grid-cols-3 sm:divide-x sm:divide-border-subtle">
      <div className="border-b border-border-subtle px-4 py-3 sm:border-b-0">
        <div className="flex items-center gap-2 text-xs font-semibold tracking-wide text-slate-500 uppercase">
          <Trophy className="h-3.5 w-3.5 text-amber-300" strokeWidth={1.8} />
          Best suite score
        </div>
        <p className="mt-2 truncate text-xl font-semibold text-slate-50">{leader.label}</p>
        <p className="mt-1 text-xs text-slate-400">{formatScore(leader.score)} normalized score</p>
      </div>
      <div className="border-b border-border-subtle px-4 py-3 sm:border-b-0">
        <p className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          Winning queries
        </p>
        <p className="mt-2 text-xl font-semibold text-slate-50">
          {leader.wins}/{leader.queryCount}
        </p>
        <p className="mt-1 truncate text-xs text-slate-400" title={leader.label}>
          Fastest completed results ({leader.label})
        </p>
      </div>
      <div className="px-4 py-3">
        <p className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          Scored workload
        </p>
        <p className="mt-2 text-xl font-semibold text-slate-50">{scoredQueryCount} queries</p>
        <p className="mt-1 text-xs text-slate-400">
          {scoreRange === null
            ? "Missing results are penalized"
            : `${scoreRange.toFixed(1)}× score spread; missing results penalized`}
        </p>
      </div>
    </div>
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

function ScoreRanking({ rows }: { rows: readonly ScoredComparisonRow[] }) {
  return (
    <div className="divide-y divide-border-subtle overflow-hidden rounded-md border border-border-subtle bg-surface-primary/25">
      {rows.map((row, index) => (
        <div
          key={row.id}
          data-score-ranking-row={row.id}
          className={cn(
            "grid min-w-0 grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 px-3 py-2.5",
            index === 0 && "bg-amber-300/[0.035]",
          )}
        >
          <span className="flex w-7 items-center gap-1 text-xs font-semibold text-slate-500">
            {index === 0 ? (
              <Trophy className="h-3.5 w-3.5 text-amber-300" strokeWidth={1.8} />
            ) : null}
            <span>#{index + 1}</span>
          </span>
          <div className="min-w-0">
            <div className="flex min-w-0 items-center gap-2">
              <span
                className="h-2.5 w-2.5 shrink-0 rounded-full"
                style={{ backgroundColor: row.color }}
              />
              <span className="truncate text-sm font-medium text-slate-100">{row.label}</span>
            </div>
            <p className="mt-1 truncate text-[0.7rem] text-slate-500">
              {row.wins}/{row.queryCount} fastest
              {row.missing > 0 ? ` · ${row.missing} missing penalized` : ""} · {row.detail}
            </p>
          </div>
          <span
            data-score={row.score}
            className="text-base font-semibold text-slate-50 tabular-nums"
          >
            {formatScore(row.score)}
          </span>
        </div>
      ))}
    </div>
  )
}

function RuntimeRankingBars({ rows }: { rows: readonly ComparisonRow[] }) {
  const bestRuntime = Math.min(...rows.map((row) => row.valueMs))
  const slowestRuntime = Math.max(...rows.map((row) => row.valueMs))

  return (
    <div className="space-y-3">
      {rows.map((row, index) => {
        const runtimeWidth = slowestRuntime > 0 ? (row.valueMs / slowestRuntime) * 100 : 0
        return (
          <div key={row.id} className="grid gap-1.5">
            <div className="flex items-center justify-between gap-3">
              <div className="flex min-w-0 items-center gap-2">
                <span className="w-5 shrink-0 text-xs font-semibold text-slate-500">
                  #{index + 1}
                </span>
                <span
                  className="h-2.5 w-2.5 shrink-0 rounded-full"
                  style={{ backgroundColor: row.color }}
                />
                <span className="truncate text-sm font-medium text-slate-100">{row.label}</span>
              </div>
              <span className="shrink-0 text-sm font-semibold text-slate-100">
                {formatRuntime(row.valueMs)}
              </span>
            </div>
            <div
              role="meter"
              aria-label={`${row.label} runtime`}
              aria-valuemin={0}
              aria-valuemax={slowestRuntime}
              aria-valuenow={row.valueMs}
              aria-valuetext={formatRuntime(row.valueMs)}
              className="h-2 overflow-hidden rounded-full bg-surface-primary"
            >
              <div
                className="h-full rounded-full"
                style={{ width: `${Math.max(4, runtimeWidth)}%`, backgroundColor: row.color }}
              />
            </div>
            <div className="flex items-center justify-between gap-2 text-[0.7rem] text-slate-500">
              <span className="truncate">{row.detail}</span>
              <span>
                {index === 0 || bestRuntime <= 0
                  ? "Fastest"
                  : `${(row.valueMs / bestRuntime).toFixed(1)}× slower`}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
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
  const headerScrollRef = useRef<HTMLDivElement>(null)
  const columnCount = useHeatmapColumnCount(rows.length, queryRows.length)
  const sortedQueryRows = useMemo(() => {
    if (!sort) return queryRows
    const seriesIndex = rows.findIndex((row) => row.id === sort.seriesId)
    if (seriesIndex < 0) return queryRows

    return queryRows
      .map((queryRow, originalIndex) => ({
        queryRow,
        originalIndex,
        score: getRelativeQueryScore(queryRow.values, seriesIndex),
      }))
      .sort((left, right) => {
        if (left.score === null && right.score === null) {
          return left.originalIndex - right.originalIndex
        }
        if (left.score === null) return 1
        if (right.score === null) return -1
        const difference = left.score - right.score
        if (difference === 0) return left.originalIndex - right.originalIndex
        return sort.direction === "best" ? difference : -difference
      })
      .map(({ queryRow }) => queryRow)
  }, [queryRows, rows, sort])
  const splitIndex = Math.ceil(sortedQueryRows.length / columnCount)
  const queryGroups =
    columnCount === 1
      ? [sortedQueryRows]
      : [sortedQueryRows.slice(0, splitIndex), sortedQueryRows.slice(splitIndex)]

  function handleSort(seriesId: string) {
    setSort((current) => {
      if (current?.seriesId !== seriesId) return { seriesId, direction: "best" }
      if (current.direction === "best") return { seriesId, direction: "worst" }
      return null
    })
  }

  return (
    <div className="max-w-full min-w-0">
      <div
        ref={headerScrollRef}
        data-heatmap-header
        className="sticky -top-4 z-30 max-w-full overflow-hidden border-b border-border-default bg-surface-inset shadow-[0_5px_12px_rgba(0,0,0,0.28)] lg:-top-5"
      >
        <div className="flex w-max min-w-full items-start gap-4">
          {queryGroups.map((group, index) => (
            <QueryHeatmapHeaderBlock
              key={`header-${index}-${group[0]?.query ?? "empty"}`}
              rows={rows}
              sort={sort}
              onSort={handleSort}
            />
          ))}
        </div>
      </div>
      <div
        data-heatmap-scroll
        className="panel-scrollbar max-w-full overflow-x-auto overflow-y-hidden pb-1"
        onScroll={(event) => {
          if (headerScrollRef.current) {
            headerScrollRef.current.scrollLeft = event.currentTarget.scrollLeft
          }
        }}
      >
        <div className="flex w-max min-w-full items-start gap-4 pt-0.5">
          {queryGroups.map((group, index) => (
            <QueryHeatmapRowsBlock
              key={`${index}-${group[0]?.query ?? "empty"}`}
              rows={rows}
              queryRows={group}
              selectedQuery={selectedQuery}
              onSelectQuery={onSelectQuery}
            />
          ))}
        </div>
      </div>
    </div>
  )
}

interface HeatmapSort {
  seriesId: string
  direction: "best" | "worst"
}

function QueryHeatmapHeaderBlock({
  rows,
  sort,
  onSort,
}: {
  rows: readonly ComparisonRow[]
  sort: HeatmapSort | null
  onSort: (seriesId: string) => void
}) {
  return (
    <div
      data-heatmap-header-block
      className="grid shrink-0 gap-0.5 text-xs"
      style={{ gridTemplateColumns: `10rem repeat(${rows.length}, 4.6875rem)` }}
    >
      <div className="flex h-9 items-center border-r border-border-default bg-surface-inset pr-2 font-semibold text-slate-500">
        Query
      </div>
      {rows.map((row) => {
        const direction = sort?.seriesId === row.id ? sort.direction : null
        const nextAction =
          direction === "best" ? "worst" : direction === "worst" ? "original" : "best"
        return (
          <button
            key={row.id}
            type="button"
            data-heatmap-series-header={row.id}
            data-sort-series={row.id}
            data-sort-direction={direction ?? "none"}
            aria-pressed={direction !== null}
            aria-label={`${row.label}: sort by ${nextAction} relative performance`}
            title={`${row.label} — ${direction === "best" ? "best relative performance first" : direction === "worst" ? "worst relative performance first" : "original query order"}`}
            onClick={() => onSort(row.id)}
            className={cn(
              "flex h-9 min-w-0 items-center gap-1 bg-surface-inset px-1.5 text-left font-medium transition-colors outline-none",
              "hover:bg-surface-raised focus-visible:ring-2 focus-visible:ring-slate-300/25 focus-visible:ring-inset",
              direction ? "text-slate-50" : "text-slate-300",
            )}
          >
            <span
              className="h-1.5 w-1.5 shrink-0 rounded-full"
              style={{ backgroundColor: row.color }}
            />
            <span className="min-w-0 flex-1 truncate">{row.label}</span>
            {direction === "best" ? (
              <ArrowUp className="h-3 w-3 shrink-0 text-accent-300" strokeWidth={2} />
            ) : direction === "worst" ? (
              <ArrowDown className="h-3 w-3 shrink-0 text-accent-300" strokeWidth={2} />
            ) : (
              <ChevronsUpDown className="h-3 w-3 shrink-0 text-slate-600" strokeWidth={1.7} />
            )}
          </button>
        )
      })}
    </div>
  )
}

function QueryHeatmapRowsBlock({
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
  return (
    <div
      data-heatmap-block
      className="grid shrink-0 gap-0.5 text-xs"
      style={{ gridTemplateColumns: `10rem repeat(${rows.length}, 4.6875rem)` }}
    >
      {queryRows.map((queryRow) => {
        const availableValues = queryRow.values.filter((value): value is number => value !== null)
        const fastestValue = Math.min(...availableValues)
        const slowestValue = Math.max(...availableValues)
        const selected = selectedQuery === queryRow.query
        return (
          <Fragment key={queryRow.query}>
            <div className="min-w-0 border-r border-border-subtle bg-surface-inset py-0.5 pr-2">
              <button
                type="button"
                onClick={() => onSelectQuery(queryRow.query)}
                title={queryRow.query}
                data-query-name={queryRow.query}
                className={cn(
                  "block h-8 w-full min-w-0 truncate rounded-sm px-2 text-left font-medium transition-colors outline-none",
                  "focus-visible:ring-2 focus-visible:ring-slate-300/20",
                  selected
                    ? "bg-accent-400/10 text-slate-50"
                    : "text-slate-400 hover:bg-surface-raised/70 hover:text-slate-200",
                )}
              >
                {queryRow.query}
              </button>
            </div>
            {queryRow.values.map((value, index) => {
              const row = rows[index]
              if (!row) return null
              const heatStyle = getHeatCellStyle(value, fastestValue, slowestValue)
              return (
                <div
                  key={`${queryRow.query}-${row.id}`}
                  aria-label={`${queryRow.query}, ${row.label}: ${value === null ? "not run" : formatRuntime(value)}`}
                  data-query={queryRow.query}
                  data-series={row.id}
                  className={cn(
                    "flex h-9 items-center justify-center rounded-sm border px-1 text-center font-mono text-[0.6875rem] font-medium tabular-nums",
                    value === null ? "border-border-subtle text-slate-600" : "text-slate-100",
                    selected && "ring-1 ring-accent-300/30",
                  )}
                  style={heatStyle}
                >
                  {value === null ? "-" : formatRuntime(value)}
                </div>
              )
            })}
          </Fragment>
        )
      })}
    </div>
  )
}

function getRelativeQueryScore(values: readonly (number | null)[], seriesIndex: number) {
  const selectedValue = values[seriesIndex]
  if (selectedValue === null || selectedValue === undefined) return null
  const competitorValues = values.filter(
    (value, index): value is number => index !== seriesIndex && value !== null,
  )
  if (competitorValues.length === 0) return null
  return selectedValue / Math.min(...competitorValues)
}

function useHeatmapColumnCount(seriesCount: number, queryCount: number): 1 | 2 {
  const [viewportWidth, setViewportWidth] = useState(() => window.innerWidth)

  useEffect(() => {
    function updateViewportWidth() {
      setViewportWidth(window.innerWidth)
    }
    window.addEventListener("resize", updateViewportWidth)
    return () => window.removeEventListener("resize", updateViewportWidth)
  }, [])

  const canSplit = seriesCount > 0 && queryCount > 12 && viewportWidth >= 1280
  return canSplit ? 2 : 1
}

function getHeatCellStyle(value: number | null, fastestValue: number, slowestValue: number) {
  if (value === null) return { backgroundColor: "rgba(7, 10, 15, 0.55)" }
  const range = slowestValue - fastestValue
  const position = range > 0 ? Math.max(0, Math.min(1, (value - fastestValue) / range)) : 0
  const hue = 155 - position * 125
  return {
    backgroundColor: `hsl(${hue} 58% ${17 + position * 6}%)`,
    borderColor: `hsl(${hue} 50% ${30 + position * 8}% / 0.72)`,
  }
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
          <Skeleton key={index} className="h-24 w-full rounded-lg" />
        ))}
      </div>
      <Skeleton className="h-80 w-full rounded-lg" />
    </>
  )
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
    ).sort()
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
    const versionOptions = unique(databaseVariants.map((variant) => variant.version)).sort()
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
  ).sort()
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
}: {
  mode: ComparisonMode
  metrics: readonly ExplorerQueryMetric[]
  variants: readonly RunVariant[]
  selection: ResolvedSelection
  databaseColors: Readonly<Record<string, string>>
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
    return {
      id: String(variant.runId),
      label,
      detail,
      color,
      valueMs: median([...queryValues.values()]),
      database: variant.database,
      queryValues,
    }
  })
}

function makeRunVariants(metrics: readonly ExplorerQueryMetric[]): RunVariant[] {
  const variants = new Map<number, RunVariant>()
  for (const metric of metrics) {
    variants.set(metric.run_id, {
      runId: metric.run_id,
      system: metric.system,
      scale: metric.suite_scale_factor,
      database: metric.db,
      version: metric.db_version,
      finishedAt: metric.finished_at,
    })
  }
  return [...variants.values()]
}

function orderQueryNames(manifestNames: readonly string[], rows: readonly ComparisonRow[]) {
  const available = new Set(rows.flatMap((row) => [...row.queryValues.keys()]))
  const ordered = manifestNames.filter((query) => available.has(query))
  const unlisted = [...available].filter((query) => !manifestNames.includes(query)).sort()
  return [...ordered, ...unlisted]
}

function makeQueryRows(queryNames: readonly string[], rows: readonly ComparisonRow[]): QueryRow[] {
  return queryNames.map((query) => ({
    query,
    values: rows.map((row) => row.queryValues.get(query) ?? null),
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

function median(values: readonly number[]) {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  const upper = sorted[middle] ?? 0
  return sorted.length % 2 === 0 ? ((sorted[middle - 1] ?? upper) + upper) / 2 : upper
}

function getModeLabel(mode: ComparisonMode, selection: ResolvedSelection) {
  if (mode === "database") return "Database ranking"
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

function getComparisonDescription(
  mode: ComparisonMode,
  suiteTitle: string,
  selection: ResolvedSelection,
  seriesCount: number,
) {
  if (mode === "database") {
    return `${suiteTitle} · ${selection.system} · SF ${selection.scale} · ${seriesCount} databases · latest completed version per database`
  }
  if (mode === "scale") {
    return `${suiteTitle} · ${selection.version} · ${selection.system} · ${seriesCount} recorded scale ${seriesCount === 1 ? "factor" : "factors"}`
  }
  if (mode === "version") {
    return `${suiteTitle} · ${selection.system} · SF ${selection.scale} · ${seriesCount} recorded ${seriesCount === 1 ? "version" : "versions"}`
  }
  return `${suiteTitle} · ${selection.version} · SF ${selection.scale} · ${seriesCount} recorded ${seriesCount === 1 ? "system" : "systems"}`
}

function formatRuntime(valueMs: number) {
  if (valueMs >= 1000) return `${(valueMs / 1000).toFixed(2)}s`
  if (valueMs >= 100) return `${Math.round(valueMs)}ms`
  if (valueMs >= 10) return `${valueMs.toFixed(1)}ms`
  return `${valueMs.toFixed(2)}ms`
}

function getSeriesColor(index: number) {
  return SERIES_COLORS[index % SERIES_COLORS.length] ?? SERIES_COLORS[0]
}

function formatDatabaseName(database: string) {
  const labels: Record<string, string> = {
    clickhouse: "ClickHouse",
    datafusion: "DataFusion",
    duckdb: "DuckDB",
    monetdb: "MonetDB",
    postgres: "PostgreSQL",
    questdb: "QuestDB",
    starrocks: "StarRocks",
    timescaledb: "TimescaleDB",
  }
  return labels[database] ?? database
}
