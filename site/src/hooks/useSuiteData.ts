import { startTransition, useCallback, useEffect, useMemo, useState } from "react"

import type { BenchmarkSuiteId } from "../lib/benchmarks"
import {
  fetchInsertSteps,
  fetchMetricSamples,
  fetchMutateSteps,
  fetchMutateSummaries,
  fetchOperationSummaries,
  fetchQueriesManifest,
  fetchQuerySteps,
  fetchQuerySummaries,
  fetchRunSummaries,
} from "../lib/queries"
import type { SuiteConfig } from "../lib/suiteConfig"
import type {
  InsertStep,
  MetricSample,
  OperationSummary,
  QueriesManifest,
  QueryStep,
  QuerySummary,
  RunSummary,
} from "../lib/types"

export interface SuiteDataState {
  loading: boolean
  deferredLoading: boolean
  error: string | null
  runSummaries: RunSummary[]
  operationSummaries: OperationSummary[]
  metricSamples: MetricSample[]
  querySummaries: QuerySummary[]
  mutateSummaries: QuerySummary[]
  insertSteps: InsertStep[]
  querySteps: QueryStep[]
  mutateSteps: QueryStep[]
  queriesManifest: QueriesManifest | null
}

function createInitialState(): SuiteDataState {
  return {
    loading: true,
    deferredLoading: true,
    error: null,
    runSummaries: [],
    operationSummaries: [],
    metricSamples: [],
    querySummaries: [],
    mutateSummaries: [],
    insertSteps: [],
    querySteps: [],
    mutateSteps: [],
    queriesManifest: null,
  }
}

export interface UseSuiteDataResult {
  state: SuiteDataState
  databases: string[]
  selectedDatabases: string[]
  setSelectedDatabases: React.Dispatch<React.SetStateAction<string[]>>
  includedDatabases: string[]
  filteredQuerySummaries: QuerySummary[]
  filteredMutateSummaries: QuerySummary[]
  filteredOperationSummaries: OperationSummary[]
  filteredQuerySteps: QueryStep[]
  filteredMutateSteps: QueryStep[]
  isLoading: boolean
  toggleDatabase: (database: string) => void
}

export function useSuiteData(
  system: string | null,
  suite: BenchmarkSuiteId,
  suiteConfig: SuiteConfig,
  isSystemLoading: boolean,
): UseSuiteDataResult {
  const [state, setState] = useState<SuiteDataState>(createInitialState)
  const [selectedDatabases, setSelectedDatabases] = useState<string[]>([])

  useEffect(() => {
    if (isSystemLoading || system === null) {
      startTransition(() => {
        setState(createInitialState())
      })
      return
    }

    let cancelled = false

    setState(createInitialState())

    const hasMutate = suiteConfig.operations.includes("mutate")

    // Phase 1: critical data for above-fold panels
    Promise.all([
      fetchRunSummaries(system, suite),
      fetchOperationSummaries(system, suite),
      fetchQuerySummaries(system, suite),
      hasMutate ? fetchMutateSummaries(system, suite) : Promise.resolve([] as QuerySummary[]),
      fetchQueriesManifest().catch(() => null),
    ])
      .then(
        ([runSummaries, operationSummaries, querySummaries, mutateSummaries, queriesManifest]) => {
          if (cancelled) return

          startTransition(() => {
            setState((prev) => ({
              ...prev,
              loading: false,
              runSummaries,
              operationSummaries,
              querySummaries,
              mutateSummaries,
              queriesManifest,
            }))
          })
        },
      )
      .catch((nextError) => {
        if (cancelled) return

        startTransition(() => {
          setState({
            ...createInitialState(),
            loading: false,
            deferredLoading: false,
            error: String(nextError),
          })
        })
      })

    // Phase 2: deferred data for below-fold panels (timeline, insert perf, resource trends)
    Promise.all([
      fetchMetricSamples(system, suite),
      fetchInsertSteps(system, suite),
      fetchQuerySteps(system, suite),
      hasMutate ? fetchMutateSteps(system, suite) : Promise.resolve([] as QueryStep[]),
    ])
      .then(([metricSamples, insertSteps, querySteps, mutateSteps]) => {
        if (cancelled) return

        startTransition(() => {
          setState((prev) => ({
            ...prev,
            deferredLoading: false,
            metricSamples,
            insertSteps,
            querySteps,
            mutateSteps,
          }))
        })
      })
      .catch(() => {
        if (cancelled) return

        startTransition(() => {
          setState((prev) => ({
            ...prev,
            deferredLoading: false,
          }))
        })
      })

    return () => {
      cancelled = true
    }
  }, [isSystemLoading, system, suite, suiteConfig.operations])

  const databases = useMemo(
    () => Array.from(new Set(state.runSummaries.map((run) => run.db))).sort(),
    [state.runSummaries],
  )

  useEffect(() => {
    setSelectedDatabases((currentSelection) => {
      if (databases.length === 0) return []

      const nextSelection = databases.filter((database) => currentSelection.includes(database))
      const resolvedSelection = nextSelection.length > 0 ? nextSelection : databases

      if (
        resolvedSelection.length === currentSelection.length &&
        resolvedSelection.every((database, index) => database === currentSelection[index])
      ) {
        return currentSelection
      }

      return resolvedSelection
    })
  }, [databases])

  const includedDatabases = useMemo(
    () => (selectedDatabases.length > 0 ? selectedDatabases : databases),
    [selectedDatabases, databases],
  )

  const includedDatabaseSet = useMemo(() => new Set(includedDatabases), [includedDatabases])

  const filteredQuerySummaries = useMemo(
    () => state.querySummaries.filter((row) => includedDatabaseSet.has(row.db)),
    [state.querySummaries, includedDatabaseSet],
  )
  const filteredMutateSummaries = useMemo(
    () => state.mutateSummaries.filter((row) => includedDatabaseSet.has(row.db)),
    [state.mutateSummaries, includedDatabaseSet],
  )
  const filteredOperationSummaries = useMemo(
    () => state.operationSummaries.filter((run) => includedDatabaseSet.has(run.db)),
    [state.operationSummaries, includedDatabaseSet],
  )

  const filteredQuerySteps = state.querySteps
  const filteredMutateSteps = state.mutateSteps

  const isLoading = isSystemLoading || state.loading

  const toggleDatabase = useCallback((database: string) => {
    setSelectedDatabases((currentSelection) => {
      const nextSelection = currentSelection.includes(database)
        ? currentSelection.filter((value) => value !== database)
        : [...currentSelection, database].sort()

      return nextSelection.length > 0 ? nextSelection : currentSelection
    })
  }, [])

  return {
    state,
    databases,
    selectedDatabases,
    setSelectedDatabases,
    includedDatabases,
    filteredQuerySummaries,
    filteredMutateSummaries,
    filteredOperationSummaries,
    filteredQuerySteps,
    filteredMutateSteps,
    isLoading,
    toggleDatabase,
  }
}
