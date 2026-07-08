import { startTransition, useCallback, useEffect, useMemo, useState } from "react"

import type { BenchmarkSuiteId } from "../lib/benchmarks"
import {
  fetchInsertSteps,
  fetchMetricSamples,
  fetchMutateSteps,
  fetchMutateSummaries,
  fetchOperationSummaries,
  fetchQueryCoverage,
  fetchQueriesManifest,
  fetchQuerySteps,
  fetchQuerySummaries,
  fetchRunSummaries,
  fetchSuiteScaleFactors,
} from "../lib/queries"
import type { SuiteConfig } from "../lib/suiteConfig"
import type {
  InsertStep,
  MetricSample,
  OperationSummary,
  QueriesManifest,
  QueryCoverage,
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
  queryCoverage: QueryCoverage[]
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
    queryCoverage: [],
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
  scaleFactors: number[]
  selectedScaleFactor: number | null
  setSelectedScaleFactor: React.Dispatch<React.SetStateAction<number | null>>
  filteredQuerySummaries: QuerySummary[]
  filteredQueryCoverage: QueryCoverage[]
  filteredMutateSummaries: QuerySummary[]
  filteredOperationSummaries: OperationSummary[]
  isLoading: boolean
  toggleDatabase: (database: string) => void
}

export function useSuiteData(
  system: string | null,
  suite: BenchmarkSuiteId,
  suiteConfig: SuiteConfig,
  isSystemLoading: boolean,
  preferredScaleFactor: number | null = null,
): UseSuiteDataResult {
  const [state, setState] = useState<SuiteDataState>(createInitialState)
  const [selectedDatabases, setSelectedDatabases] = useState<string[]>([])
  const [scaleFactors, setScaleFactors] = useState<number[]>([])
  const [selectedScaleFactor, setSelectedScaleFactor] = useState<number | null>(null)

  useEffect(() => {
    if (isSystemLoading || system === null) {
      startTransition(() => {
        setState(createInitialState())
        setScaleFactors([])
        setSelectedScaleFactor(null)
      })
      return
    }

    let cancelled = false

    setState(createInitialState())
    setScaleFactors([])
    setSelectedScaleFactor(null)

    fetchSuiteScaleFactors(system, suite)
      .then((nextScaleFactors) => {
        if (cancelled) return

        startTransition(() => {
          setScaleFactors(nextScaleFactors)
          if (nextScaleFactors.length === 0) {
            setSelectedScaleFactor(null)
            setState({
              ...createInitialState(),
              loading: false,
              deferredLoading: false,
            })
            return
          }

          setSelectedScaleFactor((currentScaleFactor) => {
            if (preferredScaleFactor !== null && nextScaleFactors.includes(preferredScaleFactor)) {
              return preferredScaleFactor
            }
            if (currentScaleFactor !== null && nextScaleFactors.includes(currentScaleFactor)) {
              return currentScaleFactor
            }
            if (nextScaleFactors.includes(suiteConfig.defaultScaleFactor)) {
              return suiteConfig.defaultScaleFactor
            }
            return nextScaleFactors[0]!
          })
        })
      })
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

    return () => {
      cancelled = true
    }
  }, [isSystemLoading, preferredScaleFactor, system, suite, suiteConfig.defaultScaleFactor])

  useEffect(() => {
    if (isSystemLoading || system === null || selectedScaleFactor === null) {
      return
    }

    let cancelled = false

    setState(createInitialState())

    const hasMutate = suiteConfig.operations.includes("mutate")

    // Phase 1: critical data for above-fold panels
    Promise.all([
      fetchRunSummaries(system, suite, selectedScaleFactor),
      fetchOperationSummaries(system, suite, selectedScaleFactor),
      fetchQueryCoverage(system, suite, selectedScaleFactor),
      fetchQuerySummaries(system, suite, selectedScaleFactor),
      hasMutate
        ? fetchMutateSummaries(system, suite, selectedScaleFactor)
        : Promise.resolve([] as QuerySummary[]),
      fetchQueriesManifest(),
    ])
      .then(
        ([
          runSummaries,
          operationSummaries,
          queryCoverage,
          querySummaries,
          mutateSummaries,
          queriesManifest,
        ]) => {
          if (cancelled) return

          startTransition(() => {
            setState((prev) => ({
              ...prev,
              loading: false,
              runSummaries,
              operationSummaries,
              queryCoverage,
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
      fetchMetricSamples(system, suite, selectedScaleFactor),
      fetchInsertSteps(system, suite, selectedScaleFactor),
      fetchQuerySteps(system, suite, selectedScaleFactor),
      hasMutate
        ? fetchMutateSteps(system, suite, selectedScaleFactor)
        : Promise.resolve([] as QueryStep[]),
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
  }, [isSystemLoading, selectedScaleFactor, system, suite, suiteConfig.operations])

  const databases = useMemo(
    () =>
      Array.from(
        new Set([
          ...state.runSummaries.map((run) => run.db),
          ...state.queryCoverage.map((run) => run.db),
        ]),
      ).sort(),
    [state.runSummaries, state.queryCoverage],
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
  const filteredQueryCoverage = useMemo(
    () => state.queryCoverage.filter((row) => includedDatabaseSet.has(row.db)),
    [state.queryCoverage, includedDatabaseSet],
  )
  const filteredMutateSummaries = useMemo(
    () => state.mutateSummaries.filter((row) => includedDatabaseSet.has(row.db)),
    [state.mutateSummaries, includedDatabaseSet],
  )
  const filteredOperationSummaries = useMemo(
    () => state.operationSummaries.filter((run) => includedDatabaseSet.has(run.db)),
    [state.operationSummaries, includedDatabaseSet],
  )

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
    scaleFactors,
    selectedScaleFactor,
    setSelectedScaleFactor,
    filteredQuerySummaries,
    filteredQueryCoverage,
    filteredMutateSummaries,
    filteredOperationSummaries,
    isLoading,
    toggleDatabase,
  }
}
