import { useEffect, useSyncExternalStore } from "react"
import {
  defaultBenchmarkId,
  getBenchmarkDefinition,
  getBenchmarkHref,
  type BenchmarkSuiteId,
} from "../lib/benchmarks"

function parseBenchmarkHash(hash: string): BenchmarkSuiteId {
  const match = hash.match(/^#\/benchmarks\/([^/?]+)/)
  const candidate = match?.[1] as BenchmarkSuiteId | undefined

  if (!candidate) {
    return defaultBenchmarkId
  }

  return getBenchmarkDefinition(candidate).id
}

function subscribe(callback: () => void): () => void {
  window.addEventListener("hashchange", callback)
  return () => {
    window.removeEventListener("hashchange", callback)
  }
}

function getSnapshot(): BenchmarkSuiteId {
  return parseBenchmarkHash(window.location.hash)
}

export function useBenchmarkRoute(): BenchmarkSuiteId {
  const currentBenchmark = useSyncExternalStore(
    subscribe,
    getSnapshot,
    () => defaultBenchmarkId,
  )

  useEffect(() => {
    const expectedHash = getBenchmarkHref(currentBenchmark)

    if (window.location.hash === expectedHash) {
      return
    }

    window.history.replaceState(
      null,
      "",
      `${window.location.pathname}${window.location.search}${expectedHash}`,
    )
  }, [currentBenchmark])

  return currentBenchmark
}
