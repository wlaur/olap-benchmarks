import {
  createContext,
  startTransition,
  useContext,
  useEffect,
  useMemo,
  useState,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
} from "react"
import { fetchSystems } from "../../lib/queries"

interface SystemContextValue {
  systems: string[]
  selectedSystem: string | null
  setSelectedSystem: Dispatch<SetStateAction<string | null>>
  loading: boolean
  error: string | null
}

const STORAGE_KEY = "olap-benchmarks:selected-system"

const SystemContext = createContext<SystemContextValue | null>(null)

export function SystemProvider({ children }: { children: ReactNode }) {
  const [systems, setSystems] = useState<string[]>([])
  const [selectedSystem, setSelectedSystemState] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    fetchSystems()
      .then((loadedSystems) => {
        if (cancelled) return

        const storedSystem = window.localStorage.getItem(STORAGE_KEY)
        const nextSelectedSystem = loadedSystems.includes(storedSystem ?? "")
          ? storedSystem
          : (loadedSystems[0] ?? null)

        startTransition(() => {
          setSystems(loadedSystems)
          setSelectedSystemState(nextSelectedSystem)
          setError(null)
          setLoading(false)
        })
      })
      .catch((nextError) => {
        if (cancelled) return

        startTransition(() => {
          setError(String(nextError))
          setLoading(false)
        })
      })

    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!selectedSystem) return
    window.localStorage.setItem(STORAGE_KEY, selectedSystem)
  }, [selectedSystem])

  const value = useMemo<SystemContextValue>(
    () => ({
      systems,
      selectedSystem,
      setSelectedSystem: setSelectedSystemState,
      loading,
      error,
    }),
    [systems, selectedSystem, loading, error],
  )

  return (
    <SystemContext.Provider value={value}>{children}</SystemContext.Provider>
  )
}

export function useSystem(): SystemContextValue {
  const value = useContext(SystemContext)

  if (!value) {
    throw new Error("useSystem must be used within a SystemProvider")
  }

  return value
}
