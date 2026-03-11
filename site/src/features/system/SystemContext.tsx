import {
  createContext,
  startTransition,
  useContext,
  useEffect,
  useEffectEvent,
  useState,
  type ReactNode,
} from "react"
import { fetchSystems } from "../../lib/queries"

interface SystemContextValue {
  systems: string[]
  selectedSystem: string | null
  setSelectedSystem: (system: string) => void
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

  const applyLoadedSystems = useEffectEvent((loadedSystems: string[]) => {
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

  const applyLoadError = useEffectEvent((nextError: unknown) => {
    startTransition(() => {
      setError(String(nextError))
      setLoading(false)
    })
  })

  useEffect(() => {
    let cancelled = false

    fetchSystems()
      .then((loadedSystems) => {
        if (cancelled) return
        applyLoadedSystems(loadedSystems)
      })
      .catch((nextError) => {
        if (cancelled) return
        applyLoadError(nextError)
      })

    return () => {
      cancelled = true
    }
  }, [applyLoadedSystems, applyLoadError])

  useEffect(() => {
    if (!selectedSystem) return
    window.localStorage.setItem(STORAGE_KEY, selectedSystem)
  }, [selectedSystem])

  const setSelectedSystem = (system: string) => {
    setSelectedSystemState(system)
  }

  return (
    <SystemContext.Provider
      value={{
        systems,
        selectedSystem,
        setSelectedSystem,
        loading,
        error,
      }}
    >
      {children}
    </SystemContext.Provider>
  )
}

export function useSystem(): SystemContextValue {
  const value = useContext(SystemContext)

  if (!value) {
    throw new Error("useSystem must be used within a SystemProvider")
  }

  return value
}
