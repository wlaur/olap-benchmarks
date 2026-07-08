import { create } from "zustand"
import { persist } from "zustand/middleware"

import { fetchBenchmarkDefinitions, type BenchmarkDefinition } from "../lib/benchmarks"
import { fetchSystems } from "../lib/queries"

const STORE_VERSION = 8

interface AppSlice {
  benchmarkDefinitions: BenchmarkDefinition[]
  suitesLoading: boolean
  suitesError: string | null
  loadSuites: () => Promise<void>
  systems: string[]
  selectedSystem: string | null
  systemLoading: boolean
  systemError: string | null
  setSelectedSystem: (system: string | null) => void
  loadSystems: () => Promise<void>
}

export const useAppStore = create<AppSlice>()(
  persist(
    (set, get) => ({
      benchmarkDefinitions: [],
      suitesLoading: true,
      suitesError: null,

      systems: [],
      selectedSystem: null,
      systemLoading: true,
      systemError: null,

      loadSuites: async () => {
        try {
          const benchmarkDefinitions = await fetchBenchmarkDefinitions()
          set({
            benchmarkDefinitions,
            suitesError: null,
            suitesLoading: false,
          })
        } catch (error) {
          set({ suitesError: String(error), suitesLoading: false })
        }
      },

      setSelectedSystem: (system) => set({ selectedSystem: system }),

      loadSystems: async () => {
        try {
          const systems = await fetchSystems()
          const { selectedSystem } = get()
          const nextSystem = systems.includes(selectedSystem ?? "")
            ? selectedSystem
            : (systems[0] ?? null)
          set({
            systems,
            selectedSystem: nextSystem,
            systemError: null,
            systemLoading: false,
          })
        } catch (error) {
          set({ systemError: String(error), systemLoading: false })
        }
      },
    }),
    {
      name: "olap-benchmarks",
      version: STORE_VERSION,
      partialize: (state) => ({
        selectedSystem: state.selectedSystem,
      }),
    },
  ),
)
