import { create } from "zustand"
import { persist } from "zustand/middleware"

import { fetchSystems } from "../lib/queries"

const STORE_VERSION = 7

interface SystemSlice {
  systems: string[]
  selectedSystem: string | null
  systemLoading: boolean
  systemError: string | null
  setSelectedSystem: (system: string | null) => void
  loadSystems: () => Promise<void>
}

export const useAppStore = create<SystemSlice>()(
  persist(
    (set, get) => ({
      systems: [],
      selectedSystem: null,
      systemLoading: true,
      systemError: null,

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
