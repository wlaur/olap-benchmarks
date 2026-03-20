import type { ResponsiveLayouts } from "react-grid-layout"
import { create } from "zustand"
import { persist } from "zustand/middleware"

import { fetchSystems } from "../lib/queries"

const LAYOUT_VERSION = 6

interface SystemSlice {
  systems: string[]
  selectedSystem: string | null
  systemLoading: boolean
  systemError: string | null
  setSelectedSystem: (system: string | null) => void
  loadSystems: () => Promise<void>
}

interface LayoutSlice {
  layouts: ResponsiveLayouts
  layoutModified: boolean
  setLayouts: (layouts: ResponsiveLayouts) => void
  markLayoutModified: () => void
  resetLayouts: () => void
}

type AppStore = SystemSlice & LayoutSlice

export const DEFAULT_LAYOUTS: ResponsiveLayouts = {
  lg: [
    { i: "overview", x: 0, y: 0, w: 4, h: 10, minH: 8 },
    { i: "insert-performance", x: 4, y: 0, w: 8, h: 12, minH: 9 },
    { i: "operations", x: 0, y: 12, w: 12, h: 24, minH: 16 },
    { i: "resource-trends", x: 0, y: 36, w: 7, h: 18, minH: 10 },
    { i: "flame-graph", x: 7, y: 36, w: 5, h: 18, minH: 10 },
  ],
  md: [
    { i: "overview", x: 0, y: 0, w: 4, h: 10, minH: 8 },
    { i: "insert-performance", x: 4, y: 0, w: 6, h: 12, minH: 9 },
    { i: "operations", x: 0, y: 12, w: 10, h: 24, minH: 16 },
    { i: "resource-trends", x: 0, y: 36, w: 10, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 52, w: 10, h: 16, minH: 10 },
  ],
  sm: [
    { i: "overview", x: 0, y: 0, w: 6, h: 10, minH: 7 },
    { i: "insert-performance", x: 0, y: 10, w: 6, h: 12, minH: 8 },
    { i: "operations", x: 0, y: 22, w: 6, h: 24, minH: 16 },
    { i: "resource-trends", x: 0, y: 46, w: 6, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 62, w: 6, h: 16, minH: 10 },
  ],
  xs: [
    { i: "overview", x: 0, y: 0, w: 4, h: 10, minH: 7 },
    { i: "insert-performance", x: 0, y: 10, w: 4, h: 12, minH: 8 },
    { i: "operations", x: 0, y: 22, w: 4, h: 24, minH: 16 },
    { i: "resource-trends", x: 0, y: 46, w: 4, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 62, w: 4, h: 16, minH: 10 },
  ],
}

export const useAppStore = create<AppStore>()(
  persist(
    (set, get) => ({
      // System slice
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

      // Layout slice
      layouts: DEFAULT_LAYOUTS,
      layoutModified: false,

      setLayouts: (layouts) => set({ layouts }),

      markLayoutModified: () => set({ layoutModified: true }),

      resetLayouts: () => set({ layouts: DEFAULT_LAYOUTS, layoutModified: false }),
    }),
    {
      name: "olap-benchmarks",
      version: LAYOUT_VERSION,
      migrate: () => ({
        layouts: DEFAULT_LAYOUTS,
        layoutModified: false,
      }),
      partialize: (state) => ({
        selectedSystem: state.selectedSystem,
        layouts: state.layouts,
        layoutModified: state.layoutModified,
      }),
    },
  ),
)
