import type { ResponsiveLayouts } from "react-grid-layout"
import { create } from "zustand"
import { persist } from "zustand/middleware"

import { fetchSystems } from "../lib/queries"

const LAYOUT_VERSION = 1

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
    { i: "overview", x: 0, y: 0, w: 12, h: 10, minH: 8 },
    { i: "insert-performance", x: 0, y: 10, w: 12, h: 10, minH: 6 },
    { i: "operations", x: 0, y: 20, w: 12, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 36, w: 12, h: 12, minH: 6 },
    { i: "resource-trends", x: 0, y: 48, w: 12, h: 14, minH: 8 },
  ],
  md: [
    { i: "overview", x: 0, y: 0, w: 10, h: 10, minH: 8 },
    { i: "insert-performance", x: 0, y: 10, w: 10, h: 10, minH: 6 },
    { i: "operations", x: 0, y: 20, w: 10, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 36, w: 10, h: 12, minH: 6 },
    { i: "resource-trends", x: 0, y: 48, w: 10, h: 14, minH: 8 },
  ],
  sm: [
    { i: "overview", x: 0, y: 0, w: 6, h: 10, minH: 8 },
    { i: "insert-performance", x: 0, y: 10, w: 6, h: 10, minH: 6 },
    { i: "operations", x: 0, y: 20, w: 6, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 36, w: 6, h: 12, minH: 6 },
    { i: "resource-trends", x: 0, y: 48, w: 6, h: 14, minH: 8 },
  ],
  xs: [
    { i: "overview", x: 0, y: 0, w: 4, h: 10, minH: 8 },
    { i: "insert-performance", x: 0, y: 10, w: 4, h: 10, minH: 6 },
    { i: "operations", x: 0, y: 20, w: 4, h: 16, minH: 10 },
    { i: "flame-graph", x: 0, y: 36, w: 4, h: 12, minH: 6 },
    { i: "resource-trends", x: 0, y: 48, w: 4, h: 14, minH: 8 },
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
      partialize: (state) => ({
        selectedSystem: state.selectedSystem,
        layouts: state.layouts,
        layoutModified: state.layoutModified,
      }),
    },
  ),
)
