import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  base: "/olap-benchmarks/",
  plugins: [react(), tailwindcss()],
  build: {
    target: "es2022",
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("@duckdb/duckdb-wasm")) return "duckdb"
          if (
            id.includes("kysely") ||
            id.includes("@coji/kysely-duckdb-wasm") ||
            id.includes("@20chan/kysely-duckdb")
          ) {
            return "query-builder"
          }
          if (id.includes("recharts")) return "charts"
          if (id.includes("@radix-ui") || id.includes("lucide-react")) {
            return "ui"
          }
          if (id.includes("/react/") || id.includes("/react-dom/") || id.includes("scheduler")) {
            return "react-vendor"
          }
          return undefined
        },
      },
    },
  },
})
