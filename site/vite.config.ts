import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { createLogger, defineConfig } from "vite"

const logger = createLogger()
const warnOnce = logger.warnOnce.bind(logger)
logger.warnOnce = (message, options) => {
  if (
    message.includes("@duckdb/duckdb-wasm") &&
    message.includes("points to a source file outside its package")
  ) {
    return
  }
  warnOnce(message, options)
}

export default defineConfig({
  base: "/olap-benchmarks/",
  customLogger: logger,
  resolve: {
    dedupe: ["@codemirror/lang-sql"],
  },
  plugins: [react(), tailwindcss()],
  build: {
    target: "es2022",
    chunkSizeWarningLimit: 2000,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules")) return "vendor"
          return undefined
        },
      },
    },
  },
})
