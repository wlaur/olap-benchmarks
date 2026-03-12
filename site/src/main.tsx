import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { HashRouter } from "react-router-dom"

import { App } from "./App"
import { SystemProvider } from "./features/system/SystemContext"

import "./index.css"

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HashRouter>
      <SystemProvider>
        <App />
      </SystemProvider>
    </HashRouter>
  </StrictMode>,
)
