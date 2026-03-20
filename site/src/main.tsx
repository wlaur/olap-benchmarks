import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { HashRouter } from "react-router-dom"

import { App } from "./App"

import "./index.css"

const faviconHref = `${import.meta.env.BASE_URL}favicon.ico`
const faviconLink = document.head.querySelector('link[rel="icon"]') as HTMLLinkElement | null

if (faviconLink) {
  faviconLink.type = "image/x-icon"
  faviconLink.href = faviconHref
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HashRouter>
      <App />
    </HashRouter>
  </StrictMode>,
)
