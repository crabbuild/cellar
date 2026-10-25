// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { StrictMode } from "react"
import { createRoot } from "react-dom/client"

import "./index.css"
import App from "./App.tsx"
import { ThemeProvider } from "@/components/theme-provider.tsx"

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider defaultTheme="system" storageKey="cellar-theme-v1">
      <App />
    </ThemeProvider>
  </StrictMode>
)
