// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { defineConfig } from "@playwright/test"
export default defineConfig({
  testDir: "./tests",
  use: { baseURL: "http://127.0.0.1:5173", browserName: "chromium" },
  webServer: {
    command: "npm run dev -- --port 5173 --strictPort",
    url: "http://127.0.0.1:5173",
    reuseExistingServer: !process.env.CI,
  },
})
