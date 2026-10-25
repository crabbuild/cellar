// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useSyncExternalStore } from "react"

const query = "(max-width: 767px)"
function subscribe(notify: () => void) {
  const media = window.matchMedia(query)
  media.addEventListener("change", notify)
  return () => media.removeEventListener("change", notify)
}
export function useIsMobile() {
  return useSyncExternalStore(
    subscribe,
    () => window.matchMedia(query).matches,
    () => false
  )
}
