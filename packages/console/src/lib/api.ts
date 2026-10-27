// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useQuery } from "@tanstack/react-query"
import { useSession } from "@/lib/session"

export type Permission =
  "metadata_read" | "data_read" | "data_write" | "operate" | "configure" |
  "fleet_topology" | "fleet_cancel" | "fleet_register" | "fleet_capacity" |
  "fleet_evacuate" | "fleet_remove" | "fleet_stop"
export type Fleet = { id: string; name: string; environment: string }
export type Session = {
  user: string
  fleets: Fleet[]
  grants: Record<string, Permission[]>
}
export type Page<T> = {
  items: T[]
  next_cursor: string | null
  observed_at: number
  coverage: string
  warnings: string[]
}
export type Node = {
  id: string
  generation: string
  address: string
  lease_current: boolean
  lease_expires_at: number
  protocol: number
  load: {
    sampled_ms: number
    owned_cells: number | null
    resident_cells: number
    rss_bytes: number | null
    in_use_bytes: number | null
    cpu_percent_x100: number
    pressured: boolean
    restoring: number
    rebalance_paused: boolean | null
    draining: boolean | null
  }
}
export type Cell = {
  scope: string
  class: string | null
  kind: string
  owner: string | null
  epoch: number | null
  observed_at: number
  error: string | null
}
import type { Definition } from "@/components/workflow-visual"

export type Binding = {
  name: string
  kind: string
  identity: string
  scope: string | null
  schedules: string[]
  event?: import("@/lib/events").EventBinding | null
  definition?: Definition | null
}
export type Application = {
  name: string
  version: string
  revision: string
  primary: boolean
  classes: string[]
  bindings: Binding[]
  crons: string[]
}
export type Operation = {
  id: string
  fleet: string
  actor: string
  target: string
  action: string
  status: string
  started_at: number
  finished_at: number | null
}

export class ApiError extends Error {
  readonly status: number
  constructor(message: string, status: number) {
    super(message)
    this.status = status
  }
}

export async function request<T>(
  token: string,
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const response = await fetch(`/api/v1${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
      Authorization: `Bearer ${token}`,
    },
  })
  const data = await response.json().catch(() => {
    throw new ApiError(
      "The console returned an unreadable response.",
      response.status
    )
  })
  if (!response.ok || data.error)
    throw new ApiError(
      data.error ?? `Request failed (${response.status}).`,
      response.status
    )
  return data as T
}

export function useApi<T>(path: string, enabled = true) {
  const { token } = useSession()
  return useQuery({
    queryKey: [path],
    queryFn: ({ signal }) => request<T>(token, path, { signal }),
    enabled,
    staleTime: 15_000,
    retry: false,
  })
}

export function fleetPath(fleet: string, path: string) {
  return `/fleets/${encodeURIComponent(fleet)}${path}`
}
export function bytes(value: number | null | undefined) {
  if (value == null) return "Not reported"
  if (value < 1024) return `${value} B`
  const i = Math.min(Math.floor(Math.log(value) / Math.log(1024)), 4)
  return `${(value / 1024 ** i).toFixed(1)} ${["B", "KiB", "MiB", "GiB", "TiB"][i]}`
}
export function date(value: number | null | undefined) {
  return value ? new Date(value).toLocaleString() : "Not reported"
}
