import { useQuery } from "@tanstack/react-query"
import { request, fleetPath, type Operation } from "@/lib/api"
import { useSession } from "@/lib/session"

export type Resource = {
  tenant: string
  application: string
  id?: string | null
  name: string
  family: string
}
export type EventBinding = {
  resource: Resource
  publish: boolean
  read: boolean
  administer: boolean
  subscriptions: string[]
}
export type Revision = {
  source: string
  incarnation: string
  lane: string
  dispatch_generation: string
  next_sequence: string
  settled_through: string
}
export type Lane = {
  revision: Revision
  resource: Resource
  status: string
  pending_rows: string
  pending_bytes: string
  retry_at_ms: string
  attempts: string
  last_error: string | null
  seal_state: "pending" | "blocked" | "accepted" | null
  blocked_reason: string | null
  oldest_pending_age_ms: string | null
}
export type Outbox = {
  incarnation: string | null
  pending_rows: string
  pending_bytes: string
  lanes: Lane[]
  next: string | null
  observation: {
    observed_ms: string
    oldest_pending_age_ms: string | null
    blocked_lanes: { reason: string; lanes: string }[]
  }
}
export type Identity = {
  resource: string
  name: string
  id: string
  generation: string
}
export type Subscription = {
  identity: Identity
  state: string
  required: boolean
  checkpoint: string
  earliest: string
  end: string
  retry_at_ms: string
  lease: null | {
    generation: string
    first: string
    last: string
    expires_ms: string
  }
  observation: {
    observed_ms: string
    lag_records: string
    retained_lag_records: string
    oldest_retained_age_ms: string | null
    history_expired: boolean
    lease_active: boolean
  }
}
export type Stats = {
  resource: Resource
  revision: string
  earliest: string
  end: string
  rows: string
  bytes: string
  observation: {
    observed_ms: string
    retention_frontier: string
    protected_records: string
    protected_bytes: string
    retention_blocked_bytes: string
    active_leases: string
    expired_subscriptions: string
    oldest_retained_age_ms: string | null
    sqlite_allocated_bytes: string
    sqlite_used_bytes: string
  }
}
export type StreamAction =
  | { kind: "describe" }
  | {
      kind: "read"
      cursor: string | null
      limit: number
      max_bytes: number
      payload: boolean
    }
  | { kind: "subscriptions" | "sources"; cursor: string | null; limit: number }
  | { kind: "audit"; after: string; limit: number }
  | {
      kind: "change"
      expected: Identity
      action: SubscriptionChange
      reason: string
    }
  | { kind: "status"; operation: string }
export type SubscriptionChange =
  | { kind: "pause" | "resume" }
  | { kind: "reset"; cursor: { resource: string; partition: 0; next: string } }
  | { kind: "delete"; force: boolean }
export type OutboxAction =
  | { kind: "inspect"; limit: number; cursor: string | null }
  | { kind: "audit"; after: string | null; limit: number }
  | { kind: "status"; operation: string }
  | { kind: "retry"; expected: Revision; reason: string }
export type EventReply<T = unknown> = {
  operation_id?: string
  result?: T
  operation?: Operation | null
  failure?: string
  replayed?: boolean
}
export type AuditPage = {
  records: { sequence: string; receipt: unknown }[]
  next?: string | null
  next_after?: string | null
  has_more?: boolean
}
export const newOperation = () => crypto.randomUUID().replaceAll("-", "")
export const cellEventsPath = (fleet: string, scope: string) =>
  fleetPath(fleet, `/cells/${encodeURIComponent(scope)}`)
export const streamPath = (
  fleet: string,
  application: string,
  binding: string
) =>
  fleetPath(
    fleet,
    `/streams/${encodeURIComponent(application)}/${encodeURIComponent(binding)}`
  )

export async function eventRequest<T>(
  token: string,
  path: string,
  action: OutboxAction | StreamAction,
  resource?: string,
  operation = newOperation(),
  signal?: AbortSignal
) {
  try {
    const reply = await request<EventReply<T>>(token, path, {
      method: "POST",
      body: JSON.stringify({
        operation_id: operation,
        ...(resource ? { resource_id: resource } : {}),
        action,
      }),
      signal,
    })
    return { ...reply, operation_id: operation }
  } catch (error) {
    if (action.kind !== "retry" && action.kind !== "change") throw error
    // Keep the client-chosen ID after a lost HTTP response. Never resubmit a write.
    return {
      operation_id: operation,
      failure: error instanceof Error ? error.message : "Write response lost",
    } as EventReply<T>
  }
}

export function useEvent<T>(
  path: string,
  action: OutboxAction | StreamAction,
  resource?: string,
  enabled = true
) {
  const { token } = useSession()
  return useQuery({
    queryKey: [path, resource, action],
    queryFn: ({ signal }) =>
      eventRequest<T>(token, path, action, resource, undefined, signal).then(
        (reply) => {
          if (reply.result === undefined)
            throw new Error(reply.failure ?? "No event observation returned.")
          return reply.result
        }
      ),
    enabled,
    staleTime: 15_000,
    retry: false,
  })
}

// Durable counters and offsets stay decimal strings all the way to display.
export function exact(value: string | null | undefined) {
  if (value == null) return "—"
  try {
    return BigInt(value).toLocaleString()
  } catch {
    return value
  }
}
export function eventTime(value: string | null | undefined) {
  if (!value || !/^\d+$/.test(value)) return "Not observed"
  const ms = BigInt(value)
  if (ms > 8_640_000_000_000_000n) return exact(value) + " ms"
  return new Date(Number(ms)).toLocaleString()
}
export function age(value: string | null | undefined) {
  if (value == null) return "—"
  const ms = BigInt(value)
  if (ms < 1000n) return `${exact(value)} ms`
  if (ms < 60_000n) return `${exact((ms / 1000n).toString())} s`
  if (ms < 3_600_000n) return `${exact((ms / 60_000n).toString())} min`
  return `${exact((ms / 3_600_000n).toString())} h`
}
