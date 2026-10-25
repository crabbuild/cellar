// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type {
  Definition,
  WorkflowEvent,
  ExecutionHistory,
} from "@/components/workflow-visual"

export type StepInterval = {
  ms: number | null
  complete: boolean
  label: string
}
export function workflowGroupKey(event: WorkflowEvent) {
  return event.stepName
    ? `${event.type.startsWith("rollback") ? "rollback" : "step"}:${event.stepName}:${event.count ?? 1}`
    : `event:${event.eventId}`
}
export function stepInterval(events: WorkflowEvent[]): StepInterval {
  const started = events.find((event) =>
    [
      "step_started",
      "sleep_started",
      "wait_started",
      "rollback_step_started",
      "workflow_started",
    ].includes(event.type)
  )
  const ended = [...events]
    .reverse()
    .find((event) =>
      [
        "step_completed",
        "step_errored",
        "sleep_completed",
        "wait_completed",
        "wait_timed_out",
        "rollback_step_completed",
        "rollback_step_errored",
        "workflow_completed",
        "workflow_errored",
        "workflow_terminated",
      ].includes(event.type)
    )
  if (started && ended && ended.timestamp >= started.timestamp)
    return {
      ms: ended.timestamp - started.timestamp,
      complete: true,
      label: "Recorded duration",
    }
  if (events.length > 1 && events.at(-1)!.timestamp >= events[0].timestamp)
    return {
      ms: events.at(-1)!.timestamp - events[0].timestamp,
      complete: false,
      label: "Observed interval (partial)",
    }
  return { ms: null, complete: false, label: "Duration not recorded" }
}
export function latestOccurrence(events: WorkflowEvent[], step: string) {
  const matching = events.filter(
    (event) => event.stepName === step && !event.type.startsWith("rollback")
  )
  const count = matching.at(-1)?.count ?? 1
  return matching.filter((event) => (event.count ?? 1) === count)
}
export function definitionDiff(
  left: Definition | null | undefined,
  right: Definition | null | undefined
) {
  if (!left || !right) return null
  const a = new Map(left.nodes.map((node) => [node.id, node])),
    b = new Map(right.nodes.map((node) => [node.id, node]))
  const edges = (definition: Definition) =>
    new Set(
      definition.edges.map((edge) =>
        JSON.stringify([edge.from, edge.to, edge.label ?? ""])
      )
    )
  const signature = (node: Definition["nodes"][number] | undefined) =>
    JSON.stringify([
      node?.label,
      node?.kind,
      node?.step ?? null,
      node?.description ?? null,
    ])
  const ae = edges(left),
    be = edges(right)
  return {
    added: [...b.keys()].filter((id) => !a.has(id)),
    removed: [...a.keys()].filter((id) => !b.has(id)),
    changed: [...a.keys()].filter(
      (id) => b.has(id) && signature(a.get(id)) !== signature(b.get(id))
    ),
    addedEdges: [...be].filter((edge) => !ae.has(edge)).length,
    removedEdges: [...ae].filter((edge) => !be.has(edge)).length,
  }
}

// Bound retained browser payloads as well as DOM work. Never mix instance histories.
export function mergeHistory(
  previous: ExecutionHistory | null,
  page: ExecutionHistory
): ExecutionHistory {
  const append =
    page.cursor > 0 &&
    previous?.instanceId === page.instanceId &&
    previous.scope === page.scope
  const events = [
    ...new Map(
      [...(append ? previous.events : []), ...page.events].map((event) => [
        event.eventId,
        event,
      ])
    ).values(),
  ].sort((a, b) => a.eventId - b.eventId)
  let bytes = 0
  let first = events.length
  for (
    let index = events.length - 1;
    index >= 0 && events.length - index <= 256;
    index--
  ) {
    bytes += new TextEncoder().encode(JSON.stringify(events[index])).byteLength
    if (bytes > 16 * 1024 * 1024 && first < events.length) break
    first = index
  }
  const retained = events.slice(first)
  const definitions = {
    ...(append ? previous.definitions : {}),
    ...page.definitions,
  }
  const generations = new Set(retained.map((event) => event.generation))
  return {
    ...page,
    cursor: append ? previous.cursor : page.cursor,
    events: retained,
    definitions: Object.fromEntries(
      Object.entries(definitions).filter(([generation]) =>
        generations.has(generation)
      )
    ),
    clipped: first > 0 || (append && previous.clipped),
  }
}
