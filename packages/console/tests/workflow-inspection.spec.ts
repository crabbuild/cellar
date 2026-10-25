// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { test, expect } from "@playwright/test"
import {
  definitionDiff,
  latestOccurrence,
  mergeHistory,
  stepInterval,
} from "../src/lib/workflow-inspection"
import type {
  ExecutionHistory,
  WorkflowEvent,
} from "../src/components/workflow-visual"
const event = (eventId: number, type: string, fields = {}): WorkflowEvent => ({
  eventId,
  type,
  timestamp: eventId * 100,
  instanceId: "one",
  generation: "run",
  ...fields,
})
test("workflow timing uses lifecycle evidence and separates repeated occurrences", () => {
  const events = [
    event(1, "step_started", { stepName: "charge", count: 1 }),
    event(2, "step_completed", { stepName: "charge", count: 1 }),
    event(3, "sleep_started", {
      stepName: "charge",
      count: 2,
      durationMs: 60000,
    }),
  ]
  expect(stepInterval(events.slice(0, 2))).toEqual({
    ms: 100,
    complete: true,
    label: "Recorded duration",
  })
  expect(stepInterval(latestOccurrence(events, "charge"))).toEqual({
    ms: null,
    complete: false,
    label: "Duration not recorded",
  })
  expect(
    stepInterval([event(4, "step_attempt_errored"), event(5, "step_completed")])
  ).toEqual({ ms: 100, complete: false, label: "Observed interval (partial)" })
  expect(
    stepInterval([
      event(9, "step_started"),
      event(10, "step_completed", { timestamp: 0 }),
    ]).complete
  ).toBe(false)
})
test("definition comparisons ignore serialization order and equivalent optional fields", () => {
  const a = {
    version: "v1",
    nodes: [{ id: "s", kind: "activity", label: "Step" }],
    edges: [],
  }
  const b = {
    version: "v2",
    nodes: [{ label: "Step", id: "s", kind: "activity", step: null }],
    edges: [],
  }
  expect(definitionDiff(a, b)?.changed).toEqual([])
  expect(
    definitionDiff(a, { ...b, nodes: [{ ...b.nodes[0], label: "Changed" }] })
      ?.changed
  ).toEqual(["s"])
  expect(definitionDiff(a, null)).toBeNull()
})
test("workflow browser history respects source identity and UTF-8 display budgets", () => {
  const page = (
    events: WorkflowEvent[],
    cursor = 0,
    scope = "old"
  ): ExecutionHistory => ({
    events,
    cursor,
    scope,
    instanceId: "one",
    done: false,
    definitions: {},
  })
  const first = mergeHistory(null, page([event(1, "step_started")]))
  expect(
    mergeHistory(
      first,
      page([event(2, "step_completed")], 1, "new")
    ).events.map((e) => e.eventId)
  ).toEqual([2])
  expect(
    mergeHistory(first, {
      ...page([event(2, "step_completed")], 1),
      instanceId: "two",
    }).events.map((e) => e.eventId)
  ).toEqual([2])
  const payload = "界".repeat(300000)
  const all = Array.from({ length: 20 }, (_, index) =>
    event(index + 1, "step_completed", { output: payload })
  )
  const bounded = mergeHistory(
    mergeHistory(null, page(all.slice(0, 10))),
    page(all.slice(10), 10)
  )
  expect(bounded.clipped).toBe(true)
  expect(bounded.events.at(-1)?.eventId).toBe(20)
  expect(
    new TextEncoder().encode(JSON.stringify(bounded.events)).byteLength
  ).toBeLessThan(16 * 1024 * 1024)
})
