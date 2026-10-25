// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState, type ReactNode } from "react"
import { useMutation } from "@tanstack/react-query"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { Badge } from "@/components/ui/badge"
import { Feedback } from "@/components/console-ui"
import { resourceRequest, type ResourceTarget } from "@/lib/resource"
import { useSession } from "@/lib/session"
import { definitionDiff } from "@/lib/workflow-inspection"
import type {
  ExecutionHistory,
  Snapshot,
  WorkflowEvent,
} from "@/components/workflow-visual"
import { mergeHistory } from "@/lib/workflow-inspection"

type RenderRun = (
  history: ExecutionHistory,
  onNext: (cursor: number) => void,
  pending: boolean
) => ReactNode
export function WorkflowComparison({
  history,
  generation,
  target,
  renderRun,
  renderDefinition,
}: {
  history: ExecutionHistory
  generation?: string
  target?: ResourceTarget
  renderRun: RenderRun
  renderDefinition: (
    snapshot: Snapshot | undefined,
    events: WorkflowEvent[]
  ) => ReactNode
}) {
  const { token } = useSession()
  const [instance, setInstance] = useState(history.instanceId)
  const [loaded, setLoaded] = useState<ExecutionHistory | null>(null)
  const [leftRun, setLeftRun] = useState(generation ?? "")
  const [rightRun, setRightRun] = useState("")
  const fetch = useMutation({
    mutationFn: async ({
      instance,
      cursor,
    }: {
      instance: string
      cursor: number
    }) => {
      if (!target) throw new Error("This workspace has no comparison route.")
      const result = (
        await resourceRequest(token, target, {
          action: "workflow",
          instance,
          command: { operation: "history", cursor },
        })
      ).result as ExecutionHistory
      if (!Array.isArray(result?.events) || result.instanceId !== instance)
        throw new Error("The owner returned another or unreadable execution.")
      return result
    },
    onSuccess: (page) => setLoaded((previous) => mergeHistory(previous, page)),
    retry: false,
  })
  const right = loaded ?? history
  const generations = (value: ExecutionHistory) => [
    ...new Set(value.events.map((event) => event.generation ?? "legacy")),
  ]
  const leftGenerations = generations(history),
    rightGenerations = generations(right)
  const left = leftGenerations.includes(leftRun)
    ? leftRun
    : leftGenerations.at(-1)
  const preferred =
    rightGenerations.find((value) => value !== left) ?? rightGenerations.at(-1)
  const selectedRight = rightGenerations.includes(rightRun)
    ? rightRun
    : preferred
  const slice = (
    value: ExecutionHistory,
    generation: string | undefined
  ): ExecutionHistory => ({
    ...value,
    events: value.events.filter(
      (event) => (event.generation ?? "legacy") === generation
    ),
  })
  const leftEvents = slice(history, left),
    rightEvents = slice(right, selectedRight)
  const leftSnapshot = left ? history.definitions?.[left] : undefined,
    rightSnapshot = selectedRight
      ? right.definitions?.[selectedRight]
      : undefined
  const diff = definitionDiff(
    leftSnapshot?.definition,
    rightSnapshot?.definition
  )
  const selector = (
    label: string,
    value: string | undefined,
    values: string[],
    source: ExecutionHistory,
    onChange: (value: string) => void
  ) => (
    <Field>
      <FieldLabel>{label}</FieldLabel>
      <select
        className="workflow-generation-select"
        aria-label={label}
        value={value ?? ""}
        onChange={(event) => onChange(event.target.value)}
      >
        {values.map((generation) => (
          <option key={generation} value={generation}>
            {source.definitions?.[generation]?.definition?.version ??
              "Unversioned"}{" "}
            · {generation}
          </option>
        ))}
      </select>
    </Field>
  )
  return (
    <section
      className="flex min-w-0 flex-col gap-4"
      aria-label="Workflow comparison"
    >
      <p className="workspace-caption">
        Compare retained execution observations and their definition snapshots.
        Missing events and definitions remain explicit; definition versions do
        not pin executable versions.
      </p>
      {target ? (
        <form
          onSubmit={(event) => {
            event.preventDefault()
            setRightRun("")
            fetch.mutate({ instance: instance.trim(), cursor: 0 })
          }}
        >
          <FieldGroup className="inspection-filter-row">
            <Field>
              <FieldLabel htmlFor="compare-instance">
                Comparison instance
              </FieldLabel>
              <Input
                id="compare-instance"
                value={instance}
                maxLength={256}
                onChange={(event) => setInstance(event.target.value)}
              />
            </Field>
            <Button
              variant="outline"
              disabled={!instance.trim() || fetch.isPending}
            >
              Load comparison run
            </Button>
          </FieldGroup>
        </form>
      ) : null}
      <Feedback error={fetch.error} loading={fetch.isPending && !loaded}>
        <div className="workflow-comparison-grid">
          <section>
            <h4>Left · {history.instanceId}</h4>
            {selector(
              "Left execution generation",
              left,
              leftGenerations,
              history,
              setLeftRun
            )}
            <p className="workspace-caption">
              {leftEvents.events.length} loaded events ·{" "}
              {history.done && !history.clipped && history.cursor === 0
                ? "Complete retained history"
                : "Partial history"}
            </p>
          </section>
          <section>
            <h4>Right · {right.instanceId}</h4>
            {selector(
              "Right execution generation",
              selectedRight,
              rightGenerations,
              right,
              setRightRun
            )}
            <p className="workspace-caption">
              {rightEvents.events.length} loaded events ·{" "}
              {right.done && !right.clipped && right.cursor === 0
                ? "Complete retained history"
                : "Partial history"}
            </p>
          </section>
        </div>
        {loaded && !loaded.done && loaded.events.at(-1) ? (
          <Button
            variant="outline"
            className="w-fit"
            disabled={fetch.isPending}
            onClick={() =>
              fetch.mutate({
                instance: loaded.instanceId,
                cursor: loaded.events.at(-1)!.eventId,
              })
            }
          >
            More comparison history
          </Button>
        ) : null}
        <Tabs defaultValue="executions">
          <TabsList>
            <TabsTrigger value="executions">Run comparison</TabsTrigger>
            <TabsTrigger value="definitions">Definition comparison</TabsTrigger>
          </TabsList>
          <TabsContent value="executions">
            <div className="workflow-comparison-grid">
              <div aria-label="Left run comparison">
                {renderRun(
                  { ...leftEvents, clipped: history.clipped },
                  () => {},
                  false
                )}
              </div>
              <div aria-label="Right run comparison">
                {renderRun(
                  { ...rightEvents, clipped: right.clipped },
                  () => {},
                  fetch.isPending
                )}
              </div>
            </div>
          </TabsContent>
          <TabsContent value="definitions">
            {diff ? (
              <div
                className="flex flex-wrap gap-2"
                aria-label="Definition changes"
              >
                <Badge variant="outline">{diff.added.length} added nodes</Badge>
                <Badge variant="outline">
                  {diff.removed.length} removed nodes
                </Badge>
                <Badge variant="outline">
                  {diff.changed.length} changed nodes
                </Badge>
                <Badge variant="outline">{diff.addedEdges} added paths</Badge>
                <Badge variant="outline">
                  {diff.removedEdges} removed paths
                </Badge>
                <p className="workspace-caption">
                  Added: {diff.added.join(", ") || "None"} · Removed:{" "}
                  {diff.removed.join(", ") || "None"} · Changed:{" "}
                  {diff.changed.join(", ") || "None"}
                </p>
              </div>
            ) : (
              <p className="workspace-caption">
                Both recorded definitions are needed to compute changes.
              </p>
            )}
            <div className="workflow-comparison-grid">
              <div aria-label="Left definition comparison">
                {renderDefinition(leftSnapshot, leftEvents.events)}
              </div>
              <div aria-label="Right definition comparison">
                {renderDefinition(rightSnapshot, rightEvents.events)}
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </Feedback>
    </section>
  )
}
