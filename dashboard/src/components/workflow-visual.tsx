import { useId, useState } from "react"
import {
  Activity,
  ArrowDown,
  Check,
  Clock,
  GitBranch,
  Layers,
  Minus,
  Plus,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { JsonView, Nothing, Status } from "@/components/console-ui"
import { date } from "@/lib/api"
import { cn } from "@/lib/utils"

export type Definition = {
  version: string
  nodes: {
    id: string
    label: string
    kind: string
    step?: string | null
    description?: string
  }[]
  edges: { from: string; to: string; label?: string }[]
}
export type Snapshot = {
  definition: Definition | null
  revision: string | null
}
export type WorkflowEvent = {
  eventId: number
  instanceId: string
  generation?: string
  timestamp: number
  type: string
  stepName?: string
  count?: number
  attempt?: number
  input?: unknown
  output?: unknown
  params?: unknown
  error?: unknown
  config?: unknown
  durationMs?: number
  eventType?: string
}
export type ExecutionHistory = {
  instanceId: string
  cursor: number
  events: WorkflowEvent[]
  definitions: Record<string, Snapshot>
  done: boolean
  clipped?: boolean
}

// Bound retained browser payloads as well as DOM work. Never mix instance histories.
export function mergeHistory(
  previous: ExecutionHistory | null,
  page: ExecutionHistory
): ExecutionHistory {
  const append = page.cursor > 0 && previous?.instanceId === page.instanceId
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
    bytes += JSON.stringify(events[index]).length
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

export function WorkflowDefinition({
  definition,
  revision,
  title = "Workflow definition",
  observations,
}: {
  definition?: Definition | null
  revision?: string | null
  title?: string
  observations?: WorkflowEvent[]
}) {
  const [selected, setSelected] = useState<string | null>(null)
  const [zoom, setZoom] = useState(1)
  const marker = useId().replaceAll(":", "")
  const nodes = definition?.nodes ?? []
  const edges = definition?.edges ?? []
  // Layer acyclic portions; preserve declaration order for cycles and disconnected nodes.
  const levels = new Map<string, number>()
  for (let pass = 0; pass < nodes.length; pass++) {
    let changed = false
    for (const node of nodes) {
      if (levels.has(node.id)) continue
      const parents = edges
        .filter((edge) => edge.to === node.id)
        .map((edge) => edge.from)
      if (parents.every((parent) => levels.has(parent))) {
        levels.set(
          node.id,
          parents.length
            ? Math.max(...parents.map((parent) => levels.get(parent)!)) + 1
            : 0
        )
        changed = true
      }
    }
    if (!changed) break
  }
  const unresolved = Math.max(0, ...levels.values()) + 1
  const rows = new Map<number, number>()
  const positions = new Map(
    nodes.map((node) => {
      const level = levels.get(node.id) ?? unresolved
      const row = rows.get(level) ?? 0
      rows.set(level, row + 1)
      return [node.id, { x: 24 + level * 280, y: 30 + row * 130 }] as const
    })
  )
  const width = Math.max(
    360,
    ...[...positions.values()].map((position) => position.x + 244)
  )
  const height = Math.max(
    180,
    ...[...positions.values()].map((position) => position.y + 110)
  )
  const active = nodes.find((node) => node.id === selected)
  const observed = (node: Definition["nodes"][number]) =>
    observations?.filter(
      (event) =>
        node.step &&
        event.stepName === node.step &&
        !event.type.startsWith("rollback")
    ) ?? []
  const activeObserved = active ? observed(active) : []
  const activeOccurrence = activeObserved.at(-1)?.count ?? 1
  const activeEvents = activeObserved.filter(
    (event) => (event.count ?? 1) === activeOccurrence
  )
  const activeValue = (field: keyof WorkflowEvent) =>
    [...activeEvents].reverse().find((event) => event[field] !== undefined)?.[
      field
    ]
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle>{title}</CardTitle>
          {definition && (
            <Badge
              variant="outline"
              className="max-w-full break-all whitespace-normal"
            >
              <GitBranch data-icon="inline-start" />
              {definition.version}
            </Badge>
          )}
        </div>
        <CardDescription>
          Declared structure for the compiled workflow. Branch labels describe
          possible paths; execution remains in Rust.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex min-w-0 flex-col gap-4">
        {!definition ? (
          <Nothing
            title={
              observations
                ? "No definition recorded for this execution"
                : "No definition published"
            }
            description={
              observations
                ? "This run has no preserved definition. Its recorded activity remains available; publishing a definition applies to future executions."
                : "Add a versioned definition to this workflow binding to preview its steps and branches before starting an instance. Existing history remains inspectable."
            }
          />
        ) : (
          <>
            <div className="flex items-center justify-between gap-3">
              <p className="text-xs text-muted-foreground">
                {nodes.length} nodes · {edges.length} connections
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="Zoom out"
                  disabled={zoom <= 0.5}
                  onClick={() => setZoom((value) => value - 0.25)}
                >
                  <Minus />
                </Button>
                <span className="text-xs tabular-nums">
                  {Math.round(zoom * 100)}%
                </span>
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="Zoom in"
                  disabled={zoom >= 1.5}
                  onClick={() => setZoom((value) => value + 0.25)}
                >
                  <Plus />
                </Button>
              </div>
            </div>
            <div
              className="workflow-canvas"
              role="region"
              aria-label={`${title} graph`}
              tabIndex={0}
            >
              <div style={{ width: width * zoom, height: height * zoom }}>
                <div
                  className="relative origin-top-left"
                  style={{ width, height, transform: `scale(${zoom})` }}
                >
                  <svg
                    width={width}
                    height={height}
                    className="absolute inset-0 text-muted-foreground"
                    aria-hidden="true"
                  >
                    <defs>
                      <marker
                        id={marker}
                        markerWidth="8"
                        markerHeight="8"
                        refX="7"
                        refY="4"
                        orient="auto"
                      >
                        <path d="M0 0 L8 4 L0 8" fill="currentColor" />
                      </marker>
                    </defs>
                    {edges.map((edge, index) => {
                      const from = positions.get(edge.from),
                        to = positions.get(edge.to)
                      if (!from || !to) return null
                      const x = from.x + 220,
                        y = from.y + 42,
                        targetY = to.y + 42
                      const bend = x + Math.max(28, (to.x - x) / 2)
                      const detour = to.x <= from.x || to.x - from.x > 280
                      const rail = Math.max(from.y, to.y) + 108
                      const path = detour
                        ? `M${x},${y} L${x + 16},${y} L${x + 16},${rail} L${to.x - 16},${rail} L${to.x - 16},${targetY} L${to.x - 4},${targetY}`
                        : `M${x},${y} C${bend},${y} ${to.x - 28},${targetY} ${to.x - 4},${targetY}`

                      return (
                        <g key={index}>
                          <path
                            d={path}
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="1.5"
                            markerEnd={`url(#${marker})`}
                          />
                          <text
                            x={(x + to.x) / 2}
                            y={detour ? rail - 5 : (y + targetY) / 2 - 8}
                            textAnchor="middle"
                            fill="currentColor"
                            fontSize="10"
                          >
                            {edge.label}
                          </text>
                        </g>
                      )
                    })}
                  </svg>
                  {nodes.map((node) => {
                    const position = positions.get(node.id)!
                    const Icon =
                      node.kind === "decision"
                        ? GitBranch
                        : node.kind === "parallel"
                          ? Layers
                          : ["wait", "sleep"].includes(node.kind)
                            ? Clock
                            : node.kind === "end"
                              ? Check
                              : Activity
                    return (
                      <button
                        key={node.id}
                        type="button"
                        className={cn(
                          "workflow-graph-node",
                          selected === node.id && "workflow-selected"
                        )}
                        style={{ left: position.x, top: position.y }}
                        aria-pressed={selected === node.id}
                        onClick={() => setSelected(node.id)}
                      >
                        <span className="flex items-center gap-2">
                          <Icon className="size-4 shrink-0" aria-hidden />
                          <span className="truncate">{node.label}</span>
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {observations && node.step ? (
                            (observed(node).at(-1)?.type.replaceAll("_", " ") ??
                            "Not observed in loaded history")
                          ) : (
                            <>
                              {node.kind}
                              {node.step ? ` · ${node.step}` : ""}
                            </>
                          )}
                        </span>
                      </button>
                    )
                  })}
                </div>
              </div>
            </div>
            {active ? (
              <div className="flex flex-col gap-2" aria-live="polite">
                <h4 className="text-sm font-semibold">{active.label}</h4>
                <p className="text-sm text-muted-foreground">
                  {active.description || "No description supplied."}
                </p>
                <p className="text-xs text-muted-foreground">
                  Step mapping: {active.step || "No durable step mapping"}
                </p>
                <p className="text-xs text-muted-foreground">
                  Outgoing paths:{" "}
                  {edges
                    .filter((edge) => edge.from === active.id)
                    .map(
                      (edge) =>
                        `${edge.label ? edge.label + " → " : ""}${nodes.find((node) => node.id === edge.to)?.label ?? edge.to}`
                    )
                    .join("; ") || "None declared"}
                </p>
                {observations && active.step && (
                  <p className="text-xs text-muted-foreground">
                    Latest observed occurrence:{" "}
                    {activeEvents.length ? activeOccurrence : "Not recorded"}
                  </p>
                )}
                {observations && active.step && (
                  <div className="grid min-w-0 gap-4 md:grid-cols-2">
                    <DataField
                      title="Recorded input"
                      value={activeValue("input")}
                    />
                    <DataField
                      title="Recorded output"
                      value={activeValue("output")}
                    />
                  </div>
                )}
              </div>
            ) : (
              <p className="text-xs text-muted-foreground">
                Select a node to inspect its description and durable step
                mapping. Scroll to explore larger definitions.
              </p>
            )}
            {revision && (
              <p className="font-mono text-xs break-all text-muted-foreground">
                Definition digest: {revision}
              </p>
            )}
          </>
        )}
      </CardContent>
    </Card>
  )
}

function tone(type: string) {
  return /errored|timed_out/.test(type)
    ? "error"
    : /completed/.test(type)
      ? "good"
      : /waiting|paused/.test(type)
        ? "warning"
        : "neutral"
}
function DataField({ title, value }: { title: string; value: unknown }) {
  return (
    <section className="flex min-w-0 flex-col gap-2">
      <h4 className="text-xs font-semibold tracking-wide text-muted-foreground uppercase">
        {title}
      </h4>
      {value === undefined ? (
        <p className="text-sm text-muted-foreground">Not recorded</p>
      ) : (
        <JsonView value={value} />
      )}
    </section>
  )
}

export function WorkflowExecution({
  history,
  pending,
  onNext,
}: {
  history: ExecutionHistory
  pending: boolean
  onNext: (cursor: number) => void
}) {
  const [run, setRun] = useState("")
  const [selected, setSelected] = useState<number | null>(null)
  const generations = [
    ...new Set(history.events.map((event) => event.generation ?? "legacy")),
  ]
  const generation = generations.includes(run) ? run : generations.at(-1)
  const events = history.events.filter(
    (event) => (event.generation ?? "legacy") === generation
  )
  const snapshot = generation ? history.definitions?.[generation] : undefined
  const groups = new Map<string, WorkflowEvent[]>()
  for (const event of events) {
    const key = event.stepName
      ? `${event.type.startsWith("rollback") ? "rollback" : "step"}:${event.stepName}:${event.count ?? 1}`
      : `event:${event.eventId}`
    groups.set(key, [...(groups.get(key) ?? []), event])
  }
  const selectedGroup =
    [...groups.values()].find((group) =>
      group.some((event) => event.eventId === selected)
    ) ?? [...groups.values()][0]
  const selectedEvent =
    selectedGroup?.find((event) => event.eventId === selected) ??
    selectedGroup?.at(-1)
  const latestWith = (field: keyof WorkflowEvent) =>
    [...(selectedGroup ?? [])]
      .reverse()
      .find((event) => event[field] !== undefined)?.[field]
  return (
    <section
      className="flex min-w-0 flex-col gap-4"
      aria-label="Workflow execution explorer"
    >
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h3 className="text-lg font-semibold">Execution explorer</h3>
          <p className="text-sm break-all text-muted-foreground">
            {history.instanceId} · {history.events.length} loaded events
          </p>
        </div>
        <label className="flex w-64 max-w-full flex-col gap-1 text-xs text-muted-foreground">
          Execution generation
          <select
            className="workflow-generation-select"
            aria-label="Execution generation"
            value={generation ?? ""}
            onChange={(event) => {
              setRun(event.target.value)
              setSelected(null)
            }}
          >
            {generations.map((value) => (
              <option key={value} value={value}>
                {value === "legacy"
                  ? "Legacy events · generation not recorded"
                  : `${history.definitions?.[value]?.definition?.version ?? "Unversioned"} · ${value}`}
              </option>
            ))}
          </select>
        </label>
      </div>
      <p className="text-xs text-muted-foreground">
        {history.done && history.cursor === 0 && !history.clipped
          ? "All retained events loaded"
          : "Partial history · load more events to extend this view"}
        {history.clipped
          ? " · older events omitted at the browser display limit"
          : ""}
        . Event order shows observations, not dependencies between parallel
        activities.
      </p>
      <Tabs defaultValue="path">
        <TabsList>
          <TabsTrigger value="path">Execution path</TabsTrigger>
          <TabsTrigger value="timeline">Activity timeline</TabsTrigger>
          <TabsTrigger value="definition">Run definition</TabsTrigger>
        </TabsList>
        <TabsContent value="path">
          <div className="workflow-execution-layout">
            <ol
              className="flex flex-col gap-2"
              aria-label="Recorded execution path"
            >
              {[...groups].map(([key, group], index) => {
                const last = group.at(-1)!
                const first = group[0]
                const duration = last.timestamp - first.timestamp
                return (
                  <li key={key}>
                    {index > 0 && (
                      <ArrowDown
                        className="mx-5 mb-2 size-4 text-muted-foreground"
                        aria-hidden
                      />
                    )}
                    <button
                      className={cn(
                        "workflow-path-node",
                        selectedGroup === group && "workflow-selected"
                      )}
                      type="button"
                      aria-pressed={selectedGroup === group}
                      onClick={() => setSelected(last.eventId)}
                    >
                      <span className="flex flex-wrap items-center justify-between gap-2">
                        <span className="min-w-0 font-medium break-all">
                          {last.stepName ?? last.type.replaceAll("_", " ")}
                          {last.stepName && (last.count ?? 1) > 1
                            ? ` · occurrence ${last.count}`
                            : ""}
                        </span>
                        <Status
                          label={last.type.replaceAll("_", " ")}
                          tone={tone(last.type)}
                        />
                      </span>
                      <span className="mt-2 block text-xs text-muted-foreground">
                        {group.length} {group.length === 1 ? "event" : "events"}
                        {last.stepName
                          ? ` · attempt ${Math.max(...group.map((event) => event.attempt ?? 1))}`
                          : ""}
                        {duration > 0
                          ? ` · observed ${(duration / 1000).toFixed(2)} s`
                          : ""}
                      </span>
                    </button>
                  </li>
                )
              })}
            </ol>
            <aside className="workflow-inspector" aria-label="Step inspector">
              {selectedEvent ? (
                <>
                  <div className="flex flex-col gap-1">
                    <h3 className="text-base font-semibold">
                      {selectedEvent.stepName ?? "Workflow event"}
                    </h3>
                    <p className="text-xs text-muted-foreground">
                      Event {selectedEvent.eventId} ·{" "}
                      {date(selectedEvent.timestamp)}
                    </p>
                  </div>
                  <DataField
                    title="Input"
                    value={
                      latestWith("input") !== undefined
                        ? latestWith("input")
                        : latestWith("params")
                    }
                  />
                  <DataField
                    title="Output / result"
                    value={latestWith("output")}
                  />
                  <DataField
                    title="Last recorded error"
                    value={latestWith("error")}
                  />
                  <DataField
                    title="Retry / timeout policy"
                    value={latestWith("config")}
                  />
                  <details>
                    <summary className="cursor-pointer text-sm">
                      Attempts and activity ({selectedGroup?.length})
                    </summary>
                    {selectedGroup?.map((event) => (
                      <div className="mt-3" key={event.eventId}>
                        <p className="text-xs">
                          #{event.eventId} · {event.type.replaceAll("_", " ")} ·{" "}
                          {date(event.timestamp)}
                        </p>
                        <JsonView value={event} />
                      </div>
                    ))}
                  </details>
                </>
              ) : (
                <Nothing
                  title="No events on this page"
                  description="Load another history page or refresh the running instance."
                />
              )}
            </aside>
          </div>
        </TabsContent>
        <TabsContent value="timeline">
          <ol className="flex flex-col gap-3">
            {events.map((event) => (
              <li key={event.eventId} className="rounded-lg border p-4">
                <div className="flex flex-wrap justify-between gap-2">
                  <Status
                    label={event.type.replaceAll("_", " ")}
                    tone={tone(event.type)}
                  />
                  <time className="text-xs text-muted-foreground">
                    {date(event.timestamp)}
                  </time>
                </div>
                <p className="my-2 text-sm">
                  #{event.eventId}
                  {event.stepName
                    ? ` · ${event.stepName} · occurrence ${event.count ?? 1} · attempt ${event.attempt ?? 1}`
                    : ""}
                </p>
                <details>
                  <summary className="cursor-pointer text-xs text-primary">
                    Event data
                  </summary>
                  <JsonView value={event} />
                </details>
              </li>
            ))}
          </ol>
        </TabsContent>
        <TabsContent value="definition">
          <WorkflowDefinition
            key={generation}
            definition={snapshot?.definition}
            revision={snapshot?.revision}
            title="Recorded run definition"
            observations={events}
          />
          <p className="mt-3 text-xs text-muted-foreground">
            Snapshot captured when this execution was created. This is
            definition versioning; the executable is not pinned by this
            snapshot. Older runs may have no recorded definition.
          </p>
        </TabsContent>
      </Tabs>
      {!history.done && history.events.at(-1) && (
        <Button
          className="w-fit"
          variant="outline"
          disabled={pending}
          onClick={() => onNext(history.events.at(-1)!.eventId)}
        >
          Next history page
        </Button>
      )}
    </section>
  )
}
