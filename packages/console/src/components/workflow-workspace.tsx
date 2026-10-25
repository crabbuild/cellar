// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useSearchParams } from "react-router-dom"
import { useState } from "react"
import {
  WorkflowExecution,
  type ExecutionHistory,
} from "@/components/workflow-visual"
import {
  List,
  Play,
  Search,
  History,
  Pause,
  RotateCcw,
  Send,
  Square,
} from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { date } from "@/lib/api"
import { JsonView, Status } from "@/components/console-ui"

type Command = Record<string, unknown>
type Instance = {
  instanceId: string
  status: string
  createdMs: number
  generation: string
}
export function WorkflowWorkspace({
  canRead,
  canWrite,
  pending,
  result,
  execute,
  confirm,
}: {
  canRead: boolean
  canWrite: boolean
  pending: boolean
  result: unknown
  execute: (command: Command) => void
  confirm: (title: string, description: string, command: Command) => void
}) {
  const [search] = useSearchParams()
  const [instance, setInstance] = useState(() => search.get("instance") ?? "")
  const [params, setParams] = useState("{}")
  const [eventType, setEventType] = useState("")
  const [cursor, setCursor] = useState("0")
  const reply =
    result && typeof result === "object"
      ? (result as Record<string, unknown>)
      : undefined
  const instances = Array.isArray(reply?.items)
    ? (reply.items as Instance[])
    : undefined
  const generation =
    reply?.instanceId === instance && typeof reply.generation === "string"
      ? reply.generation
      : null
  const command = (operation: string, fields: Command = {}) => ({
    action: "workflow",
    instance,
    ...(!["status", "history", "start"].includes(operation)
      ? { expected_generation: generation }
      : {}),
    command: { operation, ...fields },
  })
  const json = () => {
    try {
      return JSON.parse(params)
    } catch {
      toast.error("Enter valid JSON in the payload field.")
      return undefined
    }
  }
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Instances</CardTitle>
          <CardDescription>
            Inspect retained instances from the owning cells. A page scans up to
            20 application workflow cells.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Button
            variant="outline"
            disabled={!canRead || pending}
            onClick={() => execute({ action: "workflow_list", cursor: null })}
          >
            <List data-icon="inline-start" />
            Load instances
          </Button>
          {instances ? (
            <>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Instance</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead>Execution</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {instances.map((item) => (
                    <TableRow key={item.instanceId}>
                      <TableCell>
                        <Button
                          variant="link"
                          onClick={() => {
                            setInstance(item.instanceId)
                            execute({
                              action: "workflow",
                              instance: item.instanceId,
                              command: { operation: "status" },
                            })
                          }}
                        >
                          {item.instanceId}
                        </Button>
                      </TableCell>
                      <TableCell>
                        <Status label={item.status} />
                      </TableCell>
                      <TableCell>{date(item.createdMs)}</TableCell>
                      <TableCell>
                        <Button
                          variant="link"
                          disabled={pending || !canRead}
                          aria-label={`View execution of ${item.instanceId}`}
                          onClick={() => {
                            setInstance(item.instanceId)
                            setCursor("0")
                            execute({
                              action: "workflow",
                              instance: item.instanceId,
                              command: { operation: "history", cursor: 0 },
                            })
                          }}
                        >
                          <History data-icon="inline-start" />
                          View execution
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <p className="text-xs text-muted-foreground">
                {instances.length} matching instances ·{" "}
                {String(reply?.coverage ?? "partial")} coverage
              </p>
              {Array.isArray(reply?.warnings) && reply.warnings.length > 0 ? (
                <JsonView value={reply.warnings} />
              ) : null}
              {typeof reply?.next_cursor === "string" ? (
                <Button
                  variant="outline"
                  disabled={pending}
                  onClick={() =>
                    execute({
                      action: "workflow_list",
                      cursor: reply.next_cursor,
                    })
                  }
                >
                  Next page
                </Button>
              ) : null}
            </>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Instance controls</CardTitle>
          <CardDescription>
            Use an exact instance ID. Pause takes effect at a workflow
            checkpoint; inspect its status after the request.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="instance-id">Instance ID</FieldLabel>
              <Input
                id="instance-id"
                value={instance}
                onChange={(e) => setInstance(e.target.value)}
                placeholder="order-1042"
                maxLength={256}
              />
            </Field>
            <p className="text-xs text-muted-foreground">
              {generation
                ? `Observed execution: ${generation}`
                : "Inspect the instance before changing its execution."}
            </p>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                disabled={pending || !canRead || !instance}
                onClick={() => execute(command("status"))}
              >
                <Search data-icon="inline-start" />
                Inspect instance
              </Button>
              {[
                { operation: "pause", icon: Pause, label: "Pause" },
                { operation: "resume", icon: Play, label: "Resume" },
                { operation: "restart", icon: RotateCcw, label: "Restart" },
                { operation: "terminate", icon: Square, label: "Terminate" },
              ].map(({ operation, icon: Icon, label }) => (
                <Button
                  key={operation}
                  variant={
                    operation === "terminate" ? "destructive" : "outline"
                  }
                  disabled={pending || !canWrite || !instance || !generation}
                  onClick={() =>
                    confirm(
                      `${label} instance`,
                      `${label} workflow instance ${instance}. ${operation === "restart" ? "A new execution generation will begin." : operation === "terminate" ? "Running work will stop without compensation." : ""}`,
                      command(operation)
                    )
                  }
                >
                  <Icon data-icon="inline-start" />
                  {label}
                </Button>
              ))}
            </div>
            <Field>
              <FieldLabel htmlFor="history-cursor">
                History after event ID
              </FieldLabel>
              <Input
                id="history-cursor"
                inputMode="numeric"
                value={cursor}
                onChange={(e) => setCursor(e.target.value)}
              />
            </Field>
            <Button
              className="w-fit"
              variant="outline"
              disabled={
                pending || !canRead || !instance || !/^\d+$/.test(cursor)
              }
              onClick={() =>
                execute(command("history", { cursor: Number(cursor) }))
              }
            >
              <History data-icon="inline-start" />
              Load history
            </Button>
            <Field>
              <FieldLabel htmlFor="workflow-payload">JSON payload</FieldLabel>
              <Textarea
                id="workflow-payload"
                value={params}
                onChange={(e) => setParams(e.target.value)}
                className="min-h-32 font-mono"
              />
            </Field>
            <Button
              className="w-fit"
              disabled={pending || !canWrite || !instance}
              onClick={() => {
                const payload = json()
                if (payload !== undefined)
                  confirm(
                    "Start workflow",
                    `Create instance ${instance} using the configured workflow and retention policy.`,
                    command("start", { params: payload })
                  )
              }}
            >
              <Play data-icon="inline-start" />
              Start instance
            </Button>
            <Field>
              <FieldLabel htmlFor="workflow-event">Event type</FieldLabel>
              <Input
                id="workflow-event"
                value={eventType}
                onChange={(e) => setEventType(e.target.value)}
                placeholder="approval"
              />
            </Field>
            <Button
              className="w-fit"
              variant="outline"
              disabled={
                pending || !canWrite || !instance || !eventType || !generation
              }
              onClick={() => {
                const payload = json()
                if (payload !== undefined)
                  confirm(
                    "Send workflow event",
                    `Send ${eventType} to ${instance} with the JSON payload above.`,
                    command("send_event", { event_type: eventType, payload })
                  )
              }}
            >
              <Send data-icon="inline-start" />
              Send event
            </Button>
          </FieldGroup>
        </CardContent>
      </Card>
    </>
  )
}

export function WorkflowResults({
  value,
  pending,
  onNext,
  target,
}: {
  value: unknown
  pending: boolean
  onNext: (cursor: number) => void
  target?: import("@/lib/resource").ResourceTarget
}) {
  const result =
    value && typeof value === "object"
      ? (value as Record<string, unknown>)
      : null
  if (Array.isArray(result?.events)) {
    return (
      <WorkflowExecution
        key={
          (value as ExecutionHistory).scope ??
          (value as ExecutionHistory).instanceId
        }
        target={target}
        history={value as ExecutionHistory}
        pending={pending}
        onNext={onNext}
      />
    )
  }

  if (typeof result?.status === "string")
    return (
      <div className="space-y-4">
        <dl className="detail-grid">
          <dt>Instance</dt>
          <dd>{String(result.instanceId)}</dd>
          <dt>Status</dt>
          <dd>
            <Status label={result.status} />
          </dd>
          <dt>Execution generation</dt>
          <dd className="font-mono text-xs">{String(result.generation)}</dd>
          <dt>Created</dt>
          <dd>{date(Number(result.createdMs))}</dd>
          <dt>Ended</dt>
          <dd>{result.endedMs ? date(Number(result.endedMs)) : "Not ended"}</dd>
          <dt>Retention</dt>
          <dd>
            {result.retention_expired
              ? "Expired; retained until cleanup"
              : result.expiresMs
                ? date(Number(result.expiresMs))
                : "No expiry recorded"}
          </dd>
        </dl>
        <JsonView value={value} />
      </div>
    )
  return <JsonView value={value} />
}
