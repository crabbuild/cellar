import { useState } from "react"
import {
  WorkflowDefinition,
  mergeHistory,
  type ExecutionHistory,
} from "@/components/workflow-visual"
import { Link, useParams } from "react-router-dom"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { ArrowLeft, Play, Search, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { CronConfiguration } from "@/components/cron-configuration"
import { CronPreview } from "@/components/cron-preview"
import { KvResults, QueueResults } from "@/components/resource-results"
import {
  WorkflowWorkspace,
  WorkflowResults,
} from "@/components/workflow-workspace"
import { SqlBrowser } from "@/components/sql-browser"
import { SqlResults } from "@/components/sql-results"
import { Textarea } from "@/components/ui/textarea"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Feedback, JsonView, PageHeading } from "@/components/console-ui"
import { fleetPath, request, useApi, type Application } from "@/lib/api"
import { useSession } from "@/lib/session"

type Result = {
  result?: unknown
  operation?: { status: string }
  replayed?: boolean
}

export default function Workspace({ kind }: { kind: string }) {
  const { fleet, application, binding } = useParams()
  return (
    <ResourceWorkspace
      key={JSON.stringify([fleet, application, binding, kind])}
      kind={kind}
    />
  )
}

function ResourceWorkspace({ kind }: { kind: string }) {
  const { fleet = "", application = "", binding = "" } = useParams()
  const { token, session } = useSession()
  const cache = useQueryClient()
  const app = useApi<Application>(
    fleetPath(fleet, `/applications/${encodeURIComponent(application)}`)
  )
  const resource = app.data?.bindings.find((item) => item.name === binding)
  const canWrite = session.grants[fleet]?.includes("data_write") ?? false
  const canRead = session.grants[fleet]?.includes("data_read") ?? false
  const [sql, setSql] = useState(
    "SELECT name, type FROM sqlite_schema\nWHERE name NOT LIKE '__sys_%'\nORDER BY name LIMIT 100;"
  )
  const [sqlMode, setSqlMode] = useState("read")
  const [key, setKey] = useState("")
  const [value, setValue] = useState("")
  const [valueEncoding, setValueEncoding] = useState("utf8")
  const [metadata, setMetadata] = useState("null")
  const [ttl, setTtl] = useState("")
  const [prefix, setPrefix] = useState("")
  const [confirmation, setConfirmation] = useState<{
    title: string
    description: string
    command: Record<string, unknown>
  } | null>(null)
  const [workflowHistory, setWorkflowHistory] =
    useState<ExecutionHistory | null>(null)
  const action = useMutation({
    mutationFn: (command: Record<string, unknown>) =>
      request<Result>(token, fleetPath(fleet, "/resources/action"), {
        method: "POST",
        body: JSON.stringify({
          application,
          binding,
          operation_id: crypto.randomUUID(),
          command,
        }),
      }),
    onSuccess: (result, command) => {
      if (
        kind === "workflows" &&
        (command.command as { operation?: string })?.operation === "history"
      ) {
        const page = result.result as ExecutionHistory
        if (Array.isArray(page?.events))
          setWorkflowHistory((previous) => mergeHistory(previous, page))
      }
      if (result.replayed)
        toast.info(
          "This operation was already recorded. Check Operations for its outcome."
        )
      else
        toast.success(
          result.operation ? "Operation completed" : "Resource inspected"
        )
      void cache.invalidateQueries({
        queryKey: [fleetPath(fleet, "/operations")],
      })
    },
    onError: (e: Error) => toast.error(e.message),
  })
  const displayed =
    action.data ??
    (kind === "workflows" &&
    (action.variables?.command as { operation?: string })?.operation ===
      "history" &&
    workflowHistory &&
    workflowHistory.instanceId === action.variables?.instance
      ? { result: workflowHistory, operation: undefined }
      : undefined)
  const execute = (command: Record<string, unknown>) => action.mutate(command)
  const confirm = (
    title: string,
    description: string,
    command: Record<string, unknown>
  ) => setConfirmation({ title, description, command })
  return (
    <div className="page-stack">
      <Link
        className="back-link"
        to={`/fleets/${encodeURIComponent(fleet)}/${kind}`}
      >
        <ArrowLeft />
        Back to {kind}
      </Link>
      <PageHeading
        title={binding}
        description={`${application} · ${resource?.identity ?? "Application resource"}`}
      />
      <Feedback loading={app.isPending} error={app.error}>
        <div className="flex items-center gap-3">
          <Badge variant="outline">
            {kind === "cron" || kind === "workflows"
              ? "Application resource"
              : "Shared resource"}
          </Badge>
          {resource?.scope ? (
            <Link
              className="text-link text-xs"
              to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(resource.scope)}`}
            >
              Inspect backing cell →
            </Link>
          ) : null}
        </div>
        {kind === "sql" && resource ? (
          <SqlBrowser
            fleet={fleet}
            application={application}
            binding={binding}
          />
        ) : null}
        {kind === "sql" ? (
          <Card>
            <CardHeader>
              <CardTitle>SQL console</CardTitle>
              <CardDescription>
                Queries execute through the database owner. Review statements
                before running them.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  if (sqlMode === "read") {
                    execute({ action: "sql", sql, readonly: true })
                    return
                  }
                  confirm(
                    "Execute SQL",
                    "This statement may change stored data. Review the SQL and target before continuing.",
                    { action: "sql", sql, readonly: false }
                  )
                }}
              >
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="sql-mode">Query mode</FieldLabel>
                    <Select value={sqlMode} onValueChange={setSqlMode}>
                      <SelectTrigger id="sql-mode">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="read" disabled={!canRead}>
                          Read only
                        </SelectItem>
                        <SelectItem
                          value="write"
                          disabled={!canWrite || !canRead}
                        >
                          Read and write
                        </SelectItem>
                      </SelectContent>
                    </Select>
                    <FieldDescription>
                      Queries are limited to 1,000 rows per result, 1 MiB of
                      values and a bounded execution budget.
                    </FieldDescription>
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="sql">SQL statement</FieldLabel>
                    <Textarea
                      className="min-h-44 font-mono"
                      id="sql"
                      value={sql}
                      onChange={(e) => setSql(e.target.value)}
                      spellCheck={false}
                    />
                    <FieldDescription>
                      Target: {fleet} / {application} / {binding}
                    </FieldDescription>
                  </Field>
                  <div>
                    <Button
                      disabled={
                        action.isPending ||
                        !(sqlMode === "read" ? canRead : canRead && canWrite) ||
                        !resource
                      }
                    >
                      <Play data-icon="inline-start" />
                      Run statement
                    </Button>
                  </div>
                </FieldGroup>
              </form>
            </CardContent>
          </Card>
        ) : null}
        {kind === "kv" ? (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Key explorer</CardTitle>
                <CardDescription>
                  Read up to 100 keys by prefix, or inspect an exact key.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="prefix">Key prefix</FieldLabel>
                    <Input
                      id="prefix"
                      value={prefix}
                      onChange={(e) => setPrefix(e.target.value)}
                      placeholder="sessions/"
                    />
                  </Field>
                  <div>
                    <Button
                      variant="outline"
                      disabled={!canRead || action.isPending}
                      onClick={() =>
                        execute({ action: "kv_list", prefix, after: "" })
                      }
                    >
                      <Search data-icon="inline-start" />
                      List keys
                    </Button>
                  </div>
                </FieldGroup>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Read or write a key</CardTitle>
                <CardDescription>
                  Writes use last-write-wins semantics. Existing values may
                  change while you edit.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="key">Key</FieldLabel>
                    <Input
                      id="key"
                      value={key}
                      onChange={(e) => setKey(e.target.value)}
                    />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="value-encoding">
                      Value encoding
                    </FieldLabel>
                    <Select
                      value={valueEncoding}
                      onValueChange={setValueEncoding}
                    >
                      <SelectTrigger id="value-encoding">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="utf8">UTF-8 text</SelectItem>
                        <SelectItem value="base64">Base64 binary</SelectItem>
                      </SelectContent>
                    </Select>
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="value">Value</FieldLabel>
                    <Textarea
                      id="value"
                      value={value}
                      onChange={(e) => setValue(e.target.value)}
                      className="min-h-28 font-mono"
                    />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="kv-metadata">
                      Metadata (JSON)
                    </FieldLabel>
                    <Textarea
                      id="kv-metadata"
                      value={metadata}
                      onChange={(e) => setMetadata(e.target.value)}
                      className="font-mono"
                    />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="ttl">
                      Expires after (seconds)
                    </FieldLabel>
                    <Input
                      id="ttl"
                      type="number"
                      min="60"
                      placeholder="No expiration"
                      value={ttl}
                      onChange={(e) => setTtl(e.target.value)}
                    />
                  </Field>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      variant="outline"
                      disabled={!key || !canRead || action.isPending}
                      onClick={() => execute({ action: "kv_get", key })}
                    >
                      Read key
                    </Button>
                    <Button
                      disabled={!key || !canWrite || action.isPending}
                      onClick={() => {
                        let parsedMetadata: unknown
                        try {
                          parsedMetadata = JSON.parse(metadata)
                        } catch {
                          toast.error("Metadata must be valid JSON.")
                          return
                        }
                        if (
                          ttl &&
                          (!Number.isSafeInteger(Number(ttl)) ||
                            Number(ttl) < 60)
                        ) {
                          toast.error(
                            "Expiration must be a whole number of at least 60 seconds."
                          )
                          return
                        }
                        let bytes: Uint8Array
                        try {
                          bytes =
                            valueEncoding === "base64"
                              ? Uint8Array.from(atob(value), (c) =>
                                  c.charCodeAt(0)
                                )
                              : new TextEncoder().encode(value)
                        } catch {
                          toast.error("Enter valid base64.")
                          return
                        }
                        if (bytes.length > 1024 * 1024) {
                          toast.error("Dashboard values are limited to 1 MiB.")
                          return
                        }
                        let binary = ""
                        for (const byte of bytes)
                          binary += String.fromCharCode(byte)
                        confirm(
                          "Save key",
                          `Write ${key} in this shared namespace.`,
                          {
                            action: "kv_put",
                            key,
                            value_base64: btoa(binary),
                            metadata: parsedMetadata,
                            expiration_ttl: ttl ? Number(ttl) : null,
                          }
                        )
                      }}
                    >
                      Save key
                    </Button>
                    <Button
                      variant="destructive"
                      disabled={!key || !canWrite || action.isPending}
                      onClick={() =>
                        confirm(
                          "Delete key",
                          `Delete ${key} from this shared namespace.`,
                          { action: "kv_delete", key }
                        )
                      }
                    >
                      <Trash2 data-icon="inline-start" />
                      Delete key
                    </Button>
                  </div>
                </FieldGroup>
              </CardContent>
            </Card>
          </>
        ) : null}
        {kind === "queues" ? (
          <Card>
            <CardHeader>
              <CardTitle>Queue controls</CardTitle>
              <CardDescription>
                Inspect messages without consuming them. Mutations are routed to
                the broker owner.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {["info", "peek", "pause", "resume", "redrive", "purge"].map(
                  (operation) => (
                    <Button
                      key={operation}
                      variant={
                        operation === "purge" ? "destructive" : "outline"
                      }
                      disabled={
                        action.isPending ||
                        (["info", "peek"].includes(operation)
                          ? !canRead
                          : !canWrite)
                      }
                      onClick={() => {
                        const command = {
                          action: "queue",
                          operation,
                          limit: 100,
                        }
                        if (["info", "peek"].includes(operation))
                          execute(command)
                        else
                          confirm(
                            `${operation[0].toUpperCase()}${operation.slice(1)} queue`,
                            operation === "purge"
                              ? "Remove messages using this broker’s purge policy. This cannot be undone."
                              : operation === "redrive"
                                ? "Redrive up to 100 dead-letter messages. Consumers may process them again."
                                : `${operation} delivery on this shared broker.`,
                            command
                          )
                      }}
                    >
                      {operation[0].toUpperCase()}
                      {operation.slice(1)}
                    </Button>
                  )
                )}
              </div>
            </CardContent>
          </Card>
        ) : null}
        {kind === "cron" ? (
          <CronConfiguration fleet={fleet} application={application} />
        ) : null}
        {kind === "cron" ? <CronPreview fleet={fleet} /> : null}
        {kind === "cron" ? (
          <Card>
            <CardHeader>
              <CardTitle>Deployment declarations</CardTitle>
              <CardDescription>
                Expressions use Cellar’s cron dialect in UTC.{" "}
                {app.data?.primary
                  ? "This is the fleet’s primary application; inspect its scheduling cell to observe the current alarm."
                  : "These are deployment declarations. Cellar’s scheduler runs the primary application; schedules on a cohosted application are not activated."}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Expression</TableHead>
                    <TableHead>Target</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {app.data?.crons.map((expression, i) => (
                    <TableRow key={i}>
                      <TableCell>
                        <code>{expression}</code>
                      </TableCell>
                      <TableCell>Application scheduled handler</TableCell>
                    </TableRow>
                  ))}
                  {app.data?.bindings.flatMap((b) =>
                    b.schedules.map((expression, i) => (
                      <TableRow key={`${b.name}-${i}`}>
                        <TableCell>
                          <code>{expression}</code>
                        </TableCell>
                        <TableCell>Workflow · {b.name}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        ) : null}
        {kind === "workflows" ? (
          <>
            <WorkflowDefinition definition={resource?.definition} />
            <WorkflowWorkspace
              canRead={canRead}
              canWrite={canWrite}
              pending={action.isPending}
              result={action.data?.result}
              execute={execute}
              confirm={confirm}
            />
          </>
        ) : null}
        {action.error ? (
          <Alert variant="destructive">
            <AlertTitle>Operation did not return success</AlertTitle>
            <AlertDescription>
              {action.error.message} If this was a write, check Operations
              before trying it again; its outcome may be unknown.
            </AlertDescription>
          </Alert>
        ) : null}
        {displayed ? (
          <Card>
            <CardHeader>
              <CardTitle>Result</CardTitle>
              <CardDescription>
                {displayed.operation?.status.replaceAll("_", " ") ??
                  "Resource response"}
              </CardDescription>
            </CardHeader>
            <CardContent>
              {kind === "sql" && displayed.result ? (
                <SqlResults value={displayed.result} />
              ) : kind === "kv" ? (
                <KvResults
                  value={displayed.result}
                  pending={action.isPending}
                  onRead={(key) => {
                    setKey(key)
                    execute({ action: "kv_get", key })
                  }}
                  onNext={(after) =>
                    execute({
                      action: "kv_list",
                      prefix: action.variables?.prefix ?? "",
                      after,
                    })
                  }
                />
              ) : kind === "workflows" ? (
                <WorkflowResults
                  value={
                    Array.isArray(
                      (displayed.result as ExecutionHistory)?.events
                    )
                      ? workflowHistory?.instanceId ===
                        action.variables?.instance
                        ? workflowHistory
                        : displayed.result
                      : displayed.result
                  }
                  pending={action.isPending}
                  onNext={(cursor) =>
                    execute({
                      action: "workflow",
                      instance: action.variables?.instance,
                      command: { operation: "history", cursor },
                    })
                  }
                />
              ) : kind === "queues" ? (
                <QueueResults value={displayed.result} />
              ) : (
                <JsonView value={displayed.result ?? displayed} />
              )}
            </CardContent>
          </Card>
        ) : null}
      </Feedback>
      <AlertDialog
        open={confirmation !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmation(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{confirmation?.title}</AlertDialogTitle>
            <AlertDialogDescription>
              {confirmation?.description}
              <br />
              Fleet: {fleet}
              <br />
              Target: {application} / {binding}
            </AlertDialogDescription>
          </AlertDialogHeader>
          {confirmation?.command.action === "sql" ? (
            <JsonView value={confirmation.command.sql} />
          ) : null}
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (confirmation) execute(confirmation.command)
                setConfirmation(null)
              }}
            >
              Confirm operation
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
