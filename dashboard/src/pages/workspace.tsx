import { lazy, Suspense, useState } from "react"
import {
  WorkflowDefinition,
  mergeHistory,
  type ExecutionHistory,
} from "@/components/workflow-visual"
import { Link, useParams } from "react-router-dom"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { ArrowLeft } from "lucide-react"
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
import { CronConfiguration } from "@/components/cron-configuration"
import { CronPreview } from "@/components/cron-preview"
import { QueueResults } from "@/components/resource-results"
import {
  WorkflowWorkspace,
  WorkflowResults,
} from "@/components/workflow-workspace"
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

const SqlWorkbench = lazy(() => import("@/components/sql-workbench"))
const KvWorkspace = lazy(() => import("@/components/kv-workspace"))

const resourceTitles: Record<string, string> = {
  sql: "SQL databases",
  kv: "Key-value",
  queues: "Queues",
  workflows: "Workflows",
  cron: "Cron schedules",
}

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
      else if (result.operation) toast.success("Operation completed")
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
        Back to {resourceTitles[kind] ?? kind}
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
        {kind === "sql" ? (
          <Suspense fallback={<p role="status">Loading SQL workbench…</p>}>
            <SqlWorkbench
              target={{
                fleet,
                application,
                binding,
                identity: resource?.identity,
              }}
              canRead={canRead}
              canWrite={canWrite}
            />
          </Suspense>
        ) : null}
        {kind === "kv" ? (
          <Suspense fallback={<p role="status">Loading KV explorer…</p>}>
            <KvWorkspace
              target={{
                fleet,
                application,
                binding,
                identity: resource?.identity,
              }}
              canRead={canRead}
              canWrite={canWrite}
            />
          </Suspense>
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
              {kind === "workflows" ? (
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
