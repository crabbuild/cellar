import { useRef, useState } from "react"
import { Link } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import { History, RefreshCw } from "lucide-react"
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "@/components/ui/table"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetFooter,
} from "@/components/ui/sheet"
import { Badge } from "@/components/ui/badge"
import { Feedback, JsonView, Nothing, Status } from "@/components/console-ui"
import { elapsed } from "@/components/queue-message"
import { date, fleetPath, request, useApi } from "@/lib/api"
import { useSession } from "@/lib/session"

type Execution = {
  id: number
  expression: string
  schedule_index: number
  scheduled_at: number
  started_at: number
  ended_at: number | null
  duration_ms: number | null
  attempt: number
  status: string
  target: "application" | "workflow"
  node: string
  runtime_generation: number
  error?: string | null
  result?: unknown
  retry_requested: boolean | null
  background_errors?: number
  workflow_name?: string | null
  workflow_binding?: string | null
  workflow_instance?: string | null
  configuration?: {
    prefix: string
    deployment_version: string
    schedule_revision: string | null
  } | null
}
type Page = {
  items: Execution[]
  next_cursor: string | null
  observed_at: number
  retention_limit: number
  active_alarm: number | null
}
type Configuration = {
  prefix: string
  revision: string | null
  expressions: string[]
}
const label = (status: string) =>
  status === "started"
    ? "Completion not recorded"
    : status === "workflow_accepted"
      ? "Workflow accepted"
      : status.replaceAll("_", " ")
const tone = (status: string) =>
  status === "completed"
    ? ("good" as const)
    : status.startsWith("failed")
      ? ("error" as const)
      : ("warning" as const)

export function CronExecutions({
  fleet,
  application,
}: {
  fleet: string
  application: string
}) {
  const { token, session } = useSession()
  const canRead = session.grants[fleet]?.includes("data_read") ?? false
  const path = fleetPath(
    fleet,
    `/applications/${encodeURIComponent(application)}/cron`
  )
  const config = useApi<Configuration>(path)
  const executionTrigger = useRef<HTMLButtonElement | null>(null)
  const [expression, setExpression] = useState("")
  const [selected, setSelected] = useState<{
    execution: Execution
    observedAt: number
  } | null>(null)
  const history = useMutation({
    mutationFn: async ({
      expression,
      before,
    }: {
      expression: string
      before: string | null
    }) => {
      const params = new URLSearchParams({ limit: "50" })
      if (expression) params.set("expression", expression)
      if (before) params.set("before", before)
      const page = await request<Page>(token, `${path}/executions?${params}`)
      if (!Array.isArray(page.items))
        throw new Error("This owner did not return execution history.")
      return { page, expression }
    },
    retry: false,
  })
  const current = (execution: Execution) =>
    !!config.data &&
    execution.configuration?.prefix === config.data.prefix &&
    execution.configuration.schedule_revision === config.data.revision &&
    (execution.target === "workflow" ||
      config.data.expressions.includes(execution.expression))
  const workflowLink = (execution: Execution) =>
    execution.workflow_binding && execution.workflow_instance
      ? `/fleets/${encodeURIComponent(fleet)}/workflows/${encodeURIComponent(application)}/${encodeURIComponent(execution.workflow_binding)}?instance=${encodeURIComponent(execution.workflow_instance)}`
      : null
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Execution history</CardTitle>
          <CardDescription>
            Actual attempts recorded by the scheduling cell. Saving or adopting
            a schedule does not prove execution. Workflow acceptance does not
            prove workflow completion.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {canRead ? (
            <>
              <form
                onSubmit={(event) => {
                  event.preventDefault()
                  history.mutate({
                    expression: expression.trim(),
                    before: null,
                  })
                }}
              >
                <FieldGroup className="inspection-filter-row">
                  <Field>
                    <FieldLabel htmlFor="execution-expression">
                      Recorded expression
                    </FieldLabel>
                    <Input
                      id="execution-expression"
                      placeholder="All expressions, including previous schedules"
                      maxLength={256}
                      value={expression}
                      onChange={(event) => setExpression(event.target.value)}
                    />
                  </Field>
                  <Button disabled={history.isPending}>
                    <History data-icon="inline-start" />
                    Load execution history
                  </Button>
                </FieldGroup>
              </form>
              <Feedback
                loading={history.isPending && !history.data}
                error={history.error}
              >
                {history.data ? (
                  <>
                    <div className="flex flex-wrap items-center gap-3">
                      <span className="workspace-caption">
                        Observed {date(history.data.page.observed_at)} · last{" "}
                        {history.data.page.retention_limit} attempts retained
                      </span>
                      <Button
                        variant="ghost"
                        size="sm"
                        disabled={history.isPending}
                        onClick={() =>
                          history.mutate({
                            expression: history.data!.expression,
                            before: null,
                          })
                        }
                      >
                        <RefreshCw data-icon="inline-start" />
                        Refresh executions
                      </Button>
                    </div>
                    {history.data.page.items.length ? (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Schedule</TableHead>
                            <TableHead>Scheduled / started</TableHead>
                            <TableHead>Result</TableHead>
                            <TableHead>Duration</TableHead>
                            <TableHead>Configuration</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {history.data.page.items.map((execution) => (
                            <TableRow key={execution.id}>
                              <TableCell>
                                <Button
                                  variant="link"
                                  onClick={(event) => {
                                    executionTrigger.current =
                                      event.currentTarget
                                    setSelected({
                                      execution,
                                      observedAt:
                                        history.data!.page.observed_at,
                                    })
                                  }}
                                >
                                  {execution.expression}
                                </Button>
                                <p className="workspace-caption">
                                  {execution.target} · attempt{" "}
                                  {execution.attempt}
                                </p>
                              </TableCell>
                              <TableCell>
                                <div>{date(execution.scheduled_at)}</div>
                                <div className="workspace-caption">
                                  Started {date(execution.started_at)}
                                </div>
                              </TableCell>
                              <TableCell>
                                <Status
                                  label={label(execution.status)}
                                  tone={tone(execution.status)}
                                />
                              </TableCell>
                              <TableCell>
                                {elapsed(execution.duration_ms)}
                              </TableCell>
                              <TableCell>
                                <Badge variant="outline">
                                  {current(execution)
                                    ? "Current configuration"
                                    : "Earlier or unrecorded configuration"}
                                </Badge>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    ) : (
                      <Nothing
                        title="No recorded attempts"
                        description="History starts when a framework with execution recording runs an occurrence. Empty history is not evidence that this schedule never ran."
                      />
                    )}
                    <p className="workspace-caption">
                      Owner's active alarm:{" "}
                      {history.data.page.active_alarm == null
                        ? "None recorded"
                        : date(history.data.page.active_alarm)}
                      . This is a scheduling observation, not an execution
                      result.
                    </p>
                    {history.data.page.next_cursor ? (
                      <Button
                        variant="outline"
                        className="w-fit"
                        disabled={history.isPending}
                        onClick={() =>
                          history.mutate({
                            expression: history.data!.expression,
                            before: history.data!.page.next_cursor,
                          })
                        }
                      >
                        Earlier executions
                      </Button>
                    ) : null}
                  </>
                ) : (
                  <Nothing
                    title="Inspect actual executions"
                    description="Load the scheduling cell's retained attempts to see handler results and requested retries."
                  />
                )}
              </Feedback>
            </>
          ) : (
            <p className="workspace-caption">
              Data read permission is required to inspect execution history.
              Configuration and node adoption remain available with their own
              permissions.
            </p>
          )}
        </CardContent>
      </Card>
      <Sheet
        open={selected !== null}
        onOpenChange={(open) => {
          if (!open) setSelected(null)
        }}
      >
        <SheetContent
          className="resource-detail-sheet"
          onCloseAutoFocus={(event) => {
            event.preventDefault()
            executionTrigger.current?.focus()
          }}
        >
          <SheetHeader>
            <SheetTitle>Schedule execution</SheetTitle>
            <SheetDescription>
              Recorded attempt #{selected?.execution.id} · observed{" "}
              {date(selected?.observedAt)}. Started attempts without a terminal
              record may still be running or may have been interrupted.
            </SheetDescription>
          </SheetHeader>
          {selected ? (
            <div className="resource-detail-body">
              <code>{selected.execution.expression}</code>
              <Status
                label={label(selected.execution.status)}
                tone={tone(selected.execution.status)}
              />
              <dl className="detail-grid">
                <dt>Target</dt>
                <dd>{selected.execution.target}</dd>
                <dt>Scheduled</dt>
                <dd>{date(selected.execution.scheduled_at)}</dd>
                <dt>Started</dt>
                <dd>{date(selected.execution.started_at)}</dd>
                <dt>Start delay</dt>
                <dd>
                  {elapsed(
                    selected.execution.started_at -
                      selected.execution.scheduled_at
                  )}
                </dd>
                <dt>Finished</dt>
                <dd>
                  {selected.execution.ended_at == null
                    ? "Completion not recorded"
                    : date(selected.execution.ended_at)}
                </dd>
                <dt>Duration</dt>
                <dd>{elapsed(selected.execution.duration_ms)}</dd>
                <dt>Attempt in activation</dt>
                <dd>{selected.execution.attempt}</dd>
                <dt>Retry requested</dt>
                <dd>
                  {selected.execution.retry_requested == null
                    ? "Not recorded"
                    : selected.execution.retry_requested
                      ? "Yes; inspect later attempts for its outcome"
                      : "No"}
                </dd>
                <dt>Background failures</dt>
                <dd>
                  {selected.execution.target === "workflow"
                    ? "Not applicable"
                    : (selected.execution.background_errors ?? "Not recorded")}
                </dd>
                <dt>Node / runtime generation</dt>
                <dd>
                  {selected.execution.node} /{" "}
                  {selected.execution.runtime_generation}
                </dd>
                <dt>Deployment</dt>
                <dd className="break-all">
                  {selected.execution.configuration?.deployment_version ??
                    "Not recorded"}
                </dd>
                <dt>Schedule revision</dt>
                <dd className="break-all">
                  {selected.execution.configuration?.schedule_revision ??
                    "Deployment defaults or not recorded"}
                </dd>
              </dl>
              {selected.execution.error ? (
                <section>
                  <h3>Recorded error</h3>
                  <pre className="inspection-payload">
                    {selected.execution.error}
                  </pre>
                </section>
              ) : null}
              {selected.execution.target === "workflow" ? (
                <section>
                  <h3>Requested workflow</h3>
                  <p>
                    {selected.execution.workflow_name} ·{" "}
                    {selected.execution.workflow_instance}
                  </p>
                  {workflowLink(selected.execution) ? (
                    <Link
                      className="text-link"
                      to={workflowLink(selected.execution)!}
                    >
                      Inspect workflow execution →
                    </Link>
                  ) : null}
                  <p className="workspace-caption">
                    This record confirms the create request's result. Inspect
                    the workflow's own history for completion.
                  </p>
                </section>
              ) : (
                <section>
                  <h3>Handler result</h3>
                  <JsonView
                    value={selected.execution.result ?? "No result recorded"}
                  />
                  <p className="workspace-caption">
                    The handler's return kind is recorded without capturing
                    arbitrary payloads. Duration includes drained background
                    work.
                  </p>
                </section>
              )}
            </div>
          ) : null}
          <SheetFooter>
            <Button variant="outline" onClick={() => setSelected(null)}>
              Close execution details
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    </>
  )
}
