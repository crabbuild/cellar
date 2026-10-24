import { useState } from "react"
import { Link } from "react-router-dom"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { Activity, Pencil, RotateCw, Save } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Textarea } from "@/components/ui/textarea"
import { Field, FieldLabel, FieldDescription } from "@/components/ui/field"
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Feedback, Status } from "@/components/console-ui"
import { date, fleetPath, request, useApi, type Page } from "@/lib/api"
import { useSession } from "@/lib/session"

type Configuration = {
  application: string
  deployment_revision: string
  prefix: string
  revision: string | null
  expressions: string[]
  primary: boolean
}
type Observation = {
  node: string
  generation: string
  status: string
  observed_revision: string | null
  observed_at: number
  error: string | null
}

export function CronConfiguration({
  fleet,
  application,
}: {
  fleet: string
  application: string
}) {
  const { token, session } = useSession()
  const cache = useQueryClient()
  const path = fleetPath(
    fleet,
    `/applications/${encodeURIComponent(application)}/cron`
  )
  const current = useApi<Configuration>(path)
  const [draft, setDraft] = useState<{
    base: Configuration
    text: string
  } | null>(null)
  const [confirmation, setConfirmation] = useState(false)
  const canConfigure = session.grants[fleet]?.includes("configure") ?? false
  const save = useMutation({
    mutationFn: (selected: NonNullable<typeof draft>) =>
      request<{ configuration?: Configuration; replayed?: boolean }>(
        token,
        path,
        {
          method: "POST",
          body: JSON.stringify({
            operation_id: crypto.randomUUID(),
            deployment_revision: selected.base.deployment_revision,
            expected_revision: selected.base.revision,
            expressions: selected.text
              .split("\n")
              .map((value) => value.trim())
              .filter(Boolean),
          }),
        }
      ),
    onSuccess: (reply) => {
      toast.info(
        reply.replayed
          ? "Already recorded. Inspect Operations before retrying."
          : "Schedule saved. Check node adoption below."
      )
      setDraft(null)
      observations.reset()
      void cache.invalidateQueries({ queryKey: [path] })
      void cache.invalidateQueries({
        queryKey: [fleetPath(fleet, "/operations")],
      })
    },
  })
  const observations = useMutation({
    mutationFn: (cursor: string | null) =>
      request<Page<Observation>>(
        token,
        `${path}/nodes?limit=20${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`
      ),
  })
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Application schedule</CardTitle>
          <CardDescription>
            Changes apply to this deployment. A new deployment version or
            changed declarations restores deployment schedules. Workflow
            schedules remain in application configuration.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Feedback loading={current.isPending} error={current.error}>
            {current.data ? (
              <>
                <div className="flex flex-wrap items-center gap-3">
                  <Status
                    label={
                      current.data.primary
                        ? "Primary application"
                        : "Not active in this fleet"
                    }
                    tone={current.data.primary ? "good" : "warning"}
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => void current.refetch()}
                    disabled={current.isFetching}
                  >
                    <RotateCw data-icon="inline-start" />
                    Refresh configuration
                  </Button>
                  {current.data.primary ? (
                    <Link
                      className="text-link text-xs"
                      to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(`.cron:${application}`)}`}
                    >
                      Inspect scheduling cell →
                    </Link>
                  ) : null}
                </div>
                {draft ? (
                  <>
                    <Field>
                      <FieldLabel htmlFor="cron-expressions">
                        Cron expressions
                      </FieldLabel>
                      <Textarea
                        id="cron-expressions"
                        className="min-h-36 font-mono"
                        value={draft.text}
                        maxLength={16448}
                        onChange={(e) =>
                          setDraft({ ...draft, text: e.target.value })
                        }
                      />
                      <FieldDescription>
                        One expression per line, up to 64. Leave empty to
                        disable application scheduled handlers. Times use UTC.
                      </FieldDescription>
                    </Field>
                    <div className="flex gap-2">
                      <Button
                        disabled={save.isPending}
                        onClick={() => setConfirmation(true)}
                      >
                        <Save data-icon="inline-start" />
                        Save schedules
                      </Button>
                      <Button
                        variant="outline"
                        disabled={save.isPending}
                        onClick={() => setDraft(null)}
                      >
                        Cancel editing
                      </Button>
                    </div>
                  </>
                ) : (
                  <>
                    {current.data.expressions.length ? (
                      <ul className="space-y-2">
                        {current.data.expressions.map((expression, i) => (
                          <li
                            key={`${expression}-${i}`}
                            className="rounded-md border px-3 py-2 font-mono text-sm"
                          >
                            {expression}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="text-sm text-muted-foreground">
                        No application scheduled handlers are configured.
                      </p>
                    )}
                    <Button
                      variant="outline"
                      disabled={!canConfigure || !current.data.primary}
                      onClick={() =>
                        setDraft({
                          base: current.data!,
                          text: current.data!.expressions.join("\n"),
                        })
                      }
                    >
                      <Pencil data-icon="inline-start" />
                      Edit schedules
                    </Button>
                  </>
                )}
                <p className="text-xs break-all text-muted-foreground">
                  Schedule revision:{" "}
                  {current.data.revision ?? "Deployment defaults"}
                </p>
              </>
            ) : null}
          </Feedback>
          {save.error ? (
            <Alert variant="destructive">
              <AlertTitle>Schedule save was not confirmed</AlertTitle>
              <AlertDescription>
                {save.error.message} Refresh configuration and inspect
                Operations before retrying.
              </AlertDescription>
            </Alert>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Node adoption</CardTitle>
          <CardDescription>
            Observe up to 20 nodes per page. Adoption confirms the loaded
            configuration; inspect the scheduling cell for its alarm. Embedded
            hosts adopt through their host application.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-2">
            <Button
              variant="outline"
              disabled={observations.isPending}
              onClick={() => observations.mutate(null)}
            >
              <Activity data-icon="inline-start" />
              Check node adoption
            </Button>
            {observations.data?.next_cursor ? (
              <Button
                variant="outline"
                disabled={observations.isPending}
                onClick={() =>
                  observations.mutate(observations.data!.next_cursor)
                }
              >
                Next nodes
              </Button>
            ) : null}
          </div>
          <Feedback loading={observations.isPending} error={observations.error}>
            {observations.data ? (
              <>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Node</TableHead>
                      <TableHead>Schedule</TableHead>
                      <TableHead>Observed</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {observations.data.items.map((node) => (
                      <TableRow key={`${node.node}@${node.generation}`}>
                        <TableCell>
                          <Link
                            className="text-link"
                            to={`/fleets/${encodeURIComponent(fleet)}/nodes/${encodeURIComponent(node.node)}`}
                          >
                            {node.node}
                          </Link>
                          {node.error ? (
                            <p className="mt-1 text-xs text-muted-foreground">
                              {node.error}
                            </p>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          <Status
                            label={node.status.replaceAll("_", " ")}
                            tone={
                              node.status === "adopted" ? "good" : "warning"
                            }
                          />
                        </TableCell>
                        <TableCell className="text-xs">
                          {date(node.observed_at)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                {!observations.data.items.length ? (
                  <p className="text-sm text-muted-foreground">
                    No nodes in this page.
                  </p>
                ) : null}
                <p className="text-xs text-muted-foreground">
                  Coverage: {observations.data.coverage}. Observed{" "}
                  {date(observations.data.observed_at)}.
                </p>
                {observations.data.warnings.map((warning, i) => (
                  <p
                    className="text-xs text-amber-700 dark:text-amber-400"
                    key={i}
                  >
                    {warning}
                  </p>
                ))}
              </>
            ) : null}
          </Feedback>
        </CardContent>
      </Card>
      <AlertDialog open={confirmation} onOpenChange={setConfirmation}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Save these schedules?</AlertDialogTitle>
            <AlertDialogDescription>
              Fleet: {fleet}
              <br />
              Application: {application}
              <br />
              Deployment revision: {draft?.base.deployment_revision}
              <br />
              The server rejects a stale revision. Nodes adopt independently;
              this does not run a scheduled handler immediately.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <pre className="max-h-40 overflow-auto rounded-md bg-muted p-3 text-xs">
            {draft?.text.trim() || "Disable all application scheduled handlers"}
          </pre>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (draft) save.mutate(draft)
                setConfirmation(false)
              }}
            >
              Confirm schedule change
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
