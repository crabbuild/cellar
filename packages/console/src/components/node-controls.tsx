// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { NodeStopControls } from "@/components/node-stop-controls"
import { useState } from "react"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { Activity, Pause, Play, RotateCw } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
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
import { JsonView, Status } from "@/components/console-ui"
import { date, fleetPath, request, type Node } from "@/lib/api"
import { useSession } from "@/lib/session"

type Live = {
  generation: string
  authoritative: boolean
  fleet_ready: boolean
  draining: boolean
  observed_at: number
  capabilities: { balancing: boolean; reload: boolean; stop?: boolean }
  deployment: { version: string; generation: number } | null
}
type Action = "state" | "pause_balancing" | "resume_balancing" | "reload"
const labels: Record<Action, string> = {
  state: "Inspect live state",
  pause_balancing: "Pause balancing",
  resume_balancing: "Resume balancing",
  reload: "Reload deployment",
}
export function NodeControls({ fleet, node }: { fleet: string; node: Node }) {
  const { token, session } = useSession()
  const cache = useQueryClient()
  const [live, setLive] = useState<Live | null>(null)
  const [confirmation, setConfirmation] = useState<Action | null>(null)
  const canOperate = session.grants[fleet]?.includes("operate") ?? false
  const mutation = useMutation({
    mutationFn: (action: Action) =>
      request<{ result: unknown; replayed?: boolean }>(
        token,
        fleetPath(fleet, `/nodes/${encodeURIComponent(node.id)}/action`),
        {
          method: "POST",
          body: JSON.stringify({
            generation: node.generation,
            action,
            operation_id: crypto.randomUUID(),
          }),
        }
      ),
    onSuccess: (reply, action) => {
      if (reply.replayed) {
        toast.info(
          "Operation already recorded. Check Operations for its outcome."
        )
        return
      }
      if (action === "state") setLive(reply.result as Live)
      else {
        toast.success(
          action === "reload"
            ? "Reload request completed"
            : "Balancing preference accepted; waiting for lease publication"
        )
        void cache.invalidateQueries({
          queryKey: [fleetPath(fleet, "/operations")],
        })
      }
    },
    onError: (error: Error) => toast.error(error.message),
  })
  const observed = live?.generation === node.generation ? live : null
  return (
    <Card className="mt-5">
      <CardHeader>
        <CardTitle>Live state and controls</CardTitle>
        <CardDescription>
          Commands target this exact node generation. Inspect the host to
          discover its supported controls.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            disabled={mutation.isPending || !node.lease_current}
            onClick={() => mutation.mutate("state")}
          >
            <Activity data-icon="inline-start" />
            Inspect live state
          </Button>
          {(
            [
              { action: "pause_balancing", icon: Pause },
              { action: "resume_balancing", icon: Play },
              { action: "reload", icon: RotateCw },
            ] as const
          ).map(({ action, icon: Icon }) => (
            <Button
              key={action}
              variant="outline"
              disabled={
                !canOperate ||
                mutation.isPending ||
                !node.lease_current ||
                !observed?.authoritative ||
                observed.draining ||
                !(action === "reload"
                  ? observed.capabilities.reload
                  : observed.capabilities.balancing)
              }
              onClick={() => setConfirmation(action)}
            >
              <Icon data-icon="inline-start" />
              {labels[action]}
            </Button>
          ))}
        </div>
        {observed ? (
          <dl className="detail-grid">
            <dt>Node authority</dt>
            <dd>
              <Status
                label={observed.authoritative ? "Confirmed" : "Unavailable"}
                tone={observed.authoritative ? "good" : "warning"}
              />
            </dd>
            <dt>Fleet readiness gate</dt>
            <dd>{observed.fleet_ready ? "Opened" : "Waiting"}</dd>
            <dt>Draining</dt>
            <dd>{observed.draining ? "Yes" : "No"}</dd>
            <dt>Deployment</dt>
            <dd>{observed.deployment?.version ?? "No application runtime"}</dd>
            <dt>Observed at</dt>
            <dd>{date(observed.observed_at)}</dd>
            {!observed.capabilities.reload ? (
              <>
                <dt>Reload</dt>
                <dd>
                  Managed by the embedding host; no deployment watcher is
                  configured.
                </dd>
              </>
            ) : null}
          </dl>
        ) : null}
        {mutation.error ? (
          <Alert variant="destructive">
            <AlertTitle>Node action failed</AlertTitle>
            <AlertDescription>
              {mutation.error.message} A lost reply may leave the outcome
              unknown; refresh before repeating a mutation.
            </AlertDescription>
          </Alert>
        ) : null}
        {mutation.data && mutation.variables !== "state" ? (
          <JsonView value={mutation.data} />
        ) : null}
        <NodeStopControls
          fleet={fleet}
          node={node}
          available={
            !!observed?.capabilities.stop &&
            observed.authoritative &&
            !observed.draining &&
            node.lease_current
          }
        />
        <AlertDialog
          open={confirmation !== null}
          onOpenChange={(open) => {
            if (!open) setConfirmation(null)
          }}
        >
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>
                {confirmation ? labels[confirmation] : "Node action"}
              </AlertDialogTitle>
              <AlertDialogDescription>
                {confirmation === "pause_balancing"
                  ? "One paused node stops every donor in the fleet from balancing. Resume from this same node to clear its preference."
                  : confirmation === "resume_balancing"
                    ? "Clear this node’s pause preference. Another paused node can keep fleet balancing paused."
                    : "Ask this host to load its current deployment pointer. The existing generation continues serving if the new deployment fails to load."}
                <br />
                Fleet: {fleet}
                <br />
                Node: {node.id}
                <br />
                Generation: {node.generation}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={() => {
                  if (confirmation) mutation.mutate(confirmation)
                  setConfirmation(null)
                }}
              >
                Confirm node action
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </CardContent>
    </Card>
  )
}
