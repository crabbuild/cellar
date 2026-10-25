// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { Power } from "lucide-react"
import { Button } from "@/components/ui/button"
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
import { StopReceiptView, type StopLocator } from "@/components/stop-receipt"
import { fleetPath, request, type Node } from "@/lib/api"
import { useSession } from "@/lib/session"

type Mode = "handoff" | "preserve"
export function NodeStopControls({
  fleet,
  node,
  available,
}: {
  fleet: string
  node: Node
  available: boolean
}) {
  const { token, session } = useSession()
  const cache = useQueryClient()
  const [confirmation, setConfirmation] = useState<Mode | null>(null)
  const [tracking, setTracking] = useState<StopLocator | null>(null)
  const stop = useMutation({
    mutationFn: ({ mode, operation }: { mode: Mode; operation: string }) =>
      request(
        token,
        fleetPath(fleet, `/nodes/${encodeURIComponent(node.id)}/action`),
        {
          method: "POST",
          body: JSON.stringify({
            generation: node.generation,
            operation_id: operation,
            action: { stop: { operation_id: operation, mode } },
          }),
        }
      ),
    onSettled: () => {
      void cache.invalidateQueries({
        queryKey: [fleetPath(fleet, "/operations")],
      })
    },
  })
  const allowed =
    available &&
    (session.grants[fleet]?.includes("operate") ?? false) &&
    !tracking &&
    !stop.isPending
  return (
    <div className="space-y-4 border-t pt-4">
      <div>
        <h3 className="text-sm font-semibold">Drain and stop</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          A durable receipt tracks drain proof and joined work. Your host’s
          supervisor controls whether the process is restarted.
        </p>
      </div>
      <div className="flex flex-wrap gap-2">
        <Button
          variant="outline"
          disabled={!allowed}
          onClick={() => setConfirmation("handoff")}
        >
          <Power data-icon="inline-start" />
          Drain and stop node
        </Button>
        <Button
          variant="outline"
          disabled={!allowed}
          onClick={() => setConfirmation("preserve")}
        >
          Stop for same-node restart
        </Button>
      </div>
      {!available ? (
        <p className="text-xs text-muted-foreground">
          Inspect a serving node with durable stop reporting to enable these
          controls.
        </p>
      ) : null}
      {stop.error ? (
        <Alert variant="destructive">
          <AlertTitle>Stop acknowledgment was not confirmed</AlertTitle>
          <AlertDescription>
            {stop.error.message} Check the durable receipt below before taking
            another action.
          </AlertDescription>
        </Alert>
      ) : null}
      {tracking ? <StopReceiptView fleet={fleet} target={tracking} /> : null}
      <AlertDialog
        open={confirmation !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmation(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {confirmation === "preserve"
                ? "Stop for same-node restart?"
                : "Drain and stop this node?"}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {confirmation === "preserve"
                ? "Drain admitted work and preserve local ownership for restarting this same node at its existing address. This request does not start a replacement process."
                : "Drain admitted work, release cell ownership for other nodes, and stop this node. Remaining fleet capacity must serve the workload."}
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
                if (confirmation) {
                  const operation = crypto.randomUUID()
                  setTracking({
                    node: node.id,
                    generation: node.generation,
                    actor: session.user,
                    operation,
                  })
                  stop.mutate({ mode: confirmation, operation })
                }
                setConfirmation(null)
              }}
            >
              Confirm node stop
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
