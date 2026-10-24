// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { LogOut } from "lucide-react"
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
import { JsonView } from "@/components/console-ui"
import { fleetPath, request, useApi, type Cell, type Node } from "@/lib/api"
import { useSession } from "@/lib/session"

export function CellControls({ fleet, cell }: { fleet: string; cell: Cell }) {
  const { token, session } = useSession()
  const cache = useQueryClient()
  const owner = useApi<Node>(
    fleetPath(fleet, `/nodes/${encodeURIComponent(cell.owner ?? "")}`),
    !!cell.owner
  )
  const [target, setTarget] = useState<{
    node: string
    generation: string
    scope: string
    epoch: number
  } | null>(null)
  const mutation = useMutation({
    mutationFn: (selected: NonNullable<typeof target>) =>
      request<{ result: unknown; replayed?: boolean }>(
        token,
        fleetPath(fleet, `/nodes/${encodeURIComponent(selected.node)}/action`),
        {
          method: "POST",
          body: JSON.stringify({
            generation: selected.generation,
            action: { evict: { scope: selected.scope, epoch: selected.epoch } },
            operation_id: crypto.randomUUID(),
          }),
        }
      ),
    onSuccess: (reply) => {
      toast.success(
        reply.replayed
          ? "Operation already recorded; inspect Operations for its outcome"
          : "Eviction completed; later requests may reactivate the cell"
      )
      void cache.invalidateQueries({
        queryKey: [
          fleetPath(fleet, `/cells/${encodeURIComponent(cell.scope)}`),
        ],
      })
      void cache.invalidateQueries({
        queryKey: [fleetPath(fleet, "/operations")],
      })
    },
    onError: (error: Error) => toast.error(error.message),
  })
  const canOperate = session.grants[fleet]?.includes("operate") ?? false
  return (
    <Card className="mt-5">
      <CardHeader>
        <CardTitle>Cell lifecycle</CardTitle>
        <CardDescription>
          Release the active runtime after its durability checks pass. Stored
          data remains available, and a later request can activate the cell
          again.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <Button
          variant="outline"
          disabled={
            !canOperate ||
            mutation.isPending ||
            !owner.data?.lease_current ||
            cell.epoch == null ||
            !!cell.error
          }
          onClick={() => {
            if (owner.data && cell.epoch != null)
              setTarget({
                node: owner.data.id,
                generation: owner.data.generation,
                scope: cell.scope,
                epoch: cell.epoch,
              })
          }}
        >
          <LogOut data-icon="inline-start" />
          Evict cell
        </Button>
        {owner.error ? (
          <p className="text-sm text-muted-foreground">
            Owner unavailable: {owner.error.message}
          </p>
        ) : null}
        {mutation.error ? (
          <Alert variant="destructive">
            <AlertTitle>Eviction was not confirmed</AlertTitle>
            <AlertDescription>
              {mutation.error.message}. Refresh ownership before acting again. A
              lost reply may leave an unknown outcome.
            </AlertDescription>
          </Alert>
        ) : null}
        {mutation.data ? <JsonView value={mutation.data} /> : null}
        <AlertDialog
          open={target !== null}
          onOpenChange={(open) => {
            if (!open) setTarget(null)
          }}
        >
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Evict this cell?</AlertDialogTitle>
              <AlertDialogDescription>
                The owner checks the exact epoch and waits for eviction to
                finish. Active work or pending alarms may prevent eviction. This
                does not delete data or force a move to another node.
                <br />
                Fleet: {fleet}
                <br />
                Cell: {target?.scope}
                <br />
                Epoch: {target?.epoch}
                <br />
                Owner: {target?.node}
                <br />
                Node generation: {target?.generation}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={() => {
                  if (target) mutation.mutate(target)
                  setTarget(null)
                }}
              >
                Confirm eviction
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </CardContent>
    </Card>
  )
}
