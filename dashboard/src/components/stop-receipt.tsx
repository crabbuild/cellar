import { useQuery } from "@tanstack/react-query"
import { CheckCircle2, Clock3 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Status } from "@/components/console-ui"
import { date, fleetPath, request } from "@/lib/api"
import { useSession } from "@/lib/session"

export type StopLocator = {
  node: string
  generation: string
  actor: string
  operation: string
}
type Proof = {
  mode: "handoff" | "preserve"
  ingress_drained: boolean
  application_work_drained: boolean
  pending_application_events: number
  cells_drained: boolean
  pending_dispatches: number
  log_quiesced: boolean
  local_durability_closed: boolean
  clean_reload_prepared: boolean
  drain_timed_out: boolean
  deadline_elapsed: boolean
}
type Receipt = {
  phase: string
  requested_mode: "handoff" | "preserve"
  observed_at: number
  domain_joined: boolean
  proof: Proof | null
  error: string | null
}

export function StopReceiptView({
  fleet,
  target,
}: {
  fleet: string
  target: StopLocator
}) {
  const { token } = useSession()
  const path = fleetPath(
    fleet,
    `/nodes/${encodeURIComponent(target.node)}/stops/${encodeURIComponent(target.generation)}/${encodeURIComponent(target.actor)}/${encodeURIComponent(target.operation)}`
  )
  const receipt = useQuery({
    queryKey: [path],
    queryFn: () => request<{ receipt: Receipt | null }>(token, path),
    refetchInterval: (query) =>
      query.state.data?.receipt?.domain_joined ? false : 3000,
  })
  const observed = receipt.data?.receipt
  const proof = observed?.proof
  const phases = [
    ["Ingress drained", proof?.ingress_drained],
    ["Application work drained", proof?.application_work_drained],
    ["Cells drained", proof?.cells_drained],
    ["Node log quiesced", proof?.log_quiesced],
    ["Local durability closed", proof?.local_durability_closed],
    ["Owned work joined", observed?.domain_joined],
  ] as const
  return (
    <section aria-label="Node stop proof" className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Status
          label={observed?.phase.replaceAll("_", " ") ?? "Awaiting receipt"}
          tone={observed?.phase === "complete" && observed.domain_joined ? "good" : "warning"}
        />
        <Button
          size="sm"
          variant="outline"
          onClick={() => void receipt.refetch()}
          disabled={receipt.isFetching}
        >
          Refresh receipt
        </Button>
      </div>
      <p className="text-xs break-all text-muted-foreground">
        {target.node} · generation {target.generation}
        <br />
        Operation {target.operation}
      </p>
      {receipt.error ? (
        <Alert variant="destructive">
          <AlertTitle>Receipt could not be refreshed</AlertTitle>
          <AlertDescription>
            {receipt.error.message} The last observation remains below; this
            does not establish the stop outcome.
          </AlertDescription>
        </Alert>
      ) : null}
      {observed ? (
        <>
          <div className="grid gap-3 sm:grid-cols-2">
            {phases.map(([label, complete]) => (
              <div
                className="flex items-center gap-2 rounded-md border p-3 text-sm"
                key={label}
              >
                {complete ? (
                  <CheckCircle2 className="size-4 text-emerald-600" />
                ) : (
                  <Clock3 className="size-4 text-muted-foreground" />
                )}
                <div>
                  {label}
                  <p className="text-xs text-muted-foreground">
                    {complete ? "Confirmed" : "Not confirmed"}
                  </p>
                </div>
              </div>
            ))}
          </div>
          {proof ? (
            <p className="text-sm text-muted-foreground">
              Pending application events: {proof.pending_application_events}.
              Pending dispatches: {proof.pending_dispatches}.
              {proof.drain_timed_out || proof.deadline_elapsed
                ? " The shutdown budget expired with incomplete proof."
                : ""}
              {observed.requested_mode === "preserve"
                ? ` Restart preparation: ${proof.clean_reload_prepared ? "confirmed" : "not confirmed"}.`
                : ""}
            </p>
          ) : (
            <p className="text-sm text-muted-foreground">
              The node has not yet published its final drain proof.
            </p>
          )}
          {observed.error ? (
            <Alert variant="destructive">
              <AlertTitle>Stop did not complete normally</AlertTitle>
              <AlertDescription>{observed.error}</AlertDescription>
            </Alert>
          ) : null}
          <p className="text-xs text-muted-foreground">
            Observed {date(observed.observed_at)}. This receipt remains
            available after the node listener closes.
          </p>
        </>
      ) : (
        <p className="text-sm text-muted-foreground">
          No durable receipt has been observed yet. A lost reply or unreachable
          node leaves the outcome unknown. This view only checks receipts; it
          does not repeat the stop request.
        </p>
      )}
    </section>
  )
}
