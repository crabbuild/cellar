// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import { useSearchParams } from "react-router-dom"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card"
import {
  Field,
  FieldGroup,
  FieldLabel,
  FieldDescription,
} from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Checkbox } from "@/components/ui/checkbox"
import { Feedback, JsonView } from "@/components/console-ui"
import {
  EventReview,
  EventOutcome,
  EventAudit,
  EventNotice,
} from "@/components/event-ui"
import { useSession } from "@/lib/session"
import {
  cellEventsPath,
  eventRequest,
  receiptResult,
  type EventReply,
  type OutboxAction,
  type StreamAction,
} from "@/lib/events"

const bounded = (value: string, max: number) =>
  value.trim().length > 0 && new TextEncoder().encode(value).length <= max

export function SourceLifecycle({
  fleet,
  scope,
  incarnation,
  onChanged,
}: {
  fleet: string
  scope: string
  incarnation: string | null
  onChanged: () => void
}) {
  const { token, session } = useSession()
  const path = `${cellEventsPath(fleet, scope)}/outbox`
  const [kind, setKind] = useState<"destroy" | "recreate" | null>(null)
  const [expected, setExpected] = useState("")
  const [confirmation, setConfirmation] = useState("")
  const [loss, setLoss] = useState("")
  const [outcome, setOutcome] = useState<EventReply | null>(null)
  const allowed =
    session.grants[fleet]?.includes("operate") &&
    session.grants[fleet]?.includes("data_write")
  const write = useMutation({
    mutationFn: (action: OutboxAction) => eventRequest(token, path, action),
    onSuccess: (reply) => {
      setOutcome(reply)
      setKind(null)
      onChanged()
    },
  })
  function review(action: "destroy" | "recreate") {
    setExpected(incarnation ?? "")
    setConfirmation("")
    setLoss("")
    setKind(action)
  }
  return (
    <div className="flex flex-col gap-5">
      <EventOutcome
        reply={outcome}
        error={write.error}
        checkReceipt={(id) =>
          eventRequest(token, path, { kind: "status", operation: id }).then(
            receiptResult
          )
        }
      />
      <Card>
        <CardHeader>
          <CardTitle>Source lifecycle</CardTitle>
          <CardDescription>
            Manage this cell as an event source. Destruction erases its private
            business state; recreation opens an empty producer incarnation.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <EventNotice>
            Destruction covers this cell’s private KV, SQL, inbox, outbox and
            registered facet state. Submitting immediately closes business
            admission. Cleanup requires terminal destination retirement evidence
            and records discarded obligations. Accepted closure can still be
            checking or cleaning; a deleted receipt confirms completion.
          </EventNotice>
          <dl className="detail-grid">
            <dt>Observed incarnation</dt>
            <dd className="font-mono break-all">
              {incarnation ?? "Not observed; inspect delivery lanes first"}
            </dd>
          </dl>
          {!allowed && (
            <p className="text-sm text-muted-foreground">
              Source lifecycle changes require data write and operate
              permissions.
            </p>
          )}
          <div className="flex flex-wrap gap-3">
            <Button
              variant="destructive"
              disabled={!allowed || write.isPending || !incarnation}
              onClick={() => review("destroy")}
            >
              Destroy source…
            </Button>
            <Button
              variant="outline"
              disabled={!allowed || write.isPending || !incarnation}
              onClick={() => review("recreate")}
            >
              Recreate deleted source…
            </Button>
          </div>
        </CardContent>
      </Card>
      {kind && (
        <EventReview
          title={
            kind === "destroy"
              ? "Destroy source state?"
              : "Recreate deleted source?"
          }
          description={
            kind === "destroy"
              ? "Submitting closes business admission immediately and fences existing handlers and handles. Cleanup waits for terminal destination retirement evidence, then permanently removes business state. This action cannot be undone."
              : "The backend requires completed destruction. Recreation starts empty with a fresh producer incarnation; retired state is not restored."
          }
          pending={write.isPending}
          valid={
            /^[a-f0-9]{32}$/.test(expected) &&
            confirmation === expected &&
            (kind !== "destroy" || bounded(loss, 1024))
          }
          onCancel={() => setKind(null)}
          onConfirm={(reason) =>
            write.mutate(
              kind === "destroy"
                ? {
                    kind,
                    incarnation: expected,
                    reason,
                    data_loss_acknowledgment: loss,
                  }
                : { kind: "recreate", incarnation: expected, reason }
            )
          }
        >
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="source-incarnation">
                Source incarnation
              </FieldLabel>
              <Input id="source-incarnation" value={expected} readOnly />
            </Field>
            <Field>
              <FieldLabel htmlFor="source-confirm">
                Type the source incarnation to confirm
              </FieldLabel>
              <Input
                id="source-confirm"
                value={confirmation}
                onChange={(e) => setConfirmation(e.target.value)}
                autoComplete="off"
              />
            </Field>
            {kind === "destroy" && (
              <Field>
                <FieldLabel htmlFor="source-loss">
                  Data loss acknowledgment
                </FieldLabel>
                <Textarea
                  id="source-loss"
                  value={loss}
                  onChange={(e) => setLoss(e.target.value)}
                  maxLength={1024}
                />
                <FieldDescription>
                  Describe the business data and pending obligations you accept
                  losing. Maximum 1,024 UTF-8 bytes.
                </FieldDescription>
              </Field>
            )}
          </FieldGroup>
        </EventReview>
      )}
    </div>
  )
}

export function StreamLifecycle({
  fleet,
  path,
  resource,
  revision,
  administer,
  onChanged,
}: {
  fleet: string
  path: string
  resource: string
  revision?: string
  administer: boolean
  onChanged: () => void
}) {
  const { token, session } = useSession()
  const [params] = useSearchParams()
  const [review, setReview] = useState(false)
  const [captured, setCaptured] = useState("")
  const [force, setForce] = useState(false)
  const [attestation, setAttestation] = useState("")
  const [confirmation, setConfirmation] = useState("")
  const [outcome, setOutcome] = useState<EventReply | null>(null)
  const allowed =
    administer &&
    session.grants[fleet]?.includes("operate") &&
    session.grants[fleet]?.includes("data_write")
  const write = useMutation({
    mutationFn: (action: StreamAction) =>
      eventRequest(token, path, action, resource),
    onSuccess: (reply) => {
      setOutcome(reply)
      setReview(false)
      onChanged()
    },
  })
  const proof = useMutation({
    mutationFn: () =>
      eventRequest(token, path, { kind: "retirement_proof" }, resource).then(
        receiptResult
      ),
  })
  return (
    <div className="flex flex-col gap-5">
      <EventOutcome
        reply={outcome}
        error={write.error}
        checkReceipt={(id) =>
          eventRequest(
            token,
            path,
            { kind: "retirement_status", operation: id },
            resource
          ).then(receiptResult)
        }
      />
      <Card>
        <CardHeader>
          <CardTitle>Retire stream</CardTitle>
          <CardDescription>
            Close this exact stream UUID through its catalog owner. The observed
            configuration revision guards the request.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <EventNotice>
            Coordinated retirement requires an external procedure that quiesces
            every producer, including undispatched sources. The producer
            inventory alone cannot prove this. Forced retirement can destroy
            retained history and permanently block pending source obligations.
          </EventNotice>
          <dl className="detail-grid">
            <dt>Configuration revision</dt>
            <dd>{revision ?? "No active stream observation"}</dd>
          </dl>
          {!allowed && (
            <p className="text-sm text-muted-foreground">
              Retirement requires stream administration, data write and operate
              permissions.
            </p>
          )}
          <Button
            className="self-start"
            variant="destructive"
            disabled={!allowed || !revision || write.isPending}
            onClick={() => {
              setCaptured(revision!)
              setForce(false)
              setAttestation("")
              setConfirmation("")
              setReview(true)
            }}
          >
            Retire stream…
          </Button>
        </CardContent>
      </Card>
      {administer && (
        <EventAudit
          key={params.get("operation") ?? "retirement"}
          path={path}
          resource={resource}
          retirement
        />
      )}
      <Card>
        <CardHeader>
          <CardTitle>Terminal retirement proof</CardTitle>
          <CardDescription>
            Read platform evidence for this exact resource even after resource
            grants are revoked. A successful proof confirms terminal retirement,
            not producer drainage.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <Button
            variant="outline"
            className="self-start"
            disabled={proof.isPending}
            onClick={() => proof.mutate()}
          >
            Verify terminal retirement
          </Button>
          <Feedback loading={proof.isPending} error={proof.error}>
            {proof.data != null && <JsonView value={proof.data} />}
          </Feedback>
        </CardContent>
      </Card>
      {review && (
        <EventReview
          title="Retire this stream?"
          description={`Stream ${resource} · captured revision ${captured}. This permanently closes this UUID. Name reuse creates a different resource.`}
          pending={write.isPending}
          valid={confirmation === resource && bounded(attestation, 512)}
          onCancel={() => setReview(false)}
          onConfirm={(reason) =>
            write.mutate({
              kind: "retire",
              revision: captured,
              reason,
              mode: force
                ? { mode: "force", data_loss_acknowledgment: attestation }
                : { mode: "coordinated", procedure: attestation },
            })
          }
        >
          <FieldGroup>
            <Field orientation="horizontal">
              <Checkbox
                id="force-retire"
                checked={force}
                onCheckedChange={(value) => {
                  setForce(value === true)
                  setAttestation("")
                }}
              />
              <FieldLabel htmlFor="force-retire">
                Force retirement and accept data loss
              </FieldLabel>
            </Field>
            <Field>
              <FieldLabel htmlFor="retire-attestation">
                {force
                  ? "Data loss acknowledgment"
                  : "Producer coordination procedure"}
              </FieldLabel>
              <Textarea
                id="retire-attestation"
                value={attestation}
                maxLength={512}
                onChange={(e) => setAttestation(e.target.value)}
              />
              <FieldDescription>
                Recorded in the retirement request. Maximum 512 UTF-8 bytes.
              </FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="retire-confirm">
                Type the stream UUID to confirm
              </FieldLabel>
              <Input
                id="retire-confirm"
                value={confirmation}
                onChange={(e) => setConfirmation(e.target.value)}
                autoComplete="off"
              />
            </Field>
          </FieldGroup>
        </EventReview>
      )}
    </div>
  )
}
