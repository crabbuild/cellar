import { useState, type ReactNode } from "react"
import { cn } from "cn"
import { useSearchParams } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import { AlertCircle, CheckCircle2 } from "lucide-react"
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card"
import { Field, FieldLabel, FieldDescription } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogCancel,
} from "@/components/ui/alert-dialog"
import {
  Feedback,
  JsonView,
  Nothing,
  Status,
  CopyValue,
} from "@/components/console-ui"
import { useSession } from "@/lib/session"
import {
  eventRequest,
  type EventReply,
  type AuditPage,
  type StreamAction,
  type OutboxAction,
  exact,
} from "@/lib/events"

export function EventMetrics({
  items,
}: {
  items: { label: string; value: ReactNode; detail?: string }[]
}) {
  return (
    <dl
      className={cn(
        "grid gap-3 sm:grid-cols-2",
        items.length > 2 && "xl:grid-cols-4"
      )}
    >
      {items.map((item) => (
        <div className="min-w-0 rounded-lg border bg-card p-5" key={item.label}>
          <dt className="text-sm text-muted-foreground">{item.label}</dt>
          <dd className="mt-2 font-mono text-2xl font-medium tracking-tight break-words">
            {item.value}
          </dd>
          {item.detail && (
            <dd className="mt-1 text-xs text-muted-foreground">
              {item.detail}
            </dd>
          )}
        </div>
      ))}
    </dl>
  )
}

export function EventNotice({ children }: { children: ReactNode }) {
  return (
    <p className="rounded-lg border bg-muted/30 px-4 py-3 text-sm leading-relaxed text-muted-foreground">
      {children}
    </p>
  )
}

export function EventReview({
  title,
  description,
  children,
  pending,
  onCancel,
  onConfirm,
  valid = true,
}: {
  title: string
  description: string
  children?: ReactNode
  pending: boolean
  valid?: boolean
  onCancel: () => void
  onConfirm: (reason: string) => void
}) {
  const [reason, setReason] = useState("")
  return (
    <AlertDialog
      open
      onOpenChange={(open) => {
        if (!open && !pending) onCancel()
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        {children}
        <Field>
          <FieldLabel htmlFor="event-reason">Reason</FieldLabel>
          <Textarea
            id="event-reason"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            maxLength={512}
            placeholder="Explain why this change is needed"
          />
          <FieldDescription>
            Recorded with your operator identity in the durable receipt. Maximum
            512 UTF-8 bytes.
          </FieldDescription>
        </Field>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={pending}>Cancel</AlertDialogCancel>
          <Button
            disabled={
              pending ||
              !valid ||
              !reason.trim() ||
              new TextEncoder().encode(reason).length > 512
            }
            onClick={() => onConfirm(reason)}
          >
            {pending ? "Applying…" : "Apply change"}
          </Button>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}

export function EventOutcome({
  reply,
  error,
  checkReceipt,
}: {
  reply?: EventReply | null
  error?: Error | null
  checkReceipt: (id: string) => Promise<unknown>
}) {
  const check = useMutation({ mutationFn: checkReceipt })
  if (!reply && !error) return null
  const id = reply?.operation?.id ?? reply?.operation_id
  const verified = check.isSuccess && check.variables === id
  const unknown =
    !verified &&
    (reply?.failure ||
      (reply?.operation && reply.operation.status !== "succeeded"))
  return (
    <div className="space-y-3" aria-live="polite">
      <Alert
        variant={
          !verified && (error || reply?.failure) ? "destructive" : "default"
        }
      >
        {unknown || error ? <AlertCircle /> : <CheckCircle2 />}
        <AlertTitle>
          {unknown
            ? "Outcome needs verification"
            : error
              ? "Request could not be verified"
              : "Change confirmed"}
        </AlertTitle>
        <AlertDescription>
          {verified
            ? "The owning cell confirmed the durable receipt."
            : (error?.message ??
              reply?.failure ??
              (reply?.replayed
                ? "This operation was already recorded. It was not dispatched again."
                : "The owning cell returned a durable receipt."))}
          {id && (
            <div className="mt-2 flex flex-wrap items-center gap-2">
              <code className="break-all">{id}</code>
              <CopyValue value={id} />
              <Status
                label={
                  verified
                    ? "succeeded"
                    : (reply?.operation?.status ?? "outcome_unknown")
                }
              />
              <Button
                variant="outline"
                size="sm"
                disabled={check.isPending}
                onClick={() => check.mutate(id)}
              >
                Check receipt
              </Button>
            </div>
          )}
        </AlertDescription>
      </Alert>
      {check.error && check.variables === id && (
        <Feedback error={check.error}>{null}</Feedback>
      )}
      {check.data !== undefined && check.variables === id && (
        <JsonView value={check.data} />
      )}
    </div>
  )
}

export function EventAudit({
  path,
  resource,
  source = false,
}: {
  path: string
  resource?: string
  source?: boolean
}) {
  const { token } = useSession()
  const [params] = useSearchParams()
  const [operation, setOperation] = useState(params.get("operation") ?? "")
  const lookup = useMutation({
    mutationFn: (id: string) =>
      eventRequest(
        token,
        path,
        { kind: "status", operation: id },
        resource
      ).then((reply) => {
        if (reply.result === undefined)
          throw new Error(reply.failure ?? "Receipt is unknown")
        return reply.result
      }),
  })
  const [after, setAfter] = useState("0")
  const read = useMutation({
    mutationFn: (position: string) =>
      eventRequest<AuditPage>(
        token,
        path,
        { kind: "audit", after: position, limit: 25 } as
          OutboxAction | StreamAction,
        resource
      ).then((r) => {
        if (!r.result) throw new Error(r.failure ?? "No audit page returned")
        return r.result
      }),
  })
  return (
    <Card>
      <CardHeader>
        <CardTitle>Durable operation history</CardTitle>
        <CardDescription>
          Receipts recorded by this {source ? "source cell" : "stream"}. Each
          page contains at most 25 operations.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-3 rounded-lg border p-4">
          <p className="text-sm text-muted-foreground">
            Recover a write after leaving or reloading this workspace. Use your
            operation ID from the console’s Operations page. Looking up a
            receipt never repeats the write.
          </p>
          <form
            className="flex flex-wrap items-end gap-3"
            onSubmit={(event) => {
              event.preventDefault()
              lookup.mutate(operation.trim())
            }}
          >
            <Field className="min-w-48 flex-1">
              <FieldLabel htmlFor="event-receipt-id">Operation ID</FieldLabel>
              <Input
                id="event-receipt-id"
                value={operation}
                onChange={(event) => setOperation(event.target.value)}
                maxLength={32}
                placeholder="32 lowercase hexadecimal digits"
              />
            </Field>
            <Button
              type="submit"
              variant="outline"
              disabled={
                lookup.isPending || !/^[a-f0-9]{32}$/.test(operation.trim())
              }
            >
              Look up receipt
            </Button>
          </form>
          {lookup.variables === operation.trim() && (
            <Feedback loading={lookup.isPending} error={lookup.error}>
              {lookup.isSuccess && (
                <>
                  <Status label="Durable receipt confirmed" tone="good" />
                  <JsonView value={lookup.data} />
                </>
              )}
            </Feedback>
          )}
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <Field className="max-w-56">
            <FieldLabel htmlFor="audit-after">After sequence</FieldLabel>
            <Input
              id="audit-after"
              value={after}
              onChange={(e) => setAfter(e.target.value)}
              inputMode="numeric"
            />
          </Field>
          <Button
            variant="outline"
            disabled={read.isPending || !/^(0|[1-9]\d{0,18})$/.test(after)}
            onClick={() => read.mutate(after)}
          >
            Load history
          </Button>
        </div>
        <Feedback loading={read.isPending} error={read.error}>
          {read.data &&
            (read.data.records.length ? (
              <div className="divide-y rounded-lg border">
                {read.data.records.map((record) => (
                  <details key={record.sequence} className="p-4">
                    <summary className="cursor-pointer text-sm font-medium">
                      Operation · {exact(record.sequence)}
                    </summary>
                    <div className="mt-3">
                      <JsonView value={record.receipt} />
                    </div>
                  </details>
                ))}
              </div>
            ) : (
              <Nothing
                title="No operations on this page"
                description="Durable history appears after an operator change is applied."
              />
            ))}
          {read.data?.has_more !== false &&
            (read.data?.next ?? read.data?.next_after) && (
              <Button
                variant="outline"
                onClick={() => {
                  const next = (read.data?.next ?? read.data?.next_after)!
                  setAfter(next)
                  read.mutate(next)
                }}
              >
                Next history page
              </Button>
            )}
        </Feedback>
      </CardContent>
    </Card>
  )
}
