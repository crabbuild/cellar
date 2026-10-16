import { useState } from "react"
import { StreamLifecycle } from "@/components/event-lifecycle"
import { SubscriptionDetails } from "@/components/subscription-details"
import { Link, useParams, useSearchParams } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import { ArrowLeft, ArrowUpRight, Radio, Users, Workflow } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card"
import {
  Table,
  TableHeader,
  TableHead,
  TableRow,
  TableBody,
  TableCell,
} from "@/components/ui/table"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { Input } from "@/components/ui/input"
import { Field, FieldLabel, FieldDescription } from "@/components/ui/field"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectGroup,
  SelectItem,
} from "@/components/ui/select"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet"
import {
  PageHeading,
  Feedback,
  Nothing,
  Refresh,
  Status,
  JsonView,
  CopyValue,
} from "@/components/console-ui"
import {
  EventMetrics,
  EventNotice,
  EventReview,
  EventOutcome,
  EventAudit,
} from "@/components/event-ui"
import { useApi, fleetPath, type Application, type Binding } from "@/lib/api"
import { useSession } from "@/lib/session"
import {
  streamPath,
  useEvent,
  eventRequest,
  exact,
  eventTime,
  age,
  receiptResult,
  type Resource,
  type Stats,
  type Subscription,
  type SubscriptionChange,
  type EventReply,
} from "@/lib/events"

export default function EventStreamWorkspace() {
  const { fleet = "", application = "", binding = "" } = useParams()
  const app = useApi<Application>(
    fleetPath(fleet, `/applications/${encodeURIComponent(application)}`)
  )
  const declaration = app.data?.bindings.find(
    (b) => b.name === binding && b.kind === "event_stream"
  )
  return (
    <div className="page-stack">
      <Link
        className="back-link"
        to={`/fleets/${encodeURIComponent(fleet)}/streams`}
      >
        <ArrowLeft />
        Back to event streams
      </Link>
      <PageHeading
        title={binding}
        description={`${application} · ${declaration?.event?.resource.name ?? "Event stream binding"}`}
      />
      <Feedback loading={app.isPending} error={app.error}>
        {app.data &&
          (declaration?.event ? (
            <ResolvedStream
              key={`${fleet}:${application}:${binding}:${app.data.revision}`}
              fleet={fleet}
              application={application}
              binding={declaration}
            />
          ) : (
            <Nothing
              title="Stream binding unavailable"
              description="This application no longer declares this event stream binding. Refresh the deployed application inventory."
            />
          ))}
      </Feedback>
    </div>
  )
}

function ResolvedStream({
  fleet,
  application,
  binding,
}: {
  fleet: string
  application: string
  binding: Binding
}) {
  const [params, setParams] = useSearchParams()
  const [cursor, setCursor] = useState<string | null>(null)
  const event = binding.event!
  const path = streamPath(fleet, application, binding.name)
  const selected = event.resource.id ?? params.get("resource")
  const catalog = useApi<{
    records: { resource: Resource; state: string }[]
    cursor: string | null
  }>(
    `${path}/resources?limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`,
    !selected
  )
  const candidates =
    catalog.data?.records.filter(
      (r) =>
        r.resource.name === event.resource.name &&
        r.resource.family === event.resource.family
    ) ?? []
  if (
    params.get("resource") &&
    event.resource.id &&
    params.get("resource") !== event.resource.id
  )
    return (
      <Nothing
        title="This binding now targets another stream"
        description="The saved link names an older resource UUID. Open Fleet inspection to find retained metadata and retirement proof; the current binding cannot authorize this older stream."
      />
    )
  if (!selected)
    return (
      <div className="space-y-5">
        <EventNotice>
          This binding selects a stream by name. Resolve the current UUID from
          the catalog before inspecting it. Catalog observations describe
          activation state; the deployed binding’s current grant authorizes
          subsequent operations.
        </EventNotice>
        <Feedback loading={catalog.isPending} error={catalog.error}>
          <Card>
            <CardHeader>
              <CardTitle>Resolve {event.resource.name}</CardTitle>
              <CardDescription>
                {event.resource.tenant} / {event.resource.application} · up to
                50 catalog entries per page
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {candidates.length ? (
                candidates.map((entry) => (
                  <div
                    key={entry.resource.id}
                    className="flex flex-wrap items-center justify-between gap-3 rounded-lg border p-4"
                  >
                    <div>
                      <p className="font-medium">{entry.resource.name}</p>
                      <code className="text-xs text-muted-foreground">
                        {entry.resource.id}
                      </code>
                    </div>
                    <Status label={entry.state} />
                    <Button
                      variant="outline"
                      onClick={() =>
                        setParams((p) => {
                          p.set("resource", entry.resource.id!)
                          return p
                        })
                      }
                    >
                      Inspect stream
                    </Button>
                  </div>
                ))
              ) : (
                <Nothing
                  title="No matching stream on this page"
                  description="Continue through the catalog, or check deployment activation and the declared name. A declaration alone does not prove that a stream is active."
                />
              )}
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  disabled={!cursor}
                  onClick={() => setCursor(null)}
                >
                  First page
                </Button>
                <Button
                  variant="outline"
                  disabled={!catalog.data?.cursor}
                  onClick={() => setCursor(catalog.data!.cursor)}
                >
                  Next catalog page
                </Button>
                <Refresh
                  busy={catalog.isFetching}
                  onClick={() => void catalog.refetch()}
                />
              </div>
            </CardContent>
          </Card>
        </Feedback>
      </div>
    )
  return (
    <StreamView
      key={selected}
      fleet={fleet}
      path={`${path}/action`}
      resource={selected}
      binding={binding}
      onResolve={
        event.resource.id
          ? undefined
          : () =>
              setParams((p) => {
                p.delete("resource")
                return p
              })
      }
    />
  )
}

function StreamView({
  fleet,
  path,
  resource,
  binding,
  onResolve,
}: {
  fleet: string
  path: string
  resource: string
  binding: Binding
  onResolve?: () => void
}) {
  const [params, setParams] = useSearchParams()
  const tab = params.get("view") ?? "overview"
  const event = binding.event!
  const stats = useEvent<Stats>(
    path,
    { kind: "describe" },
    resource,
    event.administer && (tab === "overview" || tab === "lifecycle")
  )
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">
            <Radio />
            Event stream
          </Badge>
          <Badge variant="secondary">{event.resource.family}</Badge>
          <code className="text-xs break-all text-muted-foreground">
            {resource}
          </code>
          <CopyValue value={resource} />
        </div>
        <div className="flex gap-2">
          {onResolve && (
            <Button variant="outline" size="sm" onClick={onResolve}>
              Resolve name again
            </Button>
          )}
          {binding.scope && (
            <Button variant="outline" size="sm" asChild>
              <Link
                to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(binding.scope)}?tab=events`}
              >
                Inspect backing cell
                <ArrowUpRight />
              </Link>
            </Button>
          )}
        </div>
      </div>
      <EventNotice>
        Read pages do not consume or acknowledge events. Payloads are excluded
        until requested. Subscription checkpoints and generations are managed
        through the stream owner; cold inspection may activate existing work.
      </EventNotice>
      <Tabs
        value={tab}
        onValueChange={(view) =>
          setParams((p) => {
            p.set("view", view)
            return p
          })
        }
      >
        <TabsList
          variant="line"
          className="flex h-auto flex-wrap justify-start"
        >
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="events">Events</TabsTrigger>
          <TabsTrigger value="subscriptions">
            <Users />
            Subscriptions
          </TabsTrigger>
          <TabsTrigger value="sources">
            <Workflow />
            Producers
          </TabsTrigger>
          <TabsTrigger value="history">Operation history</TabsTrigger>
          <TabsTrigger value="lifecycle">Lifecycle</TabsTrigger>
        </TabsList>
        <TabsContent value="overview">
          {!event.administer ? (
            <Nothing
              title="Stream administration is not delegated"
              description="This binding may read events, but stream statistics and subscription inventory require its administer capability."
            />
          ) : (
            <Feedback loading={stats.isPending} error={stats.error}>
              {stats.data && (
                <div className="space-y-5">
                  <EventMetrics
                    items={[
                      {
                        label: "Retained events",
                        value: exact(stats.data.rows),
                      },
                      {
                        label: "Retained bytes",
                        value: exact(stats.data.bytes),
                      },
                      {
                        label: "Oldest retained",
                        value: age(
                          stats.data.observation.oldest_retained_age_ms
                        ),
                      },
                      {
                        label: "Active leases",
                        value: exact(stats.data.observation.active_leases),
                      },
                    ]}
                  />
                  <Card>
                    <CardHeader>
                      <CardTitle>Retention and delivery</CardTitle>
                      <CardDescription>
                        Owner observation ·{" "}
                        {eventTime(stats.data.observation.observed_ms)}
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <dl className="detail-grid">
                        {[
                          ["Earliest retained offset", stats.data.earliest],
                          ["End offset (exclusive)", stats.data.end],
                          [
                            "SQLite allocated bytes",
                            stats.data.observation.sqlite_allocated_bytes,
                          ],
                          [
                            "SQLite used bytes",
                            stats.data.observation.sqlite_used_bytes,
                          ],
                          [
                            "Retention frontier",
                            stats.data.observation.retention_frontier,
                          ],
                          [
                            "Protected events",
                            stats.data.observation.protected_records,
                          ],
                          [
                            "Protected bytes",
                            stats.data.observation.protected_bytes,
                          ],
                          [
                            "Retention blocked bytes",
                            stats.data.observation.retention_blocked_bytes,
                          ],
                          [
                            "Expired subscriptions",
                            stats.data.observation.expired_subscriptions,
                          ],
                        ].map(([label, value]) => (
                          <div className="contents" key={label}>
                            <dt>{label}</dt>
                            <dd className="font-mono">{exact(value)}</dd>
                          </div>
                        ))}
                      </dl>
                      <Refresh
                        busy={stats.isFetching}
                        onClick={() => void stats.refetch()}
                      />
                    </CardContent>
                  </Card>
                </div>
              )}
            </Feedback>
          )}
        </TabsContent>
        <TabsContent value="events">
          {event.read ? (
            <EventReader path={path} resource={resource} />
          ) : (
            <Nothing
              title="Event reads are not delegated"
              description="The deployed binding must grant read access to browse event metadata or payloads."
            />
          )}
        </TabsContent>
        <TabsContent value="subscriptions">
          {event.administer ? (
            <Subscriptions fleet={fleet} path={path} resource={resource} />
          ) : (
            <AdminRequired />
          )}
        </TabsContent>
        <TabsContent value="sources">
          {event.administer ? (
            <Producers fleet={fleet} path={path} resource={resource} />
          ) : (
            <AdminRequired />
          )}
        </TabsContent>
        <TabsContent value="history">
          {event.administer ? (
            <EventAudit path={path} resource={resource} />
          ) : (
            <AdminRequired />
          )}
        </TabsContent>
        <TabsContent value="lifecycle">
          <StreamLifecycle
            fleet={fleet}
            path={path}
            resource={resource}
            revision={stats.data?.revision}
            administer={event.administer}
            onChanged={() => void stats.refetch()}
          />
        </TabsContent>
      </Tabs>
    </div>
  )
}

function AdminRequired() {
  return (
    <Nothing
      title="Administration is not delegated"
      description="The deployed stream binding must grant administer access to inspect and manage subscriptions, producers and operation history."
    />
  )
}

type EventRecord = {
  offset: string
  appended_ms?: string
  payload_json?: string
  event: {
    family?: string
    key?: string
    id?: string
    source?: string
    payload?: unknown
    [key: string]: unknown
  }
}
function EventReader({ path, resource }: { path: string; resource: string }) {
  const [cursor, setCursor] = useState<string | null>(null)
  const [input, setInput] = useState("")
  const [payload, setPayload] = useState(false)
  const [selected, setSelected] = useState<EventRecord | null>(null)
  const data = useEvent<{
    records: EventRecord[]
    cursor: string
    earliest: string
    end: string
    next: string
  }>(
    path,
    { kind: "read", cursor, limit: 25, max_bytes: 262144, payload },
    resource
  )
  return (
    <Card>
      <CardHeader>
        <CardTitle>Event browser</CardTitle>
        <CardDescription>
          Up to 25 events / 256 KiB per page. Reading leaves all subscription
          checkpoints unchanged.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <form
          className="flex flex-wrap items-end gap-3"
          onSubmit={(e) => {
            e.preventDefault()
            setCursor(input.trim() || null)
            setPayload(false)
          }}
        >
          <Field className="min-w-40 flex-1">
            <FieldLabel htmlFor="event-cursor">Read cursor</FieldLabel>
            <Input
              id="event-cursor"
              placeholder="Earliest retained event, or paste a v1. cursor"
              value={input}
              onChange={(e) => setInput(e.target.value)}
            />
          </Field>
          <Button variant="outline" type="submit">
            Read page
          </Button>
          <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
        </form>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className="text-xs text-muted-foreground">
            Payload {payload ? "included for this page" : "excluded"} · earliest{" "}
            {exact(data.data?.earliest)} · end {exact(data.data?.end)}
          </p>
          <Button
            variant="outline"
            size="sm"
            disabled={payload || data.isFetching || !data.data?.records.length}
            onClick={() => {
              setSelected(null)
              setPayload(true)
            }}
          >
            Include payloads for this page
          </Button>
        </div>
        <Feedback loading={data.isPending} error={data.error}>
          {data.data &&
            (data.data.records.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Offset</TableHead>
                    <TableHead>Event type</TableHead>
                    <TableHead>Source / key</TableHead>
                    <TableHead>Inspect</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.data.records.map((record) => (
                    <TableRow key={record.offset}>
                      <TableCell className="font-mono">
                        {exact(record.offset)}
                      </TableCell>
                      <TableCell>
                        {record.event.family ?? "Event"}
                        <p
                          className="max-w-64 truncate text-xs text-muted-foreground"
                          title={record.event.id}
                        >
                          {record.event.id}
                        </p>
                      </TableCell>
                      <TableCell>
                        <span
                          className="block max-w-72 truncate"
                          title={record.event.source}
                        >
                          {record.event.source ?? "—"}
                        </span>
                        <span
                          className="block max-w-72 truncate text-xs text-muted-foreground"
                          title={record.event.key}
                        >
                          {record.event.key}
                        </span>
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setSelected(record)}
                        >
                          Inspect event
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Nothing
                title="No retained events at this cursor"
                description="Try the earliest retained page. A cursor before the retention boundary is rejected; no checkpoint is advanced."
              />
            ))}
          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              disabled={!cursor}
              onClick={() => {
                setCursor(null)
                setInput("")
                setPayload(false)
                setSelected(null)
              }}
            >
              Earliest page
            </Button>
            <Button
              variant="outline"
              disabled={!data.data || data.data.next === data.data.end}
              onClick={() => {
                setCursor(data.data!.cursor)
                setInput(data.data!.cursor)
                setPayload(false)
                setSelected(null)
              }}
            >
              Next events
            </Button>
          </div>
        </Feedback>
        {selected && (
          <Sheet
            open
            onOpenChange={(open) => {
              if (!open) setSelected(null)
            }}
          >
            <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
              <SheetHeader>
                <SheetTitle>
                  Event at offset {exact(selected.offset)}
                </SheetTitle>
                <SheetDescription>
                  {payload
                    ? "Payload included in this observation."
                    : "Metadata only. Request payloads from the event browser to include event data."}
                </SheetDescription>
              </SheetHeader>
              <EventRecordView record={selected} />
            </SheetContent>
          </Sheet>
        )}
      </CardContent>
    </Card>
  )
}

function EventRecordView({ record }: { record: EventRecord }) {
  const { payload_json, ...metadata } = record
  return (
    <div className="space-y-4 p-4">
      <JsonView value={metadata} />
      {payload_json !== undefined && (
        <section className="space-y-2">
          <h3 className="text-sm font-medium">Payload · JSON</h3>
          <pre
            className="json-view"
            tabIndex={0}
            aria-label="Event payload JSON"
          >
            {payload_json}
          </pre>
        </section>
      )}
    </div>
  )
}

function Subscriptions({
  fleet,
  path,
  resource,
}: {
  fleet: string
  path: string
  resource: string
}) {
  const { token, session } = useSession()
  const [cursor, setCursor] = useState<string | null>(null)
  const [selected, setSelected] = useState<Subscription | null>(null)
  const [change, setChange] = useState<{
    subscription: Subscription
    kind: "pause" | "resume" | "reset" | "delete"
  } | null>(null)
  const [offset, setOffset] = useState("")
  const [force, setForce] = useState(false)
  const [outcome, setOutcome] = useState<EventReply | null>(null)
  const data = useEvent<{ records: Subscription[]; cursor: string | null }>(
    path,
    { kind: "subscriptions", cursor, limit: 25 },
    resource
  )
  const write = useMutation({
    mutationFn: ({
      action,
      subscription,
      reason,
    }: {
      action: SubscriptionChange
      subscription: Subscription
      reason: string
    }) =>
      eventRequest(
        token,
        path,
        { kind: "change", expected: subscription.identity, action, reason },
        resource
      ),
    onSuccess: (reply) => {
      setOutcome(reply)
      setChange(null)
      setSelected(null)
      void data.refetch()
    },
  })
  const canOperate = session.grants[fleet]?.includes("operate")
  const canReset = canOperate && session.grants[fleet]?.includes("data_write")
  const validOffset =
    /^[1-9]\d{0,18}$/.test(offset) && BigInt(offset) <= 9223372036854775807n
  return (
    <div className="space-y-4">
      <EventOutcome
        reply={outcome}
        error={write.error}
        checkReceipt={(id) =>
          eventRequest(
            token,
            path,
            { kind: "status", operation: id },
            resource
          ).then(receiptResult)
        }
      />
      <Card>
        <CardHeader>
          <CardTitle>Subscriptions</CardTitle>
          <CardDescription>
            Checkpoint, retained lag and lease observations. Inventory includes
            deleted subscription tombstones.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
          <Feedback loading={data.isPending} error={data.error}>
            {data.data &&
              (data.data.records.length ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Subscription</TableHead>
                      <TableHead>State</TableHead>
                      <TableHead className="text-right">
                        Lag / retained
                      </TableHead>
                      <TableHead>Oldest unread</TableHead>
                      <TableHead>Manage</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.data.records.map((sub) => (
                      <TableRow key={sub.identity.id}>
                        <TableCell>
                          <Button
                            variant="link"
                            className="h-auto p-0"
                            onClick={() => setSelected(sub)}
                          >
                            {sub.identity.name}
                          </Button>
                          <p className="text-xs text-muted-foreground">
                            {sub.required ? "Required" : "Optional"} ·
                            checkpoint {exact(sub.checkpoint)}
                          </p>
                        </TableCell>
                        <TableCell>
                          <Status
                            label={sub.state}
                            tone={
                              sub.observation.history_expired
                                ? "warning"
                                : "neutral"
                            }
                          />
                          <p className="text-xs text-muted-foreground">
                            {sub.observation.history_expired
                              ? "History expired"
                              : sub.observation.lease_active
                                ? "Lease active"
                                : "No active lease"}
                          </p>
                        </TableCell>
                        <TableCell className="text-right font-mono">
                          {exact(sub.observation.lag_records)} /{" "}
                          {exact(sub.observation.retained_lag_records)}
                        </TableCell>
                        <TableCell>
                          {age(sub.observation.oldest_retained_age_ms)}
                        </TableCell>
                        <TableCell>
                          <Select
                            value=""
                            onValueChange={(kind) => {
                              setOffset(
                                (BigInt(sub.checkpoint) + 1n).toString()
                              )
                              setForce(false)
                              setChange({
                                subscription: sub,
                                kind: kind as
                                  "pause" | "resume" | "reset" | "delete",
                              })
                            }}
                            disabled={
                              !canOperate ||
                              sub.state === "deleted" ||
                              write.isPending
                            }
                          >
                            <SelectTrigger
                              size="sm"
                              aria-label={`Manage ${sub.identity.name}`}
                            >
                              <SelectValue placeholder="Manage" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectGroup>
                                <SelectItem
                                  value={
                                    sub.state === "paused" ? "resume" : "pause"
                                  }
                                >
                                  {sub.state === "paused" ? "Resume" : "Pause"}
                                </SelectItem>
                                <SelectItem value="reset" disabled={!canReset}>
                                  Reset checkpoint…
                                </SelectItem>
                                <SelectItem value="delete" disabled={!canReset}>
                                  Delete subscription…
                                </SelectItem>
                              </SelectGroup>
                            </SelectContent>
                          </Select>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <Nothing
                  title="No subscriptions on this page"
                  description="Subscriptions are declared and created by the application’s stream configuration."
                />
              ))}
            <div className="flex gap-2">
              <Button
                variant="outline"
                disabled={!cursor}
                onClick={() => setCursor(null)}
              >
                First page
              </Button>
              <Button
                variant="outline"
                disabled={!data.data?.cursor}
                onClick={() => setCursor(data.data!.cursor)}
              >
                Next subscriptions
              </Button>
            </div>
          </Feedback>
        </CardContent>
      </Card>
      {selected && (
        <Sheet
          open
          onOpenChange={(open) => {
            if (!open) setSelected(null)
          }}
        >
          <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
            <SheetHeader>
              <SheetTitle>{selected.identity.name}</SheetTitle>
              <SheetDescription>
                Observed {eventTime(selected.observation.observed_ms)}. Lease
                summaries do not expose acknowledgment tokens.
              </SheetDescription>
            </SheetHeader>
            <SubscriptionDetails
              path={path}
              resource={resource}
              selected={selected}
            />
          </SheetContent>
        </Sheet>
      )}
      {change && (
        <EventReview
          key={`${change.subscription.identity.id}:${change.kind}`}
          title={`${change.kind[0].toUpperCase() + change.kind.slice(1)} ${change.subscription.identity.name}?`}
          description={
            change.kind === "reset"
              ? "Reset changes the checkpoint and rotates the subscription generation. Moving forward can skip events; moving backward replays retained history. Active leases can reject this request."
              : change.kind === "delete"
                ? "Deletion retains a tombstone and can release required retention protection. An active lease rejects deletion unless force is explicitly selected."
                : "The inspected subscription UUID and generation guard this change. Refresh after a conflict; do not repeat an operation whose outcome is unknown."
          }
          pending={write.isPending}
          valid={change.kind !== "reset" || validOffset}
          onCancel={() => setChange(null)}
          onConfirm={(reason) => {
            if (change.kind === "reset" && !validOffset) return
            const action: SubscriptionChange =
              change.kind === "reset"
                ? {
                    kind: "reset",
                    cursor: { resource, partition: 0, next: offset },
                  }
                : change.kind === "delete"
                  ? { kind: "delete", force }
                  : { kind: change.kind }
            write.mutate({ subscription: change.subscription, action, reason })
          }}
        >
          {change.kind === "reset" && (
            <Field data-invalid={!validOffset}>
              <FieldLabel htmlFor="reset-offset">Next event offset</FieldLabel>
              <Input
                id="reset-offset"
                value={offset}
                onChange={(e) => setOffset(e.target.value)}
                inputMode="numeric"
                aria-invalid={!validOffset}
              />
              <FieldDescription>
                Positive 64-bit integer. Observed retained range [
                {change.subscription.earliest}, {change.subscription.end}).{" "}
                {validOffset ? "" : "Enter a valid offset before applying."}
              </FieldDescription>
            </Field>
          )}
          {change.kind === "delete" && (
            <Field orientation="horizontal">
              <Checkbox
                id="force-delete"
                checked={force}
                onCheckedChange={(checked) => setForce(checked === true)}
              />
              <FieldLabel htmlFor="force-delete">
                Force deletion even with an active lease
              </FieldLabel>
            </Field>
          )}
        </EventReview>
      )}
    </div>
  )
}

function Producers({
  fleet,
  path,
  resource,
}: {
  fleet: string
  path: string
  resource: string
}) {
  const [cursor, setCursor] = useState<string | null>(null)
  const data = useEvent<{
    records: {
      lane: string
      source: string
      incarnation: string
      accepted_through: string
      sealed: boolean
    }[]
    cursor: string | null
  }>(path, { kind: "sources", cursor, limit: 25 }, resource)
  return (
    <Card>
      <CardHeader>
        <CardTitle>Observed producers</CardTitle>
        <CardDescription>
          Destination-accepted watermarks. This inventory cannot reveal
          undispatched source events or prove that delivery has drained.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
        <Feedback loading={data.isPending} error={data.error}>
          {data.data &&
            (data.data.records.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Source cell</TableHead>
                    <TableHead>Accepted through</TableHead>
                    <TableHead>Seal</TableHead>
                    <TableHead>Inspect</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.data.records.map((source) => (
                    <TableRow key={source.lane}>
                      <TableCell>
                        <Link
                          className="text-link block max-w-96 truncate font-mono text-xs"
                          title={source.source}
                          to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(source.source)}?tab=outbox`}
                        >
                          {source.source}
                        </Link>
                        <p className="mt-1 text-xs text-muted-foreground">
                          Incarnation {source.incarnation}
                        </p>
                      </TableCell>
                      <TableCell className="font-mono">
                        {exact(source.accepted_through)}
                      </TableCell>
                      <TableCell>
                        <Status label={source.sealed ? "Sealed" : "Open"} />
                      </TableCell>
                      <TableCell>
                        <Button variant="outline" size="sm" asChild>
                          <Link
                            to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(source.source)}?tab=outbox`}
                          >
                            Source outbox
                            <ArrowUpRight />
                          </Link>
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Nothing
                title="No accepted producers on this page"
                description="A producer appears after this destination accepts its first event."
              />
            ))}
          <div className="flex gap-2">
            <Button
              variant="outline"
              disabled={!cursor}
              onClick={() => setCursor(null)}
            >
              First page
            </Button>
            <Button
              variant="outline"
              disabled={!data.data?.cursor}
              onClick={() => setCursor(data.data!.cursor)}
            >
              Next producers
            </Button>
          </div>
        </Feedback>
      </CardContent>
    </Card>
  )
}
