import { useState } from "react"
import { Link, useSearchParams } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import { Button } from "@/components/ui/button"
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
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet"
import {
  Feedback,
  Nothing,
  Status,
  JsonView,
  Refresh,
} from "@/components/console-ui"
import {
  EventMetrics,
  EventNotice,
  EventReview,
  EventOutcome,
  EventAudit,
} from "@/components/event-ui"
import { useApi, fleetPath, type Node } from "@/lib/api"
import { useSession } from "@/lib/session"
import {
  cellEventsPath,
  useEvent,
  eventRequest,
  newOperation,
  exact,
  age,
  eventTime,
  type Outbox,
  type Lane,
  type EventReply,
} from "@/lib/events"

export function CellOutbox({ fleet, scope }: { fleet: string; scope: string }) {
  const { token, session } = useSession()
  const [params] = useSearchParams()
  const [view, setView] = useState(
    params.has("operation") ? "history" : "lanes"
  )
  const [cursor, setCursor] = useState<string | null>(null)
  const [selected, setSelected] = useState<Lane | null>(null)
  const [retry, setRetry] = useState<Lane | null>(null)
  const [outcome, setOutcome] = useState<EventReply | null>(null)
  const path = `${cellEventsPath(fleet, scope)}/outbox`
  const page = useEvent<Outbox>(
    path,
    { kind: "inspect", limit: 25, cursor },
    undefined,
    view === "lanes"
  )
  const write = useMutation({
    mutationFn: ({ lane, reason }: { lane: Lane; reason: string }) =>
      eventRequest(
        token,
        path,
        { kind: "retry", expected: lane.revision, reason },
        undefined,
        newOperation()
      ),
    onSuccess: (reply) => {
      setOutcome(reply)
      setRetry(null)
      void page.refetch()
    },
  })
  const canOperate = session.grants[fleet]?.includes("operate")
  const data = page.data
  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold">Transactional outbox</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Events committed by this cell and awaiting delivery to their
            destination streams.
          </p>
        </div>
        <Refresh busy={page.isFetching} onClick={() => void page.refetch()} />
      </div>
      <EventNotice>
        Inspection can activate a cold cell and progress existing work. Counts
        cover this source incarnation; lane pages are live observations. Retry
        advances dispatch eligibility and preserves event order.
      </EventNotice>
      <EventOutcome
        reply={outcome}
        error={write.error}
        checkReceipt={(id) =>
          eventRequest(token, path, { kind: "status", operation: id }).then(
            (r) => r.result
          )
        }
      />
      <Tabs value={view} onValueChange={setView}>
        <TabsList variant="line">
          <TabsTrigger value="lanes">Delivery lanes</TabsTrigger>
          <TabsTrigger value="history">Operation history</TabsTrigger>
        </TabsList>
        <TabsContent value="lanes">
          <Feedback loading={page.isPending} error={page.error}>
            {data && (
              <div className="space-y-5">
                <EventMetrics
                  items={[
                    {
                      label: "Pending events",
                      value: exact(data.pending_rows),
                    },
                    {
                      label: "Pending bytes",
                      value: exact(data.pending_bytes),
                    },
                    {
                      label: "Oldest pending",
                      value: age(data.observation.oldest_pending_age_ms),
                    },
                    {
                      label: "Blocked lanes",
                      value: exact(
                        data.observation.blocked_lanes
                          .reduce((sum, b) => sum + BigInt(b.lanes), 0n)
                          .toString()
                      ),
                    },
                  ]}
                />
                <Card>
                  <CardHeader>
                    <CardTitle>Delivery lanes</CardTitle>
                    <CardDescription>
                      Observed {eventTime(data.observation.observed_ms)} ·{" "}
                      {data.lanes.length} lanes on this page
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {data.lanes.length ? (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Destination</TableHead>
                            <TableHead>Delivery</TableHead>
                            <TableHead className="text-right">
                              Pending
                            </TableHead>
                            <TableHead>Oldest</TableHead>
                            <TableHead>Actions</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {data.lanes.map((lane) => (
                            <TableRow key={lane.revision.lane}>
                              <TableCell>
                                <Button
                                  variant="link"
                                  className="h-auto p-0"
                                  onClick={() => setSelected(lane)}
                                >
                                  {lane.resource.name}
                                </Button>
                                <p className="text-xs text-muted-foreground">
                                  {lane.resource.application} ·{" "}
                                  {lane.resource.family}
                                </p>
                              </TableCell>
                              <TableCell>
                                <Status
                                  label={lane.blocked_reason ?? lane.status}
                                  tone={
                                    lane.blocked_reason ? "warning" : "neutral"
                                  }
                                />
                                <p
                                  className="mt-1 max-w-64 truncate text-xs text-muted-foreground"
                                  title={lane.last_error ?? undefined}
                                >
                                  {lane.last_error ??
                                    `Attempts ${exact(lane.attempts)}`}
                                </p>
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {exact(lane.pending_rows)}
                                <p className="text-xs text-muted-foreground">
                                  {exact(lane.pending_bytes)} B
                                </p>
                              </TableCell>
                              <TableCell>
                                {age(lane.oldest_pending_age_ms)}
                              </TableCell>
                              <TableCell>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  disabled={
                                    !canOperate ||
                                    write.isPending ||
                                    (lane.pending_rows === "0" &&
                                      (!lane.seal_state ||
                                        lane.seal_state === "accepted"))
                                  }
                                  onClick={() => setRetry(lane)}
                                >
                                  Retry lane
                                </Button>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    ) : (
                      <Nothing
                        title={
                          data.incarnation
                            ? "No lanes on this page"
                            : "No outbox recorded"
                        }
                        description={
                          data.incarnation
                            ? "There are no delivery lanes in the selected page."
                            : "This cell has not recorded a transactional outbox. Events appear after the application emits them."
                        }
                      />
                    )}
                  </CardContent>
                </Card>
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
                    disabled={!data.next}
                    onClick={() => setCursor(data.next)}
                  >
                    Next lanes
                  </Button>
                </div>
              </div>
            )}
          </Feedback>
          {page.error && cursor && !data && (
            <Button variant="outline" onClick={() => setCursor(null)}>
              Reload first lane page
            </Button>
          )}
        </TabsContent>
        <TabsContent value="history">
          <EventAudit path={path} source />
        </TabsContent>
      </Tabs>
      {selected && (
        <Sheet
          open
          onOpenChange={(open) => {
            if (!open) setSelected(null)
          }}
        >
          <SheetContent className="overflow-y-auto sm:max-w-xl">
            <SheetHeader>
              <SheetTitle>{selected.resource.name}</SheetTitle>
              <SheetDescription>
                Lane identity, revision and delivery observation. No event
                payloads are included.
              </SheetDescription>
            </SheetHeader>
            <div className="space-y-4 p-4">
              <dl className="detail-grid">
                <dt>Retry eligible</dt>
                <dd>
                  {selected.retry_at_ms === "0"
                    ? "Now"
                    : eventTime(selected.retry_at_ms)}
                </dd>
                <dt>Seal</dt>
                <dd>{selected.seal_state ?? "Not requested"}</dd>
                <dt>Dispatch generation</dt>
                <dd>{exact(selected.revision.dispatch_generation)}</dd>
                <dt>Next sequence</dt>
                <dd>{exact(selected.revision.next_sequence)}</dd>
                <dt>Settled through</dt>
                <dd>{exact(selected.revision.settled_through)}</dd>
              </dl>
              <JsonView value={selected} />
              <Link
                className="text-link"
                to={`/fleets/${encodeURIComponent(fleet)}/streams`}
              >
                Find destination stream →
              </Link>
            </div>
          </SheetContent>
        </Sheet>
      )}
      {retry && (
        <EventReview
          title={`Retry ${retry.resource.name}?`}
          description="Rearm this lane using the inspected source incarnation and revision. A concurrent delivery or reset can reject the change; refresh before trying again."
          pending={write.isPending}
          onCancel={() => setRetry(null)}
          onConfirm={(reason) => write.mutate({ lane: retry, reason })}
        >
          <p className="text-sm">
            {exact(retry.pending_rows)} pending events · generation{" "}
            {exact(retry.revision.dispatch_generation)}
            {retry.seal_state && retry.seal_state !== "accepted" && (
              <span className="mt-2 block">
                Seal delivery is {retry.seal_state}; this retry includes the
                seal.
              </span>
            )}
          </p>
        </EventReview>
      )}
    </div>
  )
}

export function CellEventState({
  fleet,
  scope,
}: {
  fleet: string
  scope: string
}) {
  const data = useApi<{
    format: number
    scope: string
    sampled_ms: string
    gauges: Record<string, string>
  }>(`${cellEventsPath(fleet, scope)}/event-state`)
  const groups = [
    {
      title: "Source delivery",
      keys: ["sources", "outbox_rows", "outbox_bytes", "outbox_oldest_age_ms"],
    },
    {
      title: "Stream storage",
      keys: [
        "streams",
        "retired_streams",
        "stream_rows",
        "stream_bytes",
        "protected_records",
        "protected_bytes",
        "retention_blocked_bytes",
        "active_leases",
      ],
    },
    {
      title: "Subscriptions",
      keys: [
        "subscriptions",
        "expired_subscriptions",
        "subscription_lag_records",
        "subscription_retained_lag_records",
        "subscription_oldest_age_ms",
      ],
    },
  ]
  return (
    <div className="space-y-5">
      <div className="flex justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">Durable event state</h2>
          <p className="text-sm text-muted-foreground">
            Counters sampled from this root cell’s owner.
          </p>
        </div>
        <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
      </div>
      <EventNotice>
        Root SQLite allocation excludes facets, WAL, replication files and
        bucket storage. These gauges are a point-in-time observation of this
        cell; they are not fleet totals.
      </EventNotice>
      <Feedback loading={data.isPending} error={data.error}>
        {data.data && (
          <>
            {groups.map((group) => (
              <section className="space-y-3" key={group.title}>
                <h3 className="text-sm font-medium">{group.title}</h3>
                <EventMetrics
                  items={group.keys.map((key) => ({
                    label: key.replaceAll("_", " "),
                    value: key.endsWith("age_ms")
                      ? age(data.data.gauges[key])
                      : exact(data.data.gauges[key]),
                  }))}
                />
              </section>
            ))}
            <Card>
              <CardHeader>
                <CardTitle>Complete observation</CardTitle>
                <CardDescription>
                  Sampled {eventTime(data.data.sampled_ms)} · all gauges retain
                  exact decimal values.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <JsonView value={data.data} />
              </CardContent>
            </Card>
          </>
        )}
      </Feedback>
    </div>
  )
}

type HostMetrics = {
  observed_at: number
  generation: string
  metrics: {
    counters: { name: string; value: string }[]
    active_waiters: string
    inflight_dispatches: string
    histograms: {
      name: string
      count: string
      sum_ms: string
      buckets: { upper_ms: number | null; count: string }[]
    }[]
  }
}
export function NodeEvents({ fleet, node }: { fleet: string; node: Node }) {
  const data = useApi<HostMetrics>(
    fleetPath(
      fleet,
      `/nodes/${encodeURIComponent(node.id)}/event-metrics?generation=${encodeURIComponent(node.generation)}`
    ),
    node.lease_current
  )
  const metrics = data.data?.metrics
  const label = (name: string) =>
    name
      .replace(/^cellar_event_/, "")
      .replace(/_(total|seconds)$/, "")
      .replaceAll("_", " ")
  return (
    <div className="space-y-5">
      <div className="flex justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">Node event activity</h2>
          <p className="text-sm text-muted-foreground">
            Authenticated host metrics pinned to process generation{" "}
            {node.generation}.
          </p>
        </div>
        <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
      </div>
      <EventNotice>
        Host-lifetime counters reset when this process restarts. They include
        work served by this host and do not represent durable cell totals,
        delivery guarantees or rates.
      </EventNotice>
      {!node.lease_current ? (
        <Nothing
          title="Node lease is not current"
          description="Refresh node ownership before sampling its current process."
        />
      ) : (
        <Feedback loading={data.isPending} error={data.error}>
          {metrics && (
            <>
              <EventMetrics
                items={[
                  {
                    label: "In-flight dispatches",
                    value: exact(metrics.inflight_dispatches),
                  },
                  {
                    label: "Active waiters",
                    value: exact(metrics.active_waiters),
                  },
                ]}
              />
              <Card>
                <CardHeader>
                  <CardTitle>Event counters</CardTitle>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Activity</TableHead>
                        <TableHead className="text-right">
                          Since process start
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {metrics.counters.map((counter) => (
                        <TableRow key={counter.name}>
                          <TableCell className="capitalize">
                            {label(counter.name)}
                          </TableCell>
                          <TableCell className="text-right font-mono">
                            {exact(counter.value)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
              <div className="grid gap-4 lg:grid-cols-3">
                {metrics.histograms.map((hist) => (
                  <Card key={hist.name}>
                    <CardHeader>
                      <CardTitle className="capitalize">
                        {label(hist.name)}
                      </CardTitle>
                      <CardDescription>
                        {exact(hist.count)} samples · {exact(hist.sum_ms)} ms
                        total
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Duration ≤</TableHead>
                            <TableHead className="text-right">
                              Cumulative samples
                            </TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {hist.buckets.map((bucket) => (
                            <TableRow key={bucket.upper_ms ?? "inf"}>
                              <TableCell>
                                {bucket.upper_ms === null
                                  ? "∞"
                                  : `${bucket.upper_ms} ms`}
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {exact(bucket.count)}
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </CardContent>
                  </Card>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">
                Sampled {new Date(data.data!.observed_at).toLocaleString()}
              </p>
            </>
          )}
        </Feedback>
      )}
    </div>
  )
}
