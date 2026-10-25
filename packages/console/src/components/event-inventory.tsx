// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { Link } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Field, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableHeader,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
} from "@/components/ui/table"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet"
import { Feedback, JsonView, Nothing, Status } from "@/components/console-ui"
import { EventMetrics, EventNotice } from "@/components/event-ui"
import { request, fleetPath } from "@/lib/api"
import { useSession } from "@/lib/session"
import { exact, eventTime, type Resource } from "@/lib/events"

type Descriptor = {
  kind: "source" | "stream" | "catalog"
  scope: string
  incarnation?: string | null
  history?: boolean
  resource?: { name: string; id: string } | null
  namespace?: { tenant: string; application: string } | null
}
type Row = {
  scope: string
  status: "observed" | "unavailable"
  inventory?: { kind: "descriptor"; data: Descriptor }
  state?: { gauges: Record<string, string> }
}
type Page = {
  records: Row[]
  next: string | null
  scanned_prefixes: number
  complete_probes: boolean
  totals: {
    observed: number
    unavailable: number
    observed_gauges: Record<string, string>
  }
}
export function FleetEventInventory({ fleet }: { fleet: string }) {
  const { token, session } = useSession()
  const canInspect = session.grants[fleet]?.includes("data_read")
  const [prefix, setPrefix] = useState("")
  const [applied, setApplied] = useState("")
  const [cursor, setCursor] = useState<string | null>(null)
  const [selected, setSelected] = useState<Row | null>(null)
  const [historyCursor, setHistoryCursor] = useState<string | null>(null)
  const [historyPage, setHistoryPage] = useState<
    "history" | "resources" | "producers"
  >("history")
  const data = useMutation({
    mutationFn: ({
      prefix,
      cursor,
    }: {
      prefix: string
      cursor: string | null
    }) =>
      request<Page>(
        token,
        fleetPath(
          fleet,
          `/events/inventory?limit=25&prefix=${encodeURIComponent(prefix)}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`
        )
      ),
  })
  const detail = useMutation({
    mutationFn: ({
      scope,
      op,
      cursor,
    }: {
      scope: string
      op: "history" | "resources" | "producers"
      cursor: string | null
    }) =>
      request<{
        kind: string
        data: {
          records?: unknown[]
          cursor?: string | null
          page?: { records: unknown[]; cursor: string | null }
        }
      }>(
        token,
        fleetPath(fleet, `/cells/${encodeURIComponent(scope)}/event-inventory`),
        { method: "POST", body: JSON.stringify({ op, limit: 25, cursor }) }
      ),
  })
  const page = data.data,
    gauges = page?.totals.observed_gauges
  function inspect(row: Row) {
    setSelected(row)
    setHistoryCursor(null)
    detail.reset()
    const kind = row.inventory?.data.kind
    setHistoryPage(
      kind === "catalog"
        ? "resources"
        : kind === "stream"
          ? "producers"
          : "history"
    )
  }
  return (
    <div className="flex flex-col gap-5">
      <EventNotice>
        Inspect up to 25 root cells per page, including sources that have not
        reached a destination and resources no longer declared by an
        application. Reads may restore cold cells and progress existing work.
        This is a live observation; facets and complete historical discovery
        remain outside this inventory.
      </EventNotice>
      <Card>
        <CardHeader>
          <CardTitle>Fleet event inventory</CardTitle>
          <CardDescription>
            Discover catalogs, streams and current source identities through
            their owners. Inspection runs only when requested.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <form
            className="flex flex-wrap items-end gap-3"
            onSubmit={(e) => {
              e.preventDefault()
              setApplied(prefix.trim())
              setCursor(null)
              data.mutate({ prefix: prefix.trim(), cursor: null })
            }}
          >
            <Field className="min-w-0 flex-1">
              <FieldLabel htmlFor="event-inventory-prefix">
                Root cell prefix
              </FieldLabel>
              <Input
                id="event-inventory-prefix"
                value={prefix}
                onChange={(e) => setPrefix(e.target.value)}
                placeholder="Order: or __EventStream:"
              />
            </Field>
            <Button type="submit" disabled={data.isPending || !canInspect}>
              Inspect root page
            </Button>
          </form>
          {!canInspect && (
            <p className="text-sm text-muted-foreground">
              Fleet inspection requires data read permission.
            </p>
          )}
          <Feedback loading={data.isPending} error={data.error}>
            {page && (
              <>
                {!page.complete_probes && (
                  <EventNotice>
                    Some roots could not be observed. Their state is unknown;
                    the observed values below are partial and exclude
                    unavailable roots.
                  </EventNotice>
                )}
                {gauges && (
                  <EventMetrics
                    items={[
                      {
                        label: "Observed roots on this page",
                        value: page.totals.observed,
                      },
                      {
                        label: "Unavailable roots",
                        value: page.totals.unavailable,
                      },
                      {
                        label: "Observed pending events",
                        value: exact(gauges.outbox_rows),
                      },
                      {
                        label: "Observed retained events",
                        value: exact(gauges.stream_rows),
                      },
                    ]}
                  />
                )}
                {page.records.length ? (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Root cell</TableHead>
                        <TableHead>Observation</TableHead>
                        <TableHead>Pending / retained</TableHead>
                        <TableHead>Inspect</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {page.records.map((row) => (
                        <TableRow key={row.scope}>
                          <TableCell>
                            <Link
                              className="text-link block max-w-80 truncate font-mono text-xs"
                              title={row.scope}
                              to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(row.scope)}?tab=${row.inventory?.data.kind === "source" ? "outbox" : "events"}`}
                            >
                              {row.scope}
                            </Link>
                          </TableCell>
                          <TableCell>
                            <Status
                              label={
                                row.status === "unavailable"
                                  ? "Unavailable"
                                  : row.inventory?.data.kind === "source" &&
                                      !row.inventory.data.incarnation
                                    ? "No current outbox"
                                    : (row.inventory?.data.kind ?? "Observed")
                              }
                              tone={
                                row.status === "unavailable"
                                  ? "warning"
                                  : "neutral"
                              }
                            />
                          </TableCell>
                          <TableCell>
                            {row.state
                              ? `${exact(row.state.gauges.outbox_rows)} / ${exact(row.state.gauges.stream_rows)}`
                              : "Unknown"}
                          </TableCell>
                          <TableCell>
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={row.status !== "observed"}
                              onClick={() => inspect(row)}
                            >
                              Inspect metadata
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                ) : (
                  <Nothing
                    title="No roots on this page"
                    description="Continue if a cursor is available, or use another prefix."
                  />
                )}
                <details>
                  <summary className="cursor-pointer text-sm font-medium">
                    All observed page gauges
                  </summary>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Metric</TableHead>
                        <TableHead className="text-right">
                          Observed value
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {Object.entries(page.totals.observed_gauges).map(
                        ([key, value]) => (
                          <TableRow key={key}>
                            <TableCell>{key.replaceAll("_", " ")}</TableCell>
                            <TableCell className="text-right font-mono">
                              {exact(value)}
                            </TableCell>
                          </TableRow>
                        )
                      )}
                    </TableBody>
                  </Table>
                </details>
                <p className="text-xs text-muted-foreground">
                  {page.scanned_prefixes} prefixes inspected · page observations
                  only ·{" "}
                  {page.next
                    ? "More roots available"
                    : "End of this directory traversal"}
                </p>
                <div className="flex flex-wrap gap-2">
                  <Button
                    variant="outline"
                    disabled={!cursor || data.isPending}
                    onClick={() => {
                      setCursor(null)
                      data.mutate({ prefix: applied, cursor: null })
                    }}
                  >
                    First root page
                  </Button>
                  <Button
                    variant="outline"
                    disabled={!page.next || data.isPending}
                    onClick={() => {
                      setCursor(page.next)
                      data.mutate({ prefix: applied, cursor: page.next })
                    }}
                  >
                    Next root page
                  </Button>
                </div>
              </>
            )}
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
          <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
            <SheetHeader>
              <SheetTitle>Root event metadata</SheetTitle>
              <SheetDescription className="break-all">
                {selected.scope}
              </SheetDescription>
            </SheetHeader>
            <div className="flex flex-col gap-4 p-4">
              <JsonView value={selected.inventory?.data} />
              <p className="text-sm text-muted-foreground">
                Bounded {historyPage} pages include retained metadata without
                event bodies, grants, lease tokens or full receipts.
              </p>
              <Button
                variant="outline"
                disabled={detail.isPending}
                onClick={() => {
                  setHistoryCursor(null)
                  detail.mutate({
                    scope: selected.scope,
                    op: historyPage,
                    cursor: null,
                  })
                }}
              >
                Load {historyPage}
              </Button>
              <Feedback loading={detail.isPending} error={detail.error}>
                {detail.data && (
                  <>
                    <InventoryRecords
                      fleet={fleet}
                      kind={historyPage}
                      records={
                        detail.data.data.records ??
                        detail.data.data.page?.records ??
                        []
                      }
                    />
                    <details>
                      <summary className="cursor-pointer text-sm">
                        Observation metadata
                      </summary>
                      <JsonView value={detail.data.data} />
                    </details>
                    <Button
                      variant="outline"
                      disabled={
                        !(
                          detail.data.data.cursor ??
                          detail.data.data.page?.cursor
                        ) || detail.isPending
                      }
                      onClick={() => {
                        const next =
                          detail.data!.data.cursor ??
                          detail.data!.data.page?.cursor ??
                          null
                        setHistoryCursor(next)
                        detail.mutate({
                          scope: selected.scope,
                          op: historyPage,
                          cursor: next,
                        })
                      }}
                    >
                      Next metadata page
                    </Button>
                    {historyCursor && (
                      <Button
                        variant="ghost"
                        onClick={() => {
                          setHistoryCursor(null)
                          detail.mutate({
                            scope: selected.scope,
                            op: historyPage,
                            cursor: null,
                          })
                        }}
                      >
                        First metadata page
                      </Button>
                    )}
                  </>
                )}
              </Feedback>
            </div>
          </SheetContent>
        </Sheet>
      )}
    </div>
  )
}

function InventoryRecords({
  fleet,
  kind,
  records,
}: {
  fleet: string
  kind: "history" | "resources" | "producers"
  records: unknown[]
}) {
  const { token } = useSession()
  const proof = useMutation({
    mutationFn: (resource: Resource) =>
      request<unknown>(token, fleetPath(fleet, "/events/retirement-proof"), {
        method: "POST",
        body: JSON.stringify(resource),
      }),
  })
  if (!records.length)
    return (
      <Nothing
        title="No retained metadata on this page"
        description="Continue if a cursor is available. This page is a live observation."
      />
    )
  return (
    <div className="flex flex-col gap-3">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>
              {kind === "history"
                ? "Operation"
                : kind === "resources"
                  ? "Resource"
                  : "Producer"}
            </TableHead>
            <TableHead>State</TableHead>
            <TableHead>
              {kind === "history"
                ? "Discarded events / bytes"
                : kind === "resources"
                  ? "Accepted revision"
                  : "Accepted through"}
            </TableHead>
            <TableHead>Details</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {records.map((record, index) => {
            const row = record as Record<string, unknown>,
              resource = row.resource as Resource | undefined
            return (
              <TableRow
                key={String(row.operation ?? row.lane ?? resource?.id ?? index)}
              >
                <TableCell>
                  <span className="block max-w-64 truncate font-mono text-xs">
                    {String(
                      kind === "resources"
                        ? resource?.name
                        : kind === "history"
                          ? row.operation
                          : row.source
                    )}
                  </span>
                  <span className="block max-w-64 truncate font-mono text-xs text-muted-foreground">
                    {String(resource?.id ?? row.incarnation ?? "")}
                  </span>
                </TableCell>
                <TableCell>
                  <Status
                    label={String(
                      row.state ??
                        (kind === "history"
                          ? "Retry"
                          : row.sealed
                            ? "Sealed"
                            : "Open")
                    )}
                  />
                </TableCell>
                <TableCell className="font-mono">
                  {kind === "history"
                    ? `${exact(row.discarded_rows as string | null)} / ${exact(row.discarded_bytes as string | null)}`
                    : exact(
                        (kind === "resources"
                          ? row.accepted_revision
                          : row.accepted_through) as string | null
                      )}
                </TableCell>
                <TableCell>
                  {kind === "resources" && resource ? (
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={row.state !== "deleted" || proof.isPending}
                      onClick={() => proof.mutate(resource)}
                    >
                      Verify retirement
                    </Button>
                  ) : kind === "history" ? (
                    eventTime(row.created_ms as string)
                  ) : (
                    <Link
                      className="text-link"
                      to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(String(row.source))}?tab=outbox`}
                    >
                      Inspect outbox
                    </Link>
                  )}
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
      <Feedback loading={proof.isPending} error={proof.error}>
        {proof.data != null && <JsonView value={proof.data} />}
      </Feedback>
    </div>
  )
}
