// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { StopReceiptView, type StopLocator } from "@/components/stop-receipt"
import { Link, useParams } from "react-router-dom"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Feedback,
  JsonView,
  Nothing,
  PageHeading,
  Refresh,
  Status,
} from "@/components/console-ui"
import {
  date,
  fleetPath,
  useApi,
  type Application,
  type Operation,
} from "@/lib/api"
import { useSession } from "@/lib/session"

function eventReceiptPath(fleet: string, item: Operation) {
  const base = `/fleets/${encodeURIComponent(fleet)}`
  const operation = encodeURIComponent(item.id)
  if (
    ["outbox_retry", "outbox_destroy", "outbox_recreate"].includes(
      item.action
    ) &&
    item.target.startsWith("outbox/")
  )
    return `${base}/cells/${encodeURIComponent(item.target.slice(7))}?tab=outbox&operation=${operation}`
  const stream = /^stream\/([^/]+)\/([^/]+)\/([a-f0-9]{32})$/.exec(item.target)
  if (["subscription_change", "stream_retire"].includes(item.action) && stream)
    return `${base}/streams/${encodeURIComponent(stream[1])}/${encodeURIComponent(stream[2])}?resource=${stream[3]}&view=${item.action === "stream_retire" ? "lifecycle" : "history"}&operation=${operation}`
  return null
}

export function Operations() {
  const { session } = useSession()
  const [receipt, setReceipt] = useState<StopLocator | null>(null)
  const { fleet = "" } = useParams()
  const data = useApi<{ items: Operation[]; coverage: string }>(
    fleetPath(fleet, "/operations")
  )
  return (
    <div className="page-stack">
      <PageHeading
        title="Operations"
        description="Recorded management requests, targets and outcomes."
      >
        <Refresh onClick={() => void data.refetch()} busy={data.isFetching} />
      </PageHeading>
      <Card>
        <CardHeader>
          <CardTitle>Operation history</CardTitle>
          <CardDescription>
            Latest 100 records. An unknown outcome must be reconciled before
            retrying a write.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Feedback loading={data.isPending} error={data.error}>
            {data.data?.items.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    {["Action", "Target", "Outcome", "Actor", "Started"].map(
                      (label) => (
                        <TableHead key={label}>{label}</TableHead>
                      )
                    )}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.data.items.map((item) => (
                    <TableRow key={`${item.actor}/${item.id}`}>
                      <TableCell>{item.action.replaceAll("_", " ")}</TableCell>
                      <TableCell>
                        {item.target}
                        <div className="text-xs text-muted-foreground">
                          {item.id}
                        </div>
                        {item.actor === session.user &&
                          eventReceiptPath(fleet, item) && (
                            <Button
                              variant="link"
                              size="sm"
                              className="px-0"
                              asChild
                            >
                              <Link to={eventReceiptPath(fleet, item)!}>
                                Inspect event receipt
                              </Link>
                            </Button>
                          )}
                        {item.action === "stop" &&
                        /^node\/([^@]+)@(.+)$/.test(item.target) ? (
                          <Button
                            variant="link"
                            size="sm"
                            className="px-0"
                            onClick={() => {
                              const match = /^node\/([^@]+)@(.+)$/.exec(
                                item.target
                              )!
                              setReceipt({
                                node: match[1],
                                generation: match[2],
                                actor: item.actor,
                                operation: item.id,
                              })
                            }}
                          >
                            Inspect stop receipt
                          </Button>
                        ) : null}
                      </TableCell>
                      <TableCell>
                        <Status
                          label={item.status.replaceAll("_", " ")}
                          tone={
                            item.status === "succeeded" ? "good" : "warning"
                          }
                        />
                      </TableCell>
                      <TableCell>{item.actor}</TableCell>
                      <TableCell>{date(item.started_at)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Nothing
                title="No operations recorded"
                description="Changes made through this console appear here. Resource values and credentials are excluded from the audit record."
              />
            )}
          </Feedback>
        </CardContent>
      </Card>
      <Dialog
        open={receipt !== null}
        onOpenChange={(open) => {
          if (!open) {
            setReceipt(null)
            void data.refetch()
          }
        }}
      >
        <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Node stop receipt</DialogTitle>
            <DialogDescription>
              Read the persisted outcome, including after the node is
              unavailable.
            </DialogDescription>
          </DialogHeader>
          {receipt ? <StopReceiptView fleet={fleet} target={receipt} /> : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}

export function Settings() {
  const { fleet = "" } = useParams()
  const { session } = useSession()
  const current = session.fleets.find((item) => item.id === fleet)
  return (
    <div className="page-stack">
      <PageHeading
        title="Fleet settings"
        description="Connection identity and your access to this fleet."
      />
      <Card>
        <CardHeader>
          <CardTitle>Connection</CardTitle>
          <CardDescription>
            Fleet connections are configured by the console host. Credentials
            stay on the server.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <dl className="detail-grid">
            <dt>Fleet name</dt>
            <dd>{current?.name}</dd>
            <dt>Fleet ID</dt>
            <dd>
              <code>{fleet}</code>
            </dd>
            <dt>Environment</dt>
            <dd>{current?.environment}</dd>
            <dt>Signed in as</dt>
            <dd>{session.user}</dd>
            <dt>Permissions</dt>
            <dd className="flex flex-wrap gap-2">
              {session.grants[fleet]?.map((permission) => (
                <Badge variant="secondary" key={permission}>
                  {permission.replaceAll("_", " ")}
                </Badge>
              ))}
            </dd>
          </dl>
        </CardContent>
      </Card>
    </div>
  )
}

export function ApplicationDetail() {
  const { fleet = "", id = "" } = useParams()
  const data = useApi<Application>(
    fleetPath(fleet, `/applications/${encodeURIComponent(id)}`)
  )
  return (
    <div className="page-stack">
      <PageHeading
        title={id}
        description="Deployment identity, exported cell classes and available bindings."
      />
      <Feedback loading={data.isPending} error={data.error}>
        {data.data ? (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Resource bindings</CardTitle>
                <CardDescription>
                  These resources are available to the application’s cells.
                  Bindings do not imply private per-cell copies.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Binding</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Identity</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.data.bindings.map((binding) => (
                      <TableRow key={binding.name}>
                        <TableCell>
                          {[
                            "sql",
                            "kv",
                            "queue",
                            "workflow",
                            "event_stream",
                          ].includes(binding.kind) ? (
                            <Link
                              className="text-link"
                              to={`/fleets/${encodeURIComponent(fleet)}/${binding.kind === "event_stream" ? "streams" : binding.kind === "queue" ? "queues" : binding.kind === "workflow" ? "workflows" : binding.kind}/${encodeURIComponent(id)}/${encodeURIComponent(binding.name)}`}
                            >
                              {binding.name}
                            </Link>
                          ) : (
                            binding.name
                          )}
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">{binding.kind}</Badge>
                        </TableCell>
                        <TableCell>
                          {binding.identity || binding.event?.resource.name}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Deployment metadata</CardTitle>
                <CardDescription>
                  Version and classes observed from the published manifest.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <JsonView
                  value={{
                    version: data.data.version,
                    classes: data.data.classes,
                    revision: data.data.revision,
                  }}
                />
              </CardContent>
            </Card>
          </>
        ) : null}
      </Feedback>
    </div>
  )
}
