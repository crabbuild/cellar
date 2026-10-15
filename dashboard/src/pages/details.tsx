import { Link, useParams, useSearchParams } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import { ArrowLeft, Radio } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  CopyValue,
  Feedback,
  JsonView,
  PageHeading,
  Refresh,
  Status,
} from "@/components/console-ui"
import {
  bytes,
  date,
  fleetPath,
  request,
  useApi,
  type Cell,
  type Node,
} from "@/lib/api"
import {
  CellOutbox,
  CellEventState,
  NodeEvents,
} from "@/components/cell-events"
import { Nothing } from "@/components/console-ui"
import { NodeCells } from "@/components/node-cells"
import { NodeControls } from "@/components/node-controls"
import { CellControls } from "@/components/cell-controls"
import { CellBindings } from "@/components/cell-bindings"
import { useSession } from "@/lib/session"

export default function Details({ kind }: { kind: "nodes" | "cells" }) {
  const { fleet = "", id = "" } = useParams()
  const { token, session } = useSession()
  const [params, setParams] = useSearchParams()
  const tab = params.get("tab") ?? "overview"
  const canRead = session.grants[fleet]?.includes("data_read")
  const data = useApi<Node | Cell>(
    fleetPath(fleet, `/${kind}/${encodeURIComponent(id)}`)
  )
  const probe = useMutation({
    mutationFn: () =>
      request(
        token,
        fleetPath(fleet, `/nodes/${encodeURIComponent(id)}/probe`),
        { method: "POST" }
      ),
    onSuccess: () =>
      toast.success("Signed probe confirmed this node is reachable"),
    onError: (e: Error) => toast.error(e.message),
  })
  const record = data.data
  return (
    <div className="page-stack">
      <Link
        className="back-link"
        to={`/fleets/${encodeURIComponent(fleet)}/${kind}`}
      >
        <ArrowLeft />
        Back to {kind}
      </Link>
      <PageHeading
        title={id}
        description={
          kind === "nodes"
            ? "Node identity, current observations and ownership."
            : "Durable cell identity and observed owner. This address stays stable when ownership moves."
        }
      >
        <CopyValue value={id} />
        <Refresh onClick={() => void data.refetch()} busy={data.isFetching} />
      </PageHeading>
      <Feedback loading={data.isPending} error={data.error}>
        {record ? (
          <Tabs value={tab} onValueChange={(tab) => setParams({ tab })}>
            <TabsList variant="line">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              {kind === "cells" && (
                <TabsTrigger value="outbox">Outbox</TabsTrigger>
              )}
              <TabsTrigger value="events">
                {kind === "cells" ? "Event state" : "Event activity"}
              </TabsTrigger>
              <TabsTrigger value="observation">Observation data</TabsTrigger>
            </TabsList>
            <TabsContent value="overview">
              <Card>
                <CardHeader>
                  <CardTitle>
                    {kind === "nodes" ? "Node observation" : "Cell ownership"}
                  </CardTitle>
                  <CardDescription>
                    Observations may change after they are read.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {"lease_current" in record ? (
                    <>
                      <dl className="detail-grid">
                        <dt>Lease</dt>
                        <dd>
                          <Status
                            label={record.lease_current ? "Current" : "Expired"}
                            tone={record.lease_current ? "good" : "warning"}
                          />
                        </dd>
                        <dt>Process generation</dt>
                        <dd>
                          <code>{record.generation}</code>
                        </dd>
                        <dt>Advertised address</dt>
                        <dd>{record.address}</dd>
                        <dt>Lease expires</dt>
                        <dd>{date(record.lease_expires_at)}</dd>
                        <dt>Owned cells</dt>
                        <dd>{record.load.owned_cells ?? "Not reported"}</dd>
                        <dt>Resident cells</dt>
                        <dd>{record.load.resident_cells}</dd>
                        <dt>Memory · RSS</dt>
                        <dd>{bytes(record.load.rss_bytes)}</dd>
                        <dt>Load sampled</dt>
                        <dd>{date(record.load.sampled_ms)}</dd>
                        <dt>Balancing</dt>
                        <dd>
                          {record.load.rebalance_paused == null
                            ? "Not reported"
                            : record.load.rebalance_paused
                              ? "Paused · fleet donors wait"
                              : "Not paused by this node"}
                        </dd>
                      </dl>
                      <Button
                        variant="outline"
                        disabled={probe.isPending}
                        onClick={() => probe.mutate()}
                      >
                        <Radio data-icon="inline-start" />
                        {probe.isPending ? "Probing…" : "Run signed probe"}
                      </Button>
                    </>
                  ) : (
                    <dl className="detail-grid">
                      <dt>Class</dt>
                      <dd>{record.class ?? "Unclassified"}</dd>
                      <dt>Kind</dt>
                      <dd>{record.kind}</dd>
                      <dt>Observed owner</dt>
                      <dd>
                        {record.owner ? (
                          <Link
                            className="text-link"
                            to={`/fleets/${encodeURIComponent(fleet)}/nodes/${encodeURIComponent(record.owner)}`}
                          >
                            {record.owner}
                          </Link>
                        ) : (
                          "No owner observed"
                        )}
                      </dd>
                      <dt>Epoch</dt>
                      <dd>{record.epoch ?? "Not observed"}</dd>
                      <dt>Observed at</dt>
                      <dd>{date(record.observed_at)}</dd>
                      {record.error ? (
                        <>
                          <dt>Coverage</dt>
                          <dd className="text-muted-foreground">
                            {record.error}
                          </dd>
                        </>
                      ) : null}
                    </dl>
                  )}
                </CardContent>
              </Card>
              {"scope" in record ? (
                <>
                  <CellBindings
                    key={`${fleet}:${record.scope}`}
                    fleet={fleet}
                    cell={record}
                  />
                  <CellControls
                    key={`${record.scope}@${record.owner}@${record.epoch}`}
                    fleet={fleet}
                    cell={record}
                  />
                </>
              ) : (
                <>
                  <NodeControls
                    key={`${record.id}@${record.generation}`}
                    fleet={fleet}
                    node={record}
                  />
                  <NodeCells
                    key={`cells:${record.id}@${record.generation}`}
                    fleet={fleet}
                    node={record}
                  />
                </>
              )}
            </TabsContent>
            <TabsContent value="outbox">
              {canRead && "scope" in record ? (
                <CellOutbox
                  key={`${fleet}:${record.scope}`}
                  fleet={fleet}
                  scope={record.scope}
                />
              ) : (
                <Nothing
                  title="Event inspection requires data access"
                  description="Ask your console administrator for data_read access to this fleet."
                />
              )}
            </TabsContent>
            <TabsContent value="events">
              {canRead ? (
                "scope" in record ? (
                  <CellEventState
                    key={`${fleet}:${record.scope}`}
                    fleet={fleet}
                    scope={record.scope}
                  />
                ) : (
                  <NodeEvents
                    key={`${fleet}:${record.id}:${record.generation}`}
                    fleet={fleet}
                    node={record}
                  />
                )
              ) : (
                <Nothing
                  title="Event inspection requires data access"
                  description="Ask your console administrator for data_read access to this fleet."
                />
              )}
            </TabsContent>
            <TabsContent value="observation">
              <JsonView value={record} />
            </TabsContent>
          </Tabs>
        ) : null}
      </Feedback>
    </div>
  )
}
