import { Link, useParams } from "react-router-dom"
import {
  ArrowUpRight,
  Box,
  CircleDot,
  Layers3,
  Network,
  Server,
} from "lucide-react"
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import {
  Coverage,
  Feedback,
  Nothing,
  PageHeading,
  Refresh,
  Status,
} from "@/components/console-ui"
import {
  bytes,
  date,
  fleetPath,
  useApi,
  type Application,
  type Node,
  type Operation,
  type Page,
} from "@/lib/api"

export default function Overview() {
  const { fleet = "" } = useParams()
  const nodes = useApi<Page<Node>>(fleetPath(fleet, "/nodes"))
  const apps = useApi<Page<Application>>(fleetPath(fleet, "/applications"))
  const operations = useApi<{ items: Operation[] }>(
    fleetPath(fleet, "/operations")
  )
  const current = nodes.data?.items.filter((node) => node.lease_current) ?? []
  const knownOwned = current.filter((node) => node.load.owned_cells != null)
  const owned = knownOwned.reduce(
    (sum, node) => sum + (node.load.owned_cells ?? 0),
    0
  )
  const residents = current.reduce(
    (sum, node) => sum + node.load.resident_cells,
    0
  )
  const resourceCount = apps.data?.items.reduce(
    (sum, app) =>
      sum +
      app.bindings.filter((b) =>
        ["sql", "kv", "queue", "workflow"].includes(b.kind)
      ).length,
    0
  )
  const incomplete =
    nodes.data?.coverage !== "complete" || knownOwned.length !== current.length
  const attention =
    nodes.data?.items.filter(
      (node) => !node.lease_current || node.load.pressured || node.load.draining
    ).length ?? 0
  return (
    <div className="page-stack">
      <PageHeading
        title="Fleet overview"
        description="A clear view of your infrastructure and the cells it runs."
      >
        <Refresh
          busy={nodes.isFetching || apps.isFetching}
          onClick={() => {
            void nodes.refetch()
            void apps.refetch()
            void operations.refetch()
          }}
        />
      </PageHeading>
      <Feedback loading={nodes.isPending} error={nodes.error}>
        <div className="fleet-summary">
          <div className="summary-signal">
            <Network />
            <div>
              <span className="eyebrow">Fleet observation</span>
              <h2>
                {!nodes.data
                  ? "Waiting for observations"
                  : !nodes.data.items.length
                    ? "Ready for your first node"
                    : attention
                      ? `${attention} node${attention === 1 ? " needs" : "s need"} attention`
                      : "Your fleet, at a glance"}
              </h2>
              <p>
                {nodes.data
                  ? `Last observed ${date(nodes.data.observed_at)}`
                  : "Connect a fleet to inspect its infrastructure."}
              </p>
            </div>
          </div>
          <Badge variant="outline">
            {nodes.data?.coverage === "complete"
              ? "Inventory observed"
              : "Partial observation"}
          </Badge>
        </div>
        <div className="metric-grid">
          {[
            {
              label: "Current node leases",
              value: nodes.data ? current.length : "—",
              sub: `${nodes.data?.items.length ?? "—"} node records on this page`,
              icon: Server,
            },
            {
              label: "Reported owned cells",
              value: nodes.data ? owned.toLocaleString() : "—",
              sub: incomplete
                ? "Partial reporting · current leases"
                : "Across current node leases",
              icon: Box,
            },
            {
              label: "Resident cells",
              value: nodes.data ? residents.toLocaleString() : "—",
              sub: "Active residency on observed nodes",
              icon: CircleDot,
            },
            {
              label: "Resource bindings",
              value: resourceCount ?? "—",
              sub: "Shared resources may appear more than once",
              icon: Layers3,
            },
          ].map(({ label, value, sub, icon: Icon }) => (
            <Card key={label}>
              <CardHeader>
                <CardDescription>{label}</CardDescription>
                <CardAction>
                  <Icon className="metric-icon" />
                </CardAction>
              </CardHeader>
              <CardContent>
                <div className="metric-value">{value}</div>
                <p className="metric-caption">{sub}</p>
              </CardContent>
            </Card>
          ))}
        </div>
        {nodes.data?.coverage === "partial" ? (
          <Alert>
            <Network />
            <AlertTitle>This is a partial fleet observation</AlertTitle>
            <AlertDescription>
              Counts cover the returned node page. Open Nodes to continue the
              inventory.
            </AlertDescription>
          </Alert>
        ) : null}
        <Card>
          <CardHeader>
            <CardTitle>Cell placement</CardTitle>
            <CardDescription>
              Reported ownership across your nodes. Select a node to inspect its
              observation.
            </CardDescription>
            <CardAction>
              <Button asChild variant="ghost" size="sm">
                <Link to={`../nodes`}>
                  View nodes
                  <ArrowUpRight data-icon="inline-end" />
                </Link>
              </Button>
            </CardAction>
          </CardHeader>
          <CardContent>
            {nodes.data?.items.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Node</TableHead>
                    <TableHead>Observed state</TableHead>
                    <TableHead>Owned cells</TableHead>
                    <TableHead>Residents</TableHead>
                    <TableHead>Memory · RSS</TableHead>
                    <TableHead>CPU</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {nodes.data.items.map((node) => (
                    <TableRow key={node.id}>
                      <TableCell>
                        <Link
                          className="entity-link"
                          to={`../nodes/${encodeURIComponent(node.id)}`}
                        >
                          <span className="entity-icon">
                            <Server />
                          </span>
                          <span>
                            {node.id}
                            <small>{node.address}</small>
                          </span>
                        </Link>
                      </TableCell>
                      <TableCell>
                        <Status
                          label={
                            !node.lease_current
                              ? "Lease expired"
                              : node.load.draining
                                ? "Draining"
                                : node.load.pressured
                                  ? "Under pressure"
                                  : "Lease current"
                          }
                          tone={
                            !node.lease_current ||
                            node.load.pressured ||
                            node.load.draining
                              ? "warning"
                              : "good"
                          }
                        />
                      </TableCell>
                      <TableCell>
                        <div className="placement-number">
                          {node.load.owned_cells?.toLocaleString() ??
                            "Not reported"}
                        </div>
                        <div className="placement-track" aria-hidden>
                          <div
                            style={{
                              width: `${Math.min(100, ((node.load.owned_cells ?? 0) / Math.max(1, owned)) * 100)}%`,
                            }}
                          />
                        </div>
                      </TableCell>
                      <TableCell>{node.load.resident_cells}</TableCell>
                      <TableCell>{bytes(node.load.rss_bytes)}</TableCell>
                      <TableCell>
                        {node.load.sampled_ms
                          ? `${(node.load.cpu_percent_x100 / 100).toFixed(1)}%`
                          : "Not reported"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Nothing
                title="No node observations"
                description="Start a Cellar application using this fleet’s bucket. Its lease will appear here when published."
              />
            )}
            <Coverage page={nodes.data} />
          </CardContent>
        </Card>
      </Feedback>
      <div className="overview-bottom">
        <Card>
          <CardHeader>
            <CardTitle>Applications</CardTitle>
            <CardDescription>
              Deployment identities and their shared bindings.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Feedback loading={apps.isPending} error={apps.error}>
              {apps.data?.items.length ? (
                <div className="application-list">
                  {apps.data.items.map((app) => (
                    <Link
                      key={app.name}
                      to={`../applications/${encodeURIComponent(app.name)}`}
                    >
                      <span className="entity-icon">
                        <Box />
                      </span>
                      <div>
                        <strong>{app.name}</strong>
                        <small>
                          {app.bindings.length} bindings · {app.classes.length}{" "}
                          classes
                        </small>
                      </div>
                      <Badge variant="secondary">
                        {app.primary ? "Primary" : "Cohosted"}
                      </Badge>
                      <ArrowUpRight />
                    </Link>
                  ))}
                </div>
              ) : (
                <Nothing
                  title="No applications found"
                  description="Publish a compiled application to this fleet to inspect its deployment and bindings."
                />
              )}
            </Feedback>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Recent operations</CardTitle>
            <CardDescription>
              Management actions and their recorded outcomes.
            </CardDescription>
            <CardAction>
              <Button variant="ghost" size="sm" asChild>
                <Link to="../operations">
                  View all
                  <ArrowUpRight data-icon="inline-end" />
                </Link>
              </Button>
            </CardAction>
          </CardHeader>
          <CardContent>
            <Feedback error={operations.error}>
              {operations.data?.items.length ? (
                <div className="operation-list">
                  {operations.data.items.slice(0, 5).map((op) => (
                    <div key={op.id}>
                      <span className="activity-marker" />
                      <div>
                        <strong>{op.action.replaceAll("_", " ")}</strong>
                        <small>{op.target}</small>
                      </div>
                      <Status
                        label={op.status.replaceAll("_", " ")}
                        tone={op.status === "succeeded" ? "good" : "warning"}
                      />
                    </div>
                  ))}
                </div>
              ) : (
                <Nothing
                  title="No operations recorded"
                  description="Actions you take in the console will appear here with their target and outcome."
                />
              )}
            </Feedback>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
