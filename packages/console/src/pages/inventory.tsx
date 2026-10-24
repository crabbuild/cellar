import { useState } from "react"
import { Link, useParams, useSearchParams } from "react-router-dom"
import { Box, Search, Server } from "lucide-react"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { AddNodeGuide } from "@/components/add-node-guide"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Coverage,
  Feedback,
  NextPage,
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
  type Cell,
  type Node,
  type Page,
} from "@/lib/api"

export default function Inventory({
  kind,
}: {
  kind: "nodes" | "cells" | "applications"
}) {
  const { fleet = "" } = useParams()
  const [params, setParams] = useSearchParams()
  const [prefix, setPrefix] = useState(params.get("prefix") ?? "")
  const [search, setSearch] = useState("")
  const data = useApi<Page<Node | Cell | Application>>(
    fleetPath(fleet, `/${kind}?${params}`)
  )
  const titles = {
    nodes: ["Nodes", "Inspect node leases, capacity and the cells they own."],
    cells: [
      "Cells",
      "Find durable state and follow its ownership across the fleet.",
    ],
    applications: [
      "Applications",
      "Explore your deployments, cell classes and resource bindings.",
    ],
  }
  const filtered =
    data.data?.items.filter((item) =>
      JSON.stringify(item).toLowerCase().includes(search.toLowerCase())
    ) ?? []
  return (
    <div className="page-stack">
      <PageHeading title={titles[kind][0]} description={titles[kind][1]}>
        {kind === "nodes" ? <AddNodeGuide /> : null}
        <Refresh onClick={() => void data.refetch()} busy={data.isFetching} />
      </PageHeading>
      <Card>
        <CardHeader>
          <CardTitle>{titles[kind][0]} inventory</CardTitle>
          <CardDescription>
            Storage listings are paginated. Owner and load data describe the
            last observation.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="table-toolbar">
            {kind === "cells" ? (
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  setParams(prefix ? { prefix } : {})
                }}
              >
                <FieldGroup className="flex-row items-end">
                  <Field>
                    <FieldLabel htmlFor="prefix">Cell scope prefix</FieldLabel>
                    <Input
                      id="prefix"
                      placeholder="For example, Customer:"
                      value={prefix}
                      onChange={(e) => setPrefix(e.target.value)}
                    />
                  </Field>
                  <Button type="submit" variant="outline">
                    <Search data-icon="inline-start" />
                    Find cells
                  </Button>
                </FieldGroup>
              </form>
            ) : (
              <Field>
                <FieldLabel htmlFor="filter">Filter this page</FieldLabel>
                <Input
                  id="filter"
                  placeholder={`Search ${kind} on this page…`}
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
              </Field>
            )}
            {params.get("cursor") ? (
              <Button
                variant="ghost"
                onClick={() => {
                  params.delete("cursor")
                  setParams(params)
                }}
              >
                Back to first page
              </Button>
            ) : null}
          </div>
          <Feedback loading={data.isPending} error={data.error}>
            {filtered.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    {(kind === "nodes"
                      ? [
                          "Node",
                          "Observed state",
                          "Owned / resident",
                          "Memory",
                          "Sampled",
                        ]
                      : kind === "cells"
                        ? [
                            "Cell scope",
                            "Kind",
                            "Observed owner",
                            "Epoch",
                            "Observation",
                          ]
                        : [
                            "Application",
                            "Deployment",
                            "Classes",
                            "Bindings",
                            "Role",
                          ]
                    ).map((label) => (
                      <TableHead key={label}>{label}</TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.map((item) => {
                    if ("lease_current" in item)
                      return (
                        <TableRow key={item.id}>
                          <TableCell>
                            <Link
                              className="entity-link"
                              to={encodeURIComponent(item.id)}
                            >
                              <span className="entity-icon">
                                <Server />
                              </span>
                              <span>
                                {item.id}
                                <small>{item.address}</small>
                              </span>
                            </Link>
                          </TableCell>
                          <TableCell>
                            <Status
                              label={
                                item.lease_current
                                  ? "Lease current"
                                  : "Lease expired"
                              }
                              tone={item.lease_current ? "good" : "warning"}
                            />
                          </TableCell>
                          <TableCell>
                            {item.load.owned_cells ?? "—"} /{" "}
                            {item.load.resident_cells}
                          </TableCell>
                          <TableCell>{bytes(item.load.rss_bytes)}</TableCell>
                          <TableCell>{date(item.load.sampled_ms)}</TableCell>
                        </TableRow>
                      )
                    if ("scope" in item)
                      return (
                        <TableRow key={item.scope}>
                          <TableCell>
                            <Link
                              className="entity-link"
                              to={encodeURIComponent(item.scope)}
                            >
                              <span className="entity-icon">
                                <Box />
                              </span>
                              <span className="scope-name">
                                {item.scope}
                                <small>{item.class ?? "Unclassified"}</small>
                              </span>
                            </Link>
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline">{item.kind}</Badge>
                          </TableCell>
                          <TableCell>
                            {item.owner ? (
                              <Link
                                className="text-link"
                                to={`../nodes/${encodeURIComponent(item.owner)}`}
                              >
                                {item.owner}
                              </Link>
                            ) : (
                              "No owner observed"
                            )}
                          </TableCell>
                          <TableCell>{item.epoch ?? "—"}</TableCell>
                          <TableCell>
                            <Status
                              label={
                                item.error ? "Unavailable" : "Stored state"
                              }
                              tone={item.error ? "warning" : "neutral"}
                            />
                          </TableCell>
                        </TableRow>
                      )
                    return (
                      <TableRow key={item.name}>
                        <TableCell>
                          <Link
                            className="entity-link"
                            to={encodeURIComponent(item.name)}
                          >
                            <span className="entity-icon">
                              <Box />
                            </span>
                            {item.name}
                          </Link>
                        </TableCell>
                        <TableCell>
                          <code>{item.version.slice(0, 14)}</code>
                        </TableCell>
                        <TableCell>{item.classes.length}</TableCell>
                        <TableCell>{item.bindings.length}</TableCell>
                        <TableCell>
                          <Badge variant="secondary">
                            {item.primary ? "Primary" : "Cohosted"}
                          </Badge>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            ) : !data.error ? (
              <Nothing
                title={`No ${kind} found`}
                description={
                  kind === "cells"
                    ? "Try another scope prefix, or run your application to create durable cells."
                    : "There are no matching records in this page. Refresh after publishing your application or starting a node."
                }
              />
            ) : null}
            <Coverage page={data.data} />
            <div className="flex justify-end">
              <NextPage
                cursor={data.data?.next_cursor}
                onNext={(cursor) => {
                  params.set("cursor", cursor)
                  setParams(params)
                }}
              />
            </div>
          </Feedback>
        </CardContent>
      </Card>
    </div>
  )
}
