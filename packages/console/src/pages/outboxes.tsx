// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { FleetEventInventory } from "@/components/event-inventory"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { useState } from "react"
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom"
import { Search, ArrowUpRight } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Field, FieldLabel } from "@/components/ui/field"
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
import {
  PageHeading,
  Feedback,
  Coverage,
  NextPage,
  Nothing,
  Refresh,
} from "@/components/console-ui"
import { EventNotice } from "@/components/event-ui"
import { useApi, fleetPath, type Page, type Cell } from "@/lib/api"

export default function Outboxes() {
  const { fleet = "" } = useParams()
  const [params, setParams] = useSearchParams()
  const [prefix, setPrefix] = useState(params.get("prefix") ?? "")
  const [scope, setScope] = useState("")
  const navigate = useNavigate()
  const data = useApi<Page<Cell>>(fleetPath(fleet, `/cells?${params}`))
  const cells =
    data.data?.items.filter(
      (cell) => cell.kind !== "event_stream" && cell.kind !== "event_catalog"
    ) ?? []
  const target = (scope: string) =>
    `/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(scope)}?tab=outbox`
  return (
    <div className="page-stack">
      <PageHeading
        title="Outboxes"
        description="Follow transactional events from a source cell to their destination stream."
      >
        <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
      </PageHeading>
      <Tabs defaultValue="sources">
        <TabsList variant="line">
          <TabsTrigger value="sources">Source cells</TabsTrigger>
          <TabsTrigger value="fleet">Fleet inspection</TabsTrigger>
        </TabsList>
        <TabsContent value="fleet">
          <FleetEventInventory fleet={fleet} />
        </TabsContent>
        <TabsContent value="sources">
          <EventNotice>
            Choose a source cell to inspect its outbox. The cell listing does
            not prove that every cell has emitted events. This view does not
            scan or activate all cells in your fleet.
          </EventNotice>
          <Card>
            <CardHeader>
              <CardTitle>Open a source directly</CardTitle>
              <CardDescription>
                Use a cell scope from application logs or the stream’s producer
                inventory.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <form
                className="flex flex-wrap items-end gap-3"
                onSubmit={(e) => {
                  e.preventDefault()
                  navigate(target(scope.trim()))
                }}
              >
                <Field className="min-w-56 flex-1">
                  <FieldLabel htmlFor="source-scope">
                    Source cell scope
                  </FieldLabel>
                  <Input
                    id="source-scope"
                    value={scope}
                    onChange={(e) => setScope(e.target.value)}
                    placeholder="Order:cell-id"
                  />
                </Field>
                <Button type="submit" disabled={!scope.trim().includes(":")}>
                  Inspect outbox
                  <ArrowUpRight />
                </Button>
              </form>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Find a source cell</CardTitle>
              <CardDescription>
                Root cell inventory with observed owners. Filter by scope prefix
                to narrow a large fleet.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <form
                className="flex flex-wrap items-end gap-3"
                onSubmit={(e) => {
                  e.preventDefault()
                  setParams(prefix ? { prefix } : {})
                }}
              >
                <Field className="min-w-48 flex-1">
                  <FieldLabel htmlFor="source-prefix">
                    Cell scope prefix
                  </FieldLabel>
                  <Input
                    id="source-prefix"
                    value={prefix}
                    onChange={(e) => setPrefix(e.target.value)}
                    placeholder="Order:"
                  />
                </Field>
                <Button variant="outline" type="submit">
                  <Search />
                  Find cells
                </Button>
                {params.get("cursor") && (
                  <Button
                    variant="ghost"
                    onClick={() => {
                      params.delete("cursor")
                      setParams(params)
                    }}
                  >
                    First page
                  </Button>
                )}
              </form>
              <Feedback loading={data.isPending} error={data.error}>
                {cells.length ? (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Source cell</TableHead>
                        <TableHead>Observed owner</TableHead>
                        <TableHead>Inspect</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {cells.map((cell) => (
                        <TableRow key={cell.scope}>
                          <TableCell>
                            <Link
                              className="text-link block max-w-lg truncate font-mono text-xs"
                              title={cell.scope}
                              to={target(cell.scope)}
                            >
                              {cell.scope}
                            </Link>
                          </TableCell>
                          <TableCell>
                            {cell.owner ? (
                              <Link
                                className="text-link"
                                to={`/fleets/${encodeURIComponent(fleet)}/nodes/${encodeURIComponent(cell.owner)}?tab=events`}
                              >
                                {cell.owner}
                              </Link>
                            ) : (
                              "No owner observed"
                            )}
                          </TableCell>
                          <TableCell>
                            <Button variant="outline" size="sm" asChild>
                              <Link to={target(cell.scope)}>
                                Outbox
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
                    title="No source candidates on this page"
                    description="Try another scope prefix or continue to the next cell page."
                  />
                )}
                <Coverage page={data.data} />
                <NextPage
                  cursor={data.data?.next_cursor}
                  onNext={(cursor) => {
                    params.set("cursor", cursor)
                    setParams(params)
                  }}
                />
              </Feedback>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
