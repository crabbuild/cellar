// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Link } from "react-router-dom"
import { useMutation } from "@tanstack/react-query"
import { Boxes } from "lucide-react"
import { Button } from "@/components/ui/button"
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
import { Feedback, Status } from "@/components/console-ui"
import { date, fleetPath, request, type Node } from "@/lib/api"
import { useSession } from "@/lib/session"
type Page = {
  authoritative: boolean
  observed_at: number
  next_cursor: string | null
  items: {
    scope: string
    epoch: number
    phase: string
    active_requests: number
    alarm_at: number | null
    alarm_firing: boolean
  }[]
}
export function NodeCells({ fleet, node }: { fleet: string; node: Node }) {
  const { token } = useSession()
  const action = useMutation({
    mutationFn: (after: string | null) =>
      request<{ result: Page }>(
        token,
        fleetPath(fleet, `/nodes/${encodeURIComponent(node.id)}/action`),
        {
          method: "POST",
          body: JSON.stringify({
            generation: node.generation,
            operation_id: crypto.randomUUID(),
            action: { cells: { after, limit: 100 } },
          }),
        }
      ),
  })
  const page = action.data?.result
  return (
    <Card className="mt-5">
      <CardHeader>
        <CardTitle>Owned cells</CardTitle>
        <CardDescription>
          Read live observations from this node. Each page scans at most 100
          routing records; ownership can change between pages.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <Button
          variant="outline"
          disabled={action.isPending || !node.lease_current}
          onClick={() => action.mutate(null)}
        >
          <Boxes data-icon="inline-start" />
          Inspect owned cells
        </Button>
        <Feedback loading={action.isPending} error={action.error}>
          {page ? (
            <>
              <p className="text-xs text-muted-foreground">
                Observed {date(page.observed_at)} ·{" "}
                {page.authoritative
                  ? "Node authority confirmed"
                  : "Node authority unavailable; observations may be stale"}
              </p>
              <Table>
                <TableHeader>
                  <TableRow>
                    {["Cell", "Epoch", "Phase", "Active requests", "Alarm"].map(
                      (name) => (
                        <TableHead key={name}>{name}</TableHead>
                      )
                    )}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {page.items.map((cell) => (
                    <TableRow key={cell.scope}>
                      <TableCell>
                        <Link
                          className="text-link font-mono text-xs"
                          to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(cell.scope)}`}
                        >
                          {cell.scope}
                        </Link>
                        <div className="mt-1 flex gap-3 text-xs">
                          {!/^__Event(Stream|Catalog):/.test(cell.scope) && (
                            <Link
                              className="text-link"
                              to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(cell.scope)}?tab=outbox`}
                            >
                              Outbox
                            </Link>
                          )}
                          <Link
                            className="text-link"
                            to={`/fleets/${encodeURIComponent(fleet)}/cells/${encodeURIComponent(cell.scope)}?tab=events`}
                          >
                            Event state
                          </Link>
                        </div>
                      </TableCell>
                      <TableCell>{cell.epoch}</TableCell>
                      <TableCell>
                        <Status label={cell.phase.replaceAll("_", " ")} />
                      </TableCell>
                      <TableCell>{cell.active_requests}</TableCell>
                      <TableCell>
                        {cell.alarm_at == null
                          ? "None observed"
                          : `${date(cell.alarm_at)}${cell.alarm_firing ? " · Firing" : ""}`}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <p className="text-xs text-muted-foreground">
                {page.items.length} owned cells on this page ·{" "}
                {page.next_cursor
                  ? "More routing records available"
                  : "End of current node inventory"}
              </p>
              {page.next_cursor ? (
                <Button
                  variant="outline"
                  disabled={action.isPending}
                  onClick={() => action.mutate(page.next_cursor)}
                >
                  Next owned cells
                </Button>
              ) : null}
            </>
          ) : null}
        </Feedback>
      </CardContent>
    </Card>
  )
}
