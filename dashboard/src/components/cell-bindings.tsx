import { useState } from "react"
import { Link } from "react-router-dom"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Feedback, Coverage } from "@/components/console-ui"
import {
  fleetPath,
  useApi,
  type Page,
  type Application,
  type Cell,
} from "@/lib/api"

const routes: Record<string, string> = {
  sql: "sql",
  kv: "kv",
  queue: "queues",
  workflow: "workflows",
}
export function CellBindings({ fleet, cell }: { fleet: string; cell: Cell }) {
  const [cursor, setCursor] = useState<string | null>(null)
  const apps = useApi<Page<Application>>(
    fleetPath(
      fleet,
      `/applications?limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`
    )
  )
  const matches =
    apps.data?.items.filter(
      (app) =>
        app.classes.includes(cell.class ?? "") ||
        app.bindings.some((binding) => binding.scope === cell.scope)
    ) ?? []
  return (
    <Card className="mt-5">
      <CardHeader>
        <CardTitle>Available application bindings</CardTitle>
        <CardDescription>
          Applications that declare this class or reference this backing cell.
          Shared resources may be used by many cells; these declarations do not
          show usage.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <Feedback loading={apps.isPending} error={apps.error}>
          {matches.map((app) => (
            <div key={app.name} className="space-y-3">
              <Link
                className="text-link text-sm"
                to={`/fleets/${encodeURIComponent(fleet)}/applications/${encodeURIComponent(app.name)}`}
              >
                {app.name}
              </Link>
              <div className="flex flex-wrap gap-2">
                {app.bindings.map((binding) =>
                  routes[binding.kind] ? (
                    <Button
                      key={binding.name}
                      variant="outline"
                      size="sm"
                      asChild
                    >
                      <Link
                        to={`/fleets/${encodeURIComponent(fleet)}/${routes[binding.kind]}/${encodeURIComponent(app.name)}/${encodeURIComponent(binding.name)}`}
                      >
                        {binding.name}
                        <Badge variant="secondary">{binding.kind}</Badge>
                      </Link>
                    </Button>
                  ) : (
                    <Badge key={binding.name} variant="outline">
                      {binding.name} · {binding.kind}
                    </Badge>
                  )
                )}
              </div>
            </div>
          ))}
          {!apps.isPending && matches.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No matching application declaration on this page.
            </p>
          ) : null}
          <Coverage page={apps.data} />
          {apps.data?.next_cursor ? (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCursor(apps.data!.next_cursor)}
            >
              Next application page
            </Button>
          ) : null}
        </Feedback>
      </CardContent>
    </Card>
  )
}
