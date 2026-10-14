import { useState } from "react"
import { Link, useParams, useSearchParams } from "react-router-dom"
import {
  ArrowUpRight,
  Braces,
  Clock3,
  Database,
  Layers3,
  Workflow,
} from "lucide-react"
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Field, FieldLabel } from "@/components/ui/field"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Coverage,
  Feedback,
  NextPage,
  Nothing,
  PageHeading,
  Refresh,
} from "@/components/console-ui"
import { fleetPath, useApi, type Application, type Page } from "@/lib/api"

const definitions = {
  sql: {
    title: "SQL databases",
    description: "Query durable data through the database cell that owns it.",
    kind: "sql",
    icon: Database,
  },
  kv: {
    title: "Key-value",
    description: "Inspect shared namespaces, values and expiration policies.",
    kind: "kv",
    icon: Braces,
  },
  queues: {
    title: "Queues",
    description: "Follow background work from delivery to settlement.",
    kind: "queue",
    icon: Layers3,
  },
  workflows: {
    title: "Workflows",
    description: "Inspect durable executions, step history and their outcomes.",
    kind: "workflow",
    icon: Workflow,
  },
  cron: {
    title: "Cron schedules",
    description:
      "Recurring work, configured per application and evaluated in UTC.",
    kind: "cron",
    icon: Clock3,
  },
}

export default function Resources({
  kind,
}: {
  kind: keyof typeof definitions
}) {
  const { fleet } = useParams()
  return <ResourceList key={`${fleet}:${kind}`} kind={kind} />
}

function ResourceList({ kind }: { kind: keyof typeof definitions }) {
  const [search, setSearch] = useState("")
  const { fleet = "" } = useParams()
  const [params, setParams] = useSearchParams()
  const data = useApi<Page<Application>>(
    fleetPath(fleet, `/applications?${params}`)
  )
  const definition = definitions[kind]
  const resources =
    data.data?.items.flatMap((app) =>
      kind === "cron"
        ? [
            {
              app,
              name: app.name,
              identity: `${app.crons.length + app.bindings.reduce((sum, b) => sum + b.schedules.length, 0)} declared expressions`,
              scope: null,
              kind: "cron",
              schedules: app.crons,
            },
          ]
        : app.bindings
            .filter((binding) => binding.kind === definition.kind)
            .map((binding) => ({ app, ...binding }))
    ) ?? []
  const filtered = resources.filter((resource) =>
    [resource.name, resource.app.name, resource.identity].some((value) =>
      value.toLowerCase().includes(search.trim().toLowerCase())
    )
  )
  const Icon = definition.icon
  return (
    <div className="page-stack">
      <PageHeading
        title={definition.title}
        description={definition.description}
      >
        <Refresh busy={data.isFetching} onClick={() => void data.refetch()} />
      </PageHeading>
      <Feedback loading={data.isPending} error={data.error}>
        {resources.length ? (
          <div className="table-toolbar">
            <Field>
              <FieldLabel htmlFor="resource-filter">
                Filter resources on this page
              </FieldLabel>
              <Input
                id="resource-filter"
                placeholder="Search by name, application or identity…"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </Field>
            <div className="flex items-center gap-3">
              <span className="text-sm text-muted-foreground" role="status">
                {filtered.length} of {resources.length} resources
              </span>
              {search ? (
                <Button variant="ghost" onClick={() => setSearch("")}>
                  Clear filter
                </Button>
              ) : null}
            </div>
          </div>
        ) : null}
        {filtered.length ? (
          <div className="resource-grid">
            {filtered.map((resource) => (
              <Card key={`${resource.app.name}/${resource.name}`}>
                <CardHeader>
                  <CardTitle>{resource.name}</CardTitle>
                  <CardDescription>{resource.app.name}</CardDescription>
                  <CardAction>
                    <span className="entity-icon">
                      <Icon />
                    </span>
                  </CardAction>
                </CardHeader>
                <CardContent>
                  <p className="font-mono text-xs break-all">
                    {resource.identity}
                  </p>
                  <div className="mt-4">
                    <Badge variant="outline">
                      {kind === "workflows" || kind === "cron"
                        ? "Application resource"
                        : "Shared resource"}
                    </Badge>
                  </div>
                </CardContent>
                <CardFooter>
                  <Button variant="ghost" size="sm" asChild>
                    <Link
                      to={`${encodeURIComponent(resource.app.name)}/${encodeURIComponent(resource.name)}`}
                    >
                      Open workspace
                      <ArrowUpRight data-icon="inline-end" />
                    </Link>
                  </Button>
                </CardFooter>
              </Card>
            ))}
          </div>
        ) : (
          <Card>
            <CardContent>
              <Nothing
                title={
                  resources.length
                    ? "No matching resources"
                    : `No ${definition.title.toLowerCase()} on this page`
                }
                description={
                  resources.length
                    ? "Try another name or clear the filter to see all resources on this page."
                    : "Declare a resource binding in your application configuration, then publish the application to this fleet."
                }
              />
            </CardContent>
          </Card>
        )}
        <Coverage page={data.data} />
        <NextPage
          cursor={data.data?.next_cursor}
          onNext={(cursor) => setParams({ cursor })}
        />
      </Feedback>
    </div>
  )
}
