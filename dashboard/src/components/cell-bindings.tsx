import { useState } from "react"
import { Link } from "react-router-dom"
import {
  ArrowRight,
  ArrowUpRight,
  Box,
  Boxes,
  Braces,
  ChevronRight,
  Database,
  HardDrive,
  Layers3,
  Network,
  Workflow,
  type LucideIcon,
} from "lucide-react"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Feedback, Coverage, Nothing } from "@/components/console-ui"
import {
  fleetPath,
  useApi,
  type Page,
  type Application,
  type Binding,
  type Cell,
} from "@/lib/api"

type ResourceGroup = "Storage" | "Background work" | "Other resources"
type BindingType = {
  label: string
  icon: LucideIcon
  group: ResourceGroup
  route?: string
}

const bindingTypes: Record<string, BindingType> = {
  event_stream: {
    label: "Event stream",
    icon: Network,
    group: "Background work",
    route: "streams",
  },
  sql: {
    label: "SQL database",
    icon: Database,
    group: "Storage",
    route: "sql",
  },
  kv: {
    label: "Key-value namespace",
    icon: Braces,
    group: "Storage",
    route: "kv",
  },
  blob_bucket: { label: "Blob bucket", icon: HardDrive, group: "Storage" },
  queue: {
    label: "Queue",
    icon: Layers3,
    group: "Background work",
    route: "queues",
  },
  workflow: {
    label: "Workflow",
    icon: Workflow,
    group: "Background work",
    route: "workflows",
  },
  durable_object_namespace: {
    label: "Cell namespace",
    icon: Box,
    group: "Other resources",
  },
  service: { label: "Service", icon: Network, group: "Other resources" },
}
const resourceGroups: ResourceGroup[] = [
  "Storage",
  "Background work",
  "Other resources",
]

function bindingType(kind: string): BindingType {
  return (
    bindingTypes[kind] ?? {
      label: kind.replaceAll("_", " "),
      icon: Box,
      group: "Other resources",
    }
  )
}

function cellRelation(binding: Binding, cell: Cell) {
  if (binding.scope === cell.scope) return "Backing cell"
  if (
    binding.kind === "durable_object_namespace" &&
    binding.identity === cell.class
  )
    return "Cell class"
  return null
}

function BindingRow({
  fleet,
  app,
  binding,
  relation,
}: {
  fleet: string
  app: string
  binding: Binding
  relation?: string | null
}) {
  const { label, icon: Icon, route } = bindingType(binding.kind)
  const content = (
    <>
      <span className="binding-icon" aria-hidden>
        <Icon />
      </span>
      <span className="binding-details">
        <code className="binding-name">{binding.name}</code>
        <span className="binding-description">
          {label}
          <span aria-hidden> · </span>
          {binding.identity}
        </span>
      </span>
      {relation ? <Badge variant="secondary">{relation}</Badge> : null}
      {route ? <ArrowRight className="binding-arrow" aria-hidden /> : null}
    </>
  )
  return (
    <li className="binding-row">
      {route ? (
        <Link
          className="binding-resource-link"
          aria-label={`Open ${binding.name} ${label.toLowerCase()} workspace`}
          to={`/fleets/${encodeURIComponent(fleet)}/${route}/${encodeURIComponent(app)}/${encodeURIComponent(binding.name)}`}
        >
          {content}
        </Link>
      ) : (
        <div className="binding-resource">{content}</div>
      )}
    </li>
  )
}

function ApplicationBindings({
  fleet,
  app,
  cell,
}: {
  fleet: string
  app: Application
  cell: Cell
}) {
  const matching = app.bindings.filter((binding) => cellRelation(binding, cell))
  const remaining = app.bindings.filter(
    (binding) => !cellRelation(binding, cell)
  )
  const namespaces = remaining.filter(
    (binding) => binding.kind === "durable_object_namespace"
  )
  const resources = remaining.filter(
    (binding) => binding.kind !== "durable_object_namespace"
  )
  return (
    <section
      className="binding-application"
      aria-label={`${app.name} bindings`}
    >
      <header className="binding-application-header">
        <h3>
          <Link
            className="entity-link"
            to={`/fleets/${encodeURIComponent(fleet)}/applications/${encodeURIComponent(app.name)}`}
          >
            <span className="entity-icon" aria-hidden>
              <Boxes />
            </span>
            <span className="binding-app-name">{app.name}</span>
            <ArrowUpRight className="binding-arrow" aria-hidden />
          </Link>
        </h3>
        <span className="binding-count">
          {app.bindings.length}{" "}
          {app.bindings.length === 1 ? "binding" : "bindings"}
        </span>
      </header>
      {matching.length ? (
        <section className="binding-group" aria-label="Bindings for this cell">
          <h4>Bindings for this cell</h4>
          <ul className="binding-list binding-matching">
            {matching.map((binding) => (
              <BindingRow
                key={binding.name}
                fleet={fleet}
                app={app.name}
                binding={binding}
                relation={cellRelation(binding, cell)}
              />
            ))}
          </ul>
        </section>
      ) : null}
      {resources.length ? (
        <div className="binding-resource-groups">
          {resourceGroups.map((group) => {
            const bindings = resources.filter(
              (binding) => bindingType(binding.kind).group === group
            )
            return bindings.length ? (
              <section key={group} className="binding-group" aria-label={group}>
                <h4>{group}</h4>
                <ul className="binding-list">
                  {bindings.map((binding) => (
                    <BindingRow
                      key={binding.name}
                      fleet={fleet}
                      app={app.name}
                      binding={binding}
                    />
                  ))}
                </ul>
              </section>
            ) : null
          })}
        </div>
      ) : null}
      {namespaces.length ? (
        <Collapsible className="binding-namespaces">
          <CollapsibleTrigger asChild>
            <Button variant="ghost" className="binding-disclosure">
              <ChevronRight data-icon="inline-start" aria-hidden />
              {matching.length ? "Other cell namespaces" : "Cell namespaces"}
              <Badge variant="secondary">{namespaces.length}</Badge>
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <ul
              className="binding-list binding-namespace-list"
              aria-label="Cell namespaces"
            >
              {namespaces.map((binding) => (
                <BindingRow
                  key={binding.name}
                  fleet={fleet}
                  app={app.name}
                  binding={binding}
                />
              ))}
            </ul>
          </CollapsibleContent>
        </Collapsible>
      ) : null}
      {!app.bindings.length ? (
        <p className="text-sm text-muted-foreground">
          This application declares the cell class but has no resource bindings.
        </p>
      ) : null}
    </section>
  )
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
        <CardTitle>Application bindings</CardTitle>
        <CardDescription>
          Declared resources in applications associated with this cell. These
          declarations do not indicate resource usage.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <Feedback loading={apps.isPending} error={apps.error}>
          {matches.map((app) => (
            <ApplicationBindings
              key={app.name}
              fleet={fleet}
              app={app}
              cell={cell}
            />
          ))}
          {!apps.isPending && matches.length === 0 ? (
            <Nothing
              title="No matching applications on this page"
              description="No application on this page declares this cell class or references its backing cell."
            />
          ) : null}
          <Coverage page={apps.data} />
          {apps.data?.next_cursor ? (
            <Button
              variant="outline"
              size="sm"
              className="self-start"
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
