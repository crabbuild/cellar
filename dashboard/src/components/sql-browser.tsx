import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import {
  ChevronDown,
  Database,
  History,
  KeyRound,
  RefreshCw,
  Table2,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Field, FieldLabel } from "@/components/ui/field"
import { Feedback } from "@/components/console-ui"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { resourceRequest, type ResourceTarget } from "@/lib/resource"
import { useSession } from "@/lib/session"

export type SchemaObject = {
  name: string
  kind: string
  ddl: string
  columns: { name: string; type: string; primary: boolean }[]
}
type HistoryPage = {
  items: { id: number; name: string; applied_at: string }[]
  next_cursor: number | null
  table: string
  exists: boolean
}

export function SqlBrowser({
  target,
  objects,
  pending,
  error,
  refresh,
  inspect,
  insert,
  canRead,
}: {
  target: ResourceTarget
  objects: SchemaObject[]
  pending: boolean
  error: Error | null
  refresh: () => void
  inspect: (name: string, query: string) => void
  insert: (identifier: string) => void
  canRead: boolean
}) {
  const { token } = useSession()
  const [search, setSearch] = useState("")
  const [table, setTable] = useState("__sys_sql_migrations")
  const [history, setHistory] = useState<HistoryPage | null>(null)
  const action = useMutation({
    mutationFn: async (command: Record<string, unknown>) =>
      (await resourceRequest(token, target, command)).result as HistoryPage,
    onSuccess: setHistory,
    retry: false,
  })
  const quoted = (name: string) => `"${name.replaceAll('"', '""')}"`
  const filtered = objects.filter((object) =>
    object.name.toLocaleLowerCase().includes(search.toLocaleLowerCase())
  )
  return (
    <aside className="workbench-sidebar" aria-label="Database explorer">
      <div className="workbench-sidebar-heading">
        <h2>
          <Database />
          Database explorer
        </h2>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Browse schema"
          title="Refresh schema"
          disabled={!canRead || pending}
          onClick={refresh}
        >
          <RefreshCw />
        </Button>
      </div>
      <Field>
        <FieldLabel htmlFor="schema-filter" className="sr-only">
          Find schema object
        </FieldLabel>
        <Input
          id="schema-filter"
          placeholder="Find a table or view…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </Field>
      <div className="schema-tree">
        <Feedback loading={pending && objects.length === 0} error={error}>
          {filtered.map((object) => (
            <details key={object.name} className="schema-object">
              <summary>
                <ChevronDown />
                <Table2 />
                <span>{object.name}</span>
                <small>{object.kind}</small>
              </summary>
              <div className="schema-object-actions">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!canRead}
                  onClick={() =>
                    inspect(
                      object.name,
                      `SELECT * FROM ${quoted(object.name)} LIMIT 100;`
                    )
                  }
                >
                  Rows
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!canRead}
                  onClick={() =>
                    inspect(
                      `${object.name} columns`,
                      `PRAGMA table_info(${quoted(object.name)});`
                    )
                  }
                >
                  Columns
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => insert(quoted(object.name))}
                >
                  Insert name
                </Button>
              </div>
              <ul
                className="schema-columns"
                aria-label={`${object.name} columns`}
              >
                {object.columns.map((column) => (
                  <li key={column.name}>
                    <button
                      type="button"
                      onClick={() =>
                        insert(`${quoted(object.name)}.${quoted(column.name)}`)
                      }
                      title="Insert column into query"
                    >
                      {column.primary ? (
                        <KeyRound aria-hidden />
                      ) : (
                        <span className="schema-column-dot" aria-hidden />
                      )}
                      <span>{column.name}</span>
                      <small>{column.type}</small>
                    </button>
                  </li>
                ))}
              </ul>
              <details className="schema-definition">
                <summary>Definition</summary>
                <pre>{object.ddl || "No definition returned."}</pre>
              </details>
            </details>
          ))}
          {!pending && !filtered.length ? (
            <p className="workbench-empty">
              {search
                ? "No loaded schema objects match."
                : "No application tables or views found."}
            </p>
          ) : null}
        </Feedback>
      </div>
      <p className="workspace-caption">
        Up to 100 schema objects and 1,000 columns. Framework tables are
        protected.
      </p>
      <details className="schema-migrations">
        <summary>
          <History />
          Migration history
        </summary>
        <div className="flex flex-col gap-3">
          <Field>
            <FieldLabel htmlFor="migration-table">
              Migration history table
            </FieldLabel>
            <Input
              id="migration-table"
              value={table}
              onChange={(event) => setTable(event.target.value)}
            />
          </Field>
          <Button
            variant="outline"
            size="sm"
            disabled={!canRead || action.isPending}
            onClick={() =>
              action.mutate({ action: "sql_migrations", table, after: 0 })
            }
          >
            Load migration history
          </Button>
          <Feedback error={action.error}>
            {history ? (
              <>
                <p className="workspace-caption">
                  {history.table} ·{" "}
                  {history.exists
                    ? `${history.items.length} migrations on this page`
                    : "No history table exists"}
                </p>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Migration</TableHead>
                      <TableHead>Applied · UTC</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {history.items.map((item) => (
                      <TableRow key={item.id}>
                        <TableCell>{item.name}</TableCell>
                        <TableCell>{item.applied_at}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                {history.next_cursor !== null ? (
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={action.isPending}
                    onClick={() =>
                      action.mutate({
                        action: "sql_migrations",
                        table: history.table,
                        after: history.next_cursor,
                      })
                    }
                  >
                    Next migrations
                  </Button>
                ) : null}
              </>
            ) : null}
          </Feedback>
        </div>
      </details>
    </aside>
  )
}
