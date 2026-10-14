import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import { Database, History, Table2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Field, FieldLabel } from "@/components/ui/field"
import { Feedback } from "@/components/console-ui"
import { SqlResults } from "@/components/sql-results"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { fleetPath, request } from "@/lib/api"
import { useSession } from "@/lib/session"

type HistoryPage = {
  items: { id: number; name: string; applied_at: string }[]
  next_cursor: number | null
  table: string
  exists: boolean
}
type Schema = { name: string; kind: string; ddl: string }[]
export function SqlBrowser({
  fleet,
  application,
  binding,
}: {
  fleet: string
  application: string
  binding: string
}) {
  const { token, session } = useSession()
  const canRead = session.grants[fleet]?.includes("data_read") ?? false
  const [schema, setSchema] = useState<Schema>([])
  const [table, setTable] = useState("__sys_sql_migrations")
  const [history, setHistory] = useState<HistoryPage | null>(null)
  const [detail, setDetail] = useState<unknown>(null)
  const action = useMutation({
    mutationFn: async ({
      command,
      display,
    }: {
      command: Record<string, unknown>
      display: "schema" | "history" | "detail"
    }) => ({
      display,
      reply: await request<{ result: unknown }>(
        token,
        fleetPath(fleet, "/resources/action"),
        {
          method: "POST",
          body: JSON.stringify({
            application,
            binding,
            operation_id: crypto.randomUUID(),
            command,
          }),
        }
      ),
    }),
    onSuccess: ({ display, reply }) => {
      if (display === "history") setHistory(reply.result as HistoryPage)
      else if (display === "detail") setDetail(reply.result)
      else {
        const result = reply.result as { results?: { rows: unknown[][] }[] }
        setSchema(
          (result.results?.[0]?.rows ?? []).map(([name, kind, ddl]) => ({
            name: String(name),
            kind: String(kind),
            ddl: ddl == null ? "" : String(ddl),
          }))
        )
      }
    },
  })
  const inspect = (sql: string, display: "schema" | "detail") =>
    action.mutate({ command: { action: "sql", sql, readonly: true }, display })
  return (
    <Card>
      <CardHeader>
        <CardTitle>Database explorer</CardTitle>
        <CardDescription>
          Inspect up to 100 schema objects and browse applied migrations without
          modifying the database.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <Button
          variant="outline"
          disabled={!canRead || action.isPending}
          onClick={() =>
            inspect(
              "SELECT name, type, sql FROM sqlite_schema WHERE name NOT GLOB '__sys_*' AND name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 100",
              "schema"
            )
          }
        >
          <Database data-icon="inline-start" />
          Browse schema
        </Button>
        <Feedback loading={action.isPending} error={action.error}>
          {schema.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Definition</TableHead>
                  <TableHead>Inspect</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {schema.map((item) => (
                  <TableRow key={item.name}>
                    <TableCell className="font-mono">{item.name}</TableCell>
                    <TableCell>{item.kind}</TableCell>
                    <TableCell>
                      <pre className="max-w-lg text-xs break-all whitespace-pre-wrap">
                        {item.ddl}
                      </pre>
                    </TableCell>
                    <TableCell>
                      {["table", "view"].includes(item.kind) ? (
                        <div className="flex gap-2">
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() =>
                              inspect(
                                `PRAGMA table_info("${item.name.replaceAll('"', '""')}")`,
                                "detail"
                              )
                            }
                          >
                            Columns
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() =>
                              inspect(
                                `SELECT * FROM "${item.name.replaceAll('"', '""')}" LIMIT 100`,
                                "detail"
                              )
                            }
                          >
                            <Table2 />
                            Rows
                          </Button>
                        </div>
                      ) : null}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : action.isSuccess && action.data.display === "schema" ? (
            <p className="text-sm text-muted-foreground">
              No application schema objects found.
            </p>
          ) : null}
          {detail ? <SqlResults value={detail} /> : null}
        </Feedback>
        <div className="flex flex-wrap items-end gap-3">
          <Field className="max-w-sm">
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
            disabled={!canRead || action.isPending}
            onClick={() =>
              action.mutate({
                command: { action: "sql_migrations", table, after: 0 },
                display: "history",
              })
            }
          >
            <History data-icon="inline-start" />
            Load migration history
          </Button>
        </div>
        {history ? (
          <div className="space-y-3">
            <p className="text-xs text-muted-foreground">
              {history.table} ·{" "}
              {history.exists
                ? `${history.items.length} migrations on this page`
                : "No history table exists"}
            </p>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Migration</TableHead>
                  <TableHead>Applied at · UTC</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {history.items.map((item) => (
                  <TableRow key={item.id}>
                    <TableCell>{item.id}</TableCell>
                    <TableCell>{item.name}</TableCell>
                    <TableCell>{item.applied_at}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {history.next_cursor !== null ? (
              <Button
                variant="outline"
                disabled={action.isPending}
                onClick={() =>
                  action.mutate({
                    command: {
                      action: "sql_migrations",
                      table: history.table,
                      after: history.next_cursor,
                    },
                    display: "history",
                  })
                }
              >
                Next migrations
              </Button>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
