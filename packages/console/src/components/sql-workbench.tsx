// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useMemo, useRef, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { History, ListTree, Play, Plus, X } from "lucide-react"
import { toast } from "sonner"
import type { SQLNamespace } from "@codemirror/lang-sql"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { CodeEditor, type CodeEditorHandle } from "@/components/code-editor"
import { SqlBrowser, type SchemaObject } from "@/components/sql-browser"
import { isSqlResult, SqlResults } from "@/components/sql-results"
import {
  resourceRequest,
  useResourceAction,
  type ResourceTarget,
} from "@/lib/resource"
import { useSession } from "@/lib/session"

type QueryTab = {
  id: string
  name: string
  sql: string
  result?: unknown
  error?: string
  runId?: string
}
type Run = {
  id: string
  tab: string
  sql: string
  readonly: boolean
  at: number
  duration?: number
  status: "succeeded" | "failed" | "unknown"
}
const initialSql =
  "SELECT name, type FROM sqlite_schema\nWHERE name NOT GLOB '__sys_*' AND name NOT GLOB 'sqlite_*'\nORDER BY name LIMIT 100;"
const schemaSql =
  "SELECT name, type, sql FROM sqlite_schema WHERE name NOT GLOB '__sys_*' AND name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 100;\nSELECT m.name, p.name, p.type, p.pk FROM (SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND name NOT GLOB '__sys_*' AND name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 100) AS m JOIN pragma_table_info(m.name) AS p ORDER BY m.name, p.cid LIMIT 1000;"

export default function SqlWorkbench({
  target: currentTarget,
  canRead,
  canWrite,
}: {
  target: ResourceTarget
  canRead: boolean
  canWrite: boolean
}) {
  const [target] = useState(currentTarget)
  const targetChanged = currentTarget.identity !== target.identity
  const { token } = useSession()
  const [tabs, setTabs] = useState<QueryTab[]>([
    { id: "query-1", name: "Query 1", sql: initialSql },
  ])
  const [active, setActive] = useState("query-1")
  const [mode, setMode] = useState("read")
  const [history, setHistory] = useState<Run[]>([])
  const [historyOpen, setHistoryOpen] = useState(false)
  const [confirmation, setConfirmation] = useState<{
    tab: string
    sql: string
  } | null>(null)
  const editor = useRef<CodeEditorHandle>(null)
  const sequence = useRef(1)
  const running = useRef(false)
  const action = useResourceAction(target)
  const current = tabs.find((tab) => tab.id === active) ?? tabs[0]
  const schemaQuery = useQuery({
    queryKey: [
      "sql-schema",
      target.fleet,
      target.application,
      target.binding,
      target.identity,
    ],
    queryFn: async ({ signal }) => {
      const reply = await resourceRequest(
        token,
        target,
        { action: "sql", sql: schemaSql, readonly: true },
        signal
      )
      if (!isSqlResult(reply.result))
        throw new Error("The database returned an unreadable schema.")
      const objects: SchemaObject[] = (reply.result.results[0]?.rows ?? []).map(
        ([name, kind, ddl]) => ({
          name: String(name),
          kind: String(kind ?? "table"),
          ddl: String(ddl ?? ""),
          columns: [],
        })
      )
      const byName = new Map(objects.map((object) => [object.name, object]))
      for (const [table, name, type, primary] of reply.result.results[1]
        ?.rows ?? [])
        byName.get(String(table))?.columns.push({
          name: String(name),
          type: String(type ?? ""),
          primary: Number(primary) > 0,
        })
      return objects
    },
    enabled: canRead && !targetChanged,
    staleTime: 60_000,
    refetchOnWindowFocus: false,
    retry: false,
  })
  const schema = useMemo<SQLNamespace>(
    () =>
      Object.fromEntries(
        (schemaQuery.data ?? []).map((object) => [
          object.name,
          object.columns.map((column) => column.name),
        ])
      ),
    [schemaQuery.data]
  )
  const run = async (tab: string, sql: string, readonly: boolean) => {
    if (
      running.current ||
      targetChanged ||
      !sql.trim() ||
      !(readonly ? canRead : canRead && canWrite)
    )
      return
    running.current = true
    const id = crypto.randomUUID()
    const at = Date.now()
    setTabs((tabs) =>
      tabs.map((item) =>
        item.id === tab ? { ...item, error: undefined } : item
      )
    )
    try {
      const reply = await action.mutateAsync({ action: "sql", sql, readonly })
      if (reply.replayed)
        throw new Error(
          "This write was already recorded. Inspect its outcome in Operations."
        )
      setTabs((tabs) =>
        tabs.map((item) =>
          item.id === tab ? { ...item, result: reply.result, runId: id } : item
        )
      )
      setHistory((items) =>
        [
          {
            id,
            tab,
            sql,
            readonly,
            at,
            status: "succeeded",
            duration: isSqlResult(reply.result)
              ? reply.result.duration
              : undefined,
          } satisfies Run,
          ...items,
        ].slice(0, 40)
      )
      if (!readonly) {
        toast.success("SQL changes applied")
        void schemaQuery.refetch()
      }
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Query did not return a result."
      setTabs((tabs) =>
        tabs.map((item) =>
          item.id === tab ? { ...item, error: message } : item
        )
      )
      setHistory((items) =>
        [
          {
            id,
            tab,
            sql,
            readonly,
            at,
            status: readonly ? "failed" : "unknown",
          } satisfies Run,
          ...items,
        ].slice(0, 40)
      )
    } finally {
      running.current = false
    }
  }
  const requestRun = (query: string) => {
    if (mode === "read") void run(current.id, query, true)
    else setConfirmation({ tab: current.id, sql: query })
  }
  const createTab = (name?: string, sql = "", execute = false) => {
    if (tabs.length >= 10) {
      toast.info(
        "Close a query tab before opening another. Up to 10 tabs are supported."
      )
      return
    }
    const number = ++sequence.current
    const id = `query-${number}`
    setTabs((tabs) => [...tabs, { id, name: name ?? `Query ${number}`, sql }])
    setActive(id)
    if (execute) void run(id, sql, true)
  }
  return (
    <section
      className="data-workbench sql-workbench"
      aria-label="SQL workbench"
    >
      <SqlBrowser
        target={target}
        objects={schemaQuery.data ?? []}
        pending={schemaQuery.isFetching}
        error={schemaQuery.error}
        refresh={() => void schemaQuery.refetch()}
        canRead={canRead && !targetChanged}
        inspect={(name, sql) => createTab(name, sql, true)}
        insert={(identifier) => editor.current?.insert(identifier)}
      />
      <div className="workbench-main">
        {targetChanged ? (
          <Alert>
            <AlertTitle>Database binding changed</AlertTitle>
            <AlertDescription>
              Copy your queries before reopening this workspace. They remain
              tied to {target.identity}.
            </AlertDescription>
          </Alert>
        ) : null}
        <div className="query-tabbar">
          <Tabs value={active} onValueChange={setActive}>
            <TabsList variant="line" aria-label="SQL query tabs">
              {tabs.map((tab) => (
                <TabsTrigger
                  key={tab.id}
                  value={tab.id}
                  id={`sql-tab-${tab.id}`}
                  aria-controls={`sql-panel-${tab.id}`}
                >
                  {tab.name}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="New query tab"
            onClick={() => createTab()}
          >
            <Plus />
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label={`Close ${current.name}`}
            disabled={tabs.length === 1 || action.isPending}
            onClick={() => {
              const remaining = tabs.filter((tab) => tab.id !== active)
              setTabs(remaining)
              setActive(remaining[0].id)
            }}
          >
            <X />
          </Button>
        </div>
        <div className="query-toolbar">
          <Select value={mode} onValueChange={setMode}>
            <SelectTrigger aria-label="Query mode">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value="read" disabled={!canRead}>
                  Read only
                </SelectItem>
                <SelectItem value="write" disabled={!canRead || !canWrite}>
                  Read and write
                </SelectItem>
              </SelectGroup>
            </SelectContent>
          </Select>
          <Button
            size="sm"
            disabled={
              action.isPending ||
              targetChanged ||
              !current.sql.trim() ||
              !(mode === "read" ? canRead : canRead && canWrite)
            }
            onClick={() => requestRun(editor.current?.query() ?? current.sql)}
          >
            <Play data-icon="inline-start" />
            {action.isPending ? "Running…" : "Run statement"}
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={
              !canRead ||
              targetChanged ||
              action.isPending ||
              !current.sql.trim()
            }
            onClick={() => {
              const query = editor.current?.query() ?? current.sql
              void run(current.id, `EXPLAIN QUERY PLAN ${query}`, true)
            }}
          >
            <ListTree data-icon="inline-start" />
            Explain query
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-expanded={historyOpen}
            onClick={() => setHistoryOpen((open) => !open)}
          >
            <History data-icon="inline-start" />
            History {history.length ? `(${history.length})` : ""}
          </Button>
        </div>
        <div
          className="sql-pane-stack"
          role="tabpanel"
          id={`sql-panel-${current.id}`}
          aria-labelledby={`sql-tab-${current.id}`}
        >
          <ResizablePanelGroup orientation="vertical">
            <ResizablePanel defaultSize="48%" minSize={150}>
              <div className="query-editor-pane">
                <CodeEditor
                  key={current.id}
                  ref={editor}
                  label="SQL statement"
                  value={current.sql}
                  schema={schema}
                  onChange={(sql) =>
                    setTabs((tabs) =>
                      tabs.map((tab) =>
                        tab.id === current.id ? { ...tab, sql } : tab
                      )
                    )
                  }
                  onRun={requestRun}
                  readOnly={!canRead}
                />
              </div>
            </ResizablePanel>
            <ResizableHandle
              withHandle
              aria-label="Resize SQL editor and results"
            />
            <ResizablePanel minSize={160}>
              <div className="query-results-pane" aria-label="Query results">
                {current.error ? (
                  <Alert variant="destructive">
                    <AlertTitle>Query did not return success</AlertTitle>
                    <AlertDescription>
                      {current.error}
                      {mode === "write"
                        ? " Inspect Operations before retrying a write; its outcome may be unknown."
                        : ""}
                    </AlertDescription>
                  </Alert>
                ) : null}
                {current.result ? (
                  <SqlResults key={current.runId} value={current.result} />
                ) : (
                  <div className="workbench-empty">
                    <ListTree />
                    <p>Run a query to explore its results.</p>
                    <small>
                      Run selected SQL with ⌘/Ctrl + Enter. Add Shift to run the
                      whole editor.
                    </small>
                  </div>
                )}
              </div>
            </ResizablePanel>
          </ResizablePanelGroup>
        </div>
        <div className="workbench-status">
          <span>
            {target.application} / {target.binding}
          </span>
          <span>1,000 rows / result · 1 MiB · bounded execution</span>
          <span>⌘/Ctrl + Enter runs selection</span>
        </div>
        {historyOpen ? (
          <section className="query-history" aria-label="Session query history">
            <div className="flex items-center justify-between gap-3">
              <h3>Session query history</h3>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setHistory([])}
                disabled={!history.length}
              >
                Clear history
              </Button>
            </div>
            <p className="workspace-caption">
              Last 40 runs in this workspace. Cleared when the page reloads or
              you disconnect.
            </p>
            {history.length ? (
              <ol>
                {history.map((item) => (
                  <li key={item.id}>
                    <button
                      type="button"
                      onClick={() => createTab("History query", item.sql)}
                    >
                      <span>
                        <code>{item.sql}</code>
                        <small>
                          {new Date(item.at).toLocaleTimeString()} ·{" "}
                          {item.readonly ? "Read only" : "Read and write"} ·{" "}
                          {item.status}
                          {item.duration == null
                            ? ""
                            : ` · ${item.duration.toFixed(1)} ms`}
                        </small>
                      </span>
                      <span>Open query</span>
                    </button>
                  </li>
                ))}
              </ol>
            ) : (
              <p className="workbench-empty">
                Run a query to add it to this session’s history.
              </p>
            )}
          </section>
        ) : null}
      </div>
      <AlertDialog
        open={confirmation !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmation(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Execute SQL</AlertDialogTitle>
            <AlertDialogDescription>
              Fleet: {target.fleet} · {target.application} / {target.binding}.
              Review the selected SQL before applying changes.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <pre className="query-confirmation">{confirmation?.sql}</pre>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (confirmation)
                  void run(confirmation.tab, confirmation.sql, false)
                setConfirmation(null)
              }}
            >
              Confirm operation
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  )
}
