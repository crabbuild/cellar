// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useDeferredValue, useMemo, useState } from "react"
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  Download,
  Maximize2,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Field, FieldLabel } from "@/components/ui/field"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { CopyValue, JsonView } from "@/components/console-ui"
import { download } from "@/lib/resource"

export type ResultSet = { columns: string[]; rows: unknown[][] }
export type SqlResult = {
  results: ResultSet[]
  duration?: number
  count?: number
}
export function isSqlResult(value: unknown): value is SqlResult {
  if (
    !value ||
    typeof value !== "object" ||
    !("results" in value) ||
    !Array.isArray(value.results)
  )
    return false
  return value.results.every(
    (r) =>
      r &&
      Array.isArray(r.columns) &&
      r.columns.every((c: unknown) => typeof c === "string") &&
      Array.isArray(r.rows) &&
      r.rows.every(Array.isArray)
  )
}

export function sqlDisplay(value: unknown): string {
  return value === null
    ? "NULL"
    : typeof value === "string"
      ? value
      : (JSON.stringify(value) ?? "")
}

function compare(a: unknown, b: unknown) {
  if (a === b) return 0
  if (a === null) return -1
  if (b === null) return 1
  return typeof a === "number" && typeof b === "number"
    ? a - b
    : sqlDisplay(a).localeCompare(sqlDisplay(b), undefined, { numeric: true })
}

function csv(result: ResultSet) {
  const field = (value: unknown) =>
    `"${(value === null ? "" : sqlDisplay(value)).replaceAll('"', '""')}"`
  return (
    [
      result.columns.map(field).join(","),
      ...result.rows.map((row) => row.map(field).join(",")),
    ].join("\r\n") + "\r\n"
  )
}

function ResultGrid({ result }: { result: ResultSet }) {
  const [filter, setFilter] = useState("")
  const [column, setColumn] = useState("all")
  const [sort, setSort] = useState<{
    column: number
    direction: 1 | -1
  } | null>(null)
  const [inspection, setInspection] = useState<{
    column: string
    row: number
    value: unknown
  } | null>(null)
  const search = useDeferredValue(filter.trim().toLocaleLowerCase())
  const rows = useMemo(() => {
    const filtered = result.rows
      .map((row, index) => ({ row, index }))
      .filter(
        ({ row }) =>
          !search ||
          (column === "all" ? row : [row[Number(column)]]).some((cell) =>
            sqlDisplay(cell).toLocaleLowerCase().includes(search)
          )
      )
    return sort
      ? filtered.toSorted(
          (a, b) =>
            sort.direction * compare(a.row[sort.column], b.row[sort.column])
        )
      : filtered
  }, [result, search, column, sort])
  const selected = { columns: result.columns, rows: rows.map(({ row }) => row) }
  return (
    <div className="result-grid">
      <div className="result-toolbar">
        <Field className="min-w-0 flex-1">
          <FieldLabel className="sr-only" htmlFor="result-filter">
            Filter loaded rows
          </FieldLabel>
          <Input
            id="result-filter"
            placeholder="Filter loaded rows…"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
        </Field>
        <Select value={column} onValueChange={setColumn}>
          <SelectTrigger aria-label="Filter column">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="all">All columns</SelectItem>
              {result.columns.map((name, index) => (
                <SelectItem key={index} value={String(index)}>
                  {name}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
        <span className="result-coverage">
          {rows.length} of {result.rows.length} loaded rows
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={() =>
            download(
              "cellar-query-results.csv",
              csv(selected),
              "text/csv;charset=utf-8"
            )
          }
        >
          <Download data-icon="inline-start" />
          CSV
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() =>
            download(
              "cellar-query-results.json",
              JSON.stringify(selected, null, 2),
              "application/json"
            )
          }
        >
          <Download data-icon="inline-start" />
          JSON
        </Button>
      </div>
      <div className="result-table-scroll">
        <Table>
          <TableHeader>
            <TableRow>
              {result.columns.map((name, index) => {
                const Icon =
                  sort?.column === index
                    ? sort.direction === 1
                      ? ArrowUp
                      : ArrowDown
                    : ArrowUpDown
                return (
                  <TableHead
                    key={index}
                    aria-label={name}
                    aria-sort={
                      sort?.column === index
                        ? sort.direction === 1
                          ? "ascending"
                          : "descending"
                        : "none"
                    }
                  >
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() =>
                        setSort((previous) => ({
                          column: index,
                          direction:
                            previous?.column === index &&
                            previous.direction === 1
                              ? -1
                              : 1,
                        }))
                      }
                      aria-label={`Sort by ${name}`}
                    >
                      {name}
                      <Icon data-icon="inline-end" />
                    </Button>
                  </TableHead>
                )
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map(({ row, index }) => (
              <TableRow key={index}>
                {row.map((cell, column) => (
                  <TableCell key={column}>
                    <button
                      type="button"
                      className="result-value"
                      title="Inspect full value"
                      onClick={() =>
                        setInspection({
                          column: result.columns[column],
                          row: index + 1,
                          value: cell,
                        })
                      }
                    >
                      <span>{sqlDisplay(cell)}</span>
                      <Maximize2 aria-hidden />
                    </button>
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!rows.length ? (
          <p className="workbench-empty">
            {search
              ? "No loaded rows match this filter."
              : "Statement completed without result rows."}
          </p>
        ) : null}
      </div>
      <Dialog
        open={inspection !== null}
        onOpenChange={(open) => {
          if (!open) setInspection(null)
        }}
      >
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>
              {inspection?.column} · Row {inspection?.row}
            </DialogTitle>
            <DialogDescription>
              Complete returned value ·{" "}
              {inspection?.value === null
                ? "SQL NULL"
                : typeof inspection?.value}
            </DialogDescription>
          </DialogHeader>
          <div className="full-value">
            <pre>{inspection ? sqlDisplay(inspection.value) : ""}</pre>
          </div>
          <div className="flex justify-end">
            <CopyValue value={inspection ? sqlDisplay(inspection.value) : ""} />
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

export function SqlResults({ value }: { value: unknown }) {
  if (!isSqlResult(value)) return <JsonView value={value} />
  return (
    <div className="sql-results">
      <div className="result-summary">
        <span>{value.count ?? value.results.length} statements</span>
        <span>{value.duration?.toFixed(1) ?? "—"} ms</span>
        <span>Filtering, sorting and exports use returned rows only.</span>
      </div>
      {value.results.length === 1 ? (
        <ResultGrid result={value.results[0]} />
      ) : value.results.length > 1 ? (
        <Tabs defaultValue="0" className="min-h-0 flex-1">
          {value.results.length > 1 ? (
            <TabsList variant="line" aria-label="Statement results">
              {value.results.map((_, index) => (
                <TabsTrigger key={index} value={String(index)}>
                  Result {index + 1}
                </TabsTrigger>
              ))}
            </TabsList>
          ) : null}
          {value.results.map((result, index) => (
            <TabsContent key={index} value={String(index)} className="min-h-0">
              <ResultGrid result={result} />
            </TabsContent>
          ))}
        </Tabs>
      ) : (
        <p className="workbench-empty">
          Statement completed without result rows.
        </p>
      )}
    </div>
  )
}
