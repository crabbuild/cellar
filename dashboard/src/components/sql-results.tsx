import { Download } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { JsonView } from "@/components/console-ui"

type ResultSet = { columns: string[]; rows: unknown[][] }
function isResult(
  value: unknown
): value is { results: ResultSet[]; duration?: number; count?: number } {
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
function display(value: unknown) {
  return value === null
    ? "NULL"
    : typeof value === "string"
      ? value
      : JSON.stringify(value)
}
export function SqlResults({ value }: { value: unknown }) {
  if (!isResult(value)) return <JsonView value={value} />
  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
        <span>
          {value.count ?? value.results.length} statements ·{" "}
          {value.duration?.toFixed(1) ?? "—"} ms
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            const url = URL.createObjectURL(
              new Blob([JSON.stringify(value, null, 2)], {
                type: "application/json",
              })
            )
            const anchor = document.createElement("a")
            anchor.href = url
            anchor.download = "cellar-query-results.json"
            anchor.click()
            URL.revokeObjectURL(url)
          }}
        >
          <Download data-icon="inline-start" />
          Export results
        </Button>
      </div>
      {value.results.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          Statement completed without result rows.
        </p>
      ) : (
        value.results.map((result, i) => (
          <div key={i} className="space-y-2">
            <p className="text-xs text-muted-foreground">
              Result {i + 1} · {result.rows.length} rows
            </p>
            <Table>
              <TableHeader>
                <TableRow>
                  {result.columns.map((name, j) => (
                    <TableHead key={j}>{name}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {result.rows.map((row, j) => (
                  <TableRow key={j}>
                    {row.map((cell, k) => (
                      <TableCell
                        key={k}
                        className="max-w-96 truncate font-mono text-xs"
                        title={display(cell)}
                      >
                        {display(cell)}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ))
      )}
    </div>
  )
}
