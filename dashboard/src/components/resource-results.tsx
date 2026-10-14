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
import { JsonView, Status } from "@/components/console-ui"
import { bytes, date } from "@/lib/api"

type RecordValue = Record<string, unknown>
function object(value: unknown): RecordValue {
  return value && typeof value === "object" ? (value as RecordValue) : {}
}
function array(value: unknown): RecordValue[] {
  return Array.isArray(value) ? value.map(object) : []
}
function metadata(value: unknown) {
  if (typeof value !== "string") return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}
function decode(value: unknown, encoding: unknown): Uint8Array | null {
  try {
    if (encoding === "base64" && typeof value === "string")
      return Uint8Array.from(atob(value), (c) => c.charCodeAt(0))
    if (
      Array.isArray(value) &&
      value.every((v) => Number.isInteger(v) && v >= 0 && v <= 255)
    )
      return Uint8Array.from(value)
  } catch {
    return null
  }
  return null
}
function ValuePreview({
  value,
  encoding,
}: {
  value: unknown
  encoding: unknown
}) {
  const buffer = decode(value, encoding)
  if (!buffer) return <JsonView value={value} />
  let text: string | null = null
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(buffer)
  } catch {
    /* Binary values stay encoded. */
  }
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <span className="text-xs text-muted-foreground">
          {bytes(buffer.length)} · {text === null ? "Binary" : "UTF-8"}
        </span>
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            const url = URL.createObjectURL(
              new Blob([Uint8Array.from(buffer)], {
                type: "application/octet-stream",
              })
            )
            const link = document.createElement("a")
            link.href = url
            link.download = "cellar-value.bin"
            link.click()
            URL.revokeObjectURL(url)
          }}
        >
          <Download data-icon="inline-start" />
          Download value
        </Button>
      </div>
      <JsonView
        value={
          text ?? {
            encoding: "base64",
            value:
              typeof value === "string"
                ? value
                : btoa(
                    Array.from(buffer, (b) => String.fromCharCode(b)).join("")
                  ),
          }
        }
      />
    </div>
  )
}
export function KvResults({
  value,
  pending,
  onRead,
  onNext,
}: {
  value: unknown
  pending: boolean
  onRead: (key: string) => void
  onNext: (after: string) => void
}) {
  const result = object(value)
  if (Array.isArray(result.keys)) {
    const keys = array(result.keys)
    return (
      <div className="space-y-4">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Key</TableHead>
              <TableHead>Expiration</TableHead>
              <TableHead>Metadata</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {keys.map((key, i) => (
              <TableRow key={i}>
                <TableCell>
                  <Button
                    variant="link"
                    disabled={pending}
                    onClick={() => onRead(String(key.name))}
                  >
                    {String(key.name)}
                  </Button>
                </TableCell>
                <TableCell>
                  {key.expiration == null
                    ? "No expiration"
                    : date(Number(key.expiration))}
                </TableCell>
                <TableCell className="max-w-60 truncate">
                  {key.metadata == null ? "—" : String(key.metadata)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <p className="text-xs text-muted-foreground">
          {keys.length} keys on this page ·{" "}
          {result.complete ? "End of listing" : "More keys available"}
        </p>
        {!result.complete && keys.length > 0 ? (
          <Button
            variant="outline"
            disabled={pending}
            onClick={() => onNext(String(keys.at(-1)?.name))}
          >
            Next 100 keys
          </Button>
        ) : null}
      </div>
    )
  }
  if (result.found === false)
    return (
      <p className="text-sm text-muted-foreground">
        This key was not found. It may have expired or been deleted.
      </p>
    )
  if (result.found === true)
    return (
      <div className="space-y-4">
        <dl className="detail-grid">
          <dt>Expiration</dt>
          <dd>
            {result.expiration == null
              ? "No expiration"
              : date(Number(result.expiration))}
          </dd>
          <dt>Storage type</dt>
          <dd>{String(result.tag)}</dd>
        </dl>
        <ValuePreview value={result.value} encoding={result.valueEncoding} />
        <h3 className="text-sm font-medium">Metadata</h3>
        <JsonView value={metadata(result.metadata)} />
      </div>
    )
  return <JsonView value={value} />
}
export function QueueResults({ value }: { value: unknown }) {
  const result = object(value)
  const metrics =
    typeof result.backlogCount === "number" ? result : object(result.metrics)
  return (
    <div className="space-y-4">
      {typeof metrics.backlogCount === "number" ? (
        <dl className="detail-grid">
          <dt>Backlog</dt>
          <dd>{metrics.backlogCount.toLocaleString()} messages</dd>
          <dt>Backlog size</dt>
          <dd>{bytes(Number(metrics.backlogBytes))}</dd>
          <dt>Oldest message</dt>
          <dd>
            {metrics.oldestMessageTimestamp == null
              ? "No retained messages"
              : date(Number(metrics.oldestMessageTimestamp))}
          </dd>
          {typeof metrics.paused === "boolean" ? (
            <>
              <dt>Delivery</dt>
              <dd>
                <Status
                  label={metrics.paused ? "Paused" : "Enabled"}
                  tone={metrics.paused ? "warning" : "good"}
                />
              </dd>
            </>
          ) : null}
        </dl>
      ) : null}
      {Array.isArray(result.messages) ? (
        <>
          <p className="text-xs text-muted-foreground">
            {result.messages.length} messages inspected without consuming them
            {result.truncated === true
              ? " · Preview stopped at its byte budget; additional messages are not shown."
              : ""}
          </p>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Message</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Attempts</TableHead>
                <TableHead>Visible at</TableHead>
                <TableHead>Payload</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {array(result.messages).map((message, i) => (
                <TableRow key={i}>
                  <TableCell className="font-mono text-xs">
                    {String(message.id)}
                  </TableCell>
                  <TableCell>
                    <Status label={String(message.state)} />
                  </TableCell>
                  <TableCell>{String(message.attempts)}</TableCell>
                  <TableCell>{date(Number(message.visibleAt))}</TableCell>
                  <TableCell>
                    <details>
                      <summary className="cursor-pointer text-primary">
                        {String(message.contentType)} ·{" "}
                        {bytes(Number(message.size))}
                      </summary>
                      <div className="mt-3 max-w-96 min-w-64">
                        <ValuePreview
                          value={message.bodyBase64}
                          encoding="base64"
                        />
                      </div>
                    </details>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </>
      ) : null}
      <details>
        <summary className="cursor-pointer text-xs text-muted-foreground">
          Complete broker response
        </summary>
        <div className="mt-3">
          <JsonView value={value} />
        </div>
      </details>
    </div>
  )
}
