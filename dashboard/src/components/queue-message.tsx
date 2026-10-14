import { useState } from "react"
import { Download } from "lucide-react"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetFooter,
} from "@/components/ui/sheet"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { CopyValue, Status } from "@/components/console-ui"
import { bytes, date } from "@/lib/api"
import { kvBytes, kvText } from "@/lib/kv-values"
import { download } from "@/lib/resource"

export type QueueMessage = {
  id: string
  bodyBase64: string
  contentType: string
  size: number
  enqueuedAt: number
  visibleAt: number
  attempts: number
  deliveryAttempts?: number
  state: string
  leasedUntil: number | null
  deadLetterSource: string | null
  transferTarget: string | null
  lease?: { active: boolean; expiresAt: number | null; generation: number }
  provenance?: {
    sourceQueue: string | null
    transferId: string | null
    kind: string | null
    targetQueue: string | null
  }
}
export function elapsed(ms: number | null | undefined) {
  if (ms == null || !Number.isFinite(ms)) return "Not recorded"
  if (ms < 1000) return `${Math.max(0, ms).toFixed(0)} ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)} s`
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)} min`
  if (ms < 86400000) return `${(ms / 3600000).toFixed(1)} h`
  return `${(ms / 86400000).toFixed(1)} days`
}
export function QueueMessagePanel({
  message,
  observedAt,
  onClose,
  returnFocus,
}: {
  message: QueueMessage | null
  observedAt?: number
  onClose: () => void
  returnFocus?: () => void
}) {
  return (
    <Sheet
      open={message !== null}
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
    >
      <SheetContent
        className="resource-detail-sheet"
        onCloseAutoFocus={(event) => {
          if (returnFocus) {
            event.preventDefault()
            returnFocus()
          }
        }}
      >
        <SheetHeader>
          <SheetTitle>Message details</SheetTitle>
          <SheetDescription>
            Non-consuming snapshot
            {observedAt == null ? "" : ` · ${date(observedAt)}`}. Delivery may
            have changed since this observation.
          </SheetDescription>
        </SheetHeader>
        {message ? (
          <div className="resource-detail-body">
            <div className="flex items-center justify-between gap-3">
              <code className="break-all">{message.id}</code>
              <CopyValue value={message.id} />
            </div>
            <Status label={message.state} />
            <dl className="detail-grid">
              <dt>Age in this queue</dt>
              <dd>
                {elapsed(
                  observedAt == null ? null : observedAt - message.enqueuedAt
                )}
              </dd>
              <dt>Enqueued</dt>
              <dd>{date(message.enqueuedAt)}</dd>
              <dt>Delivery attempts</dt>
              <dd>
                {message.deliveryAttempts ?? message.attempts}{" "}
                {message.deliveryAttempts == null
                  ? "(recorded retries; total not reported)"
                  : ""}
              </dd>
              <dt>Visibility</dt>
              <dd>
                {date(message.visibleAt)} ·{" "}
                {observedAt == null
                  ? message.state
                  : message.visibleAt <= observedAt
                    ? "Due at observation"
                    : "Delayed at observation"}
              </dd>
              <dt>Lease expiry</dt>
              <dd>
                {message.lease?.expiresAt != null
                  ? date(message.lease.expiresAt)
                  : message.leasedUntil != null
                    ? date(message.leasedUntil)
                    : "No recorded lease"}
              </dd>
              <dt>Lease state</dt>
              <dd>
                {message.lease
                  ? message.lease.active
                    ? "Active at observation"
                    : message.lease.expiresAt == null
                      ? "Not leased"
                      : "Expired at observation"
                  : "Not reported"}
              </dd>
              <dt>Lease generation</dt>
              <dd>{message.lease?.generation ?? "Not reported"}</dd>
              <dt>Content</dt>
              <dd>
                {message.contentType} · {bytes(message.size)}
              </dd>
            </dl>
            <section aria-label="Dead-letter provenance">
              <h3>Dead-letter provenance</h3>
              <dl className="detail-grid">
                <dt>Source queue</dt>
                <dd>
                  {message.provenance?.sourceQueue ??
                    message.deadLetterSource ??
                    "No dead-letter source recorded"}
                </dd>
                <dt>Transfer identity</dt>
                <dd className="break-all">
                  {message.provenance?.transferId ?? "Not recorded"}
                </dd>
                <dt>Transfer kind</dt>
                <dd>
                  {message.provenance?.kind ??
                    (message.deadLetterSource
                      ? "Received from dead-letter transfer"
                      : "No transfer in progress")}
                </dd>
                <dt>Transfer target</dt>
                <dd>
                  {message.provenance?.targetQueue ??
                    message.transferTarget ??
                    "None"}
                </dd>
              </dl>
              <p className="workspace-caption">
                Age and attempts describe this queue. Earlier source-queue
                attempt history is not retained.
              </p>
            </section>
            <Payload key={message.id} message={message} />
          </div>
        ) : null}
        <SheetFooter>
          <Button variant="outline" onClick={onClose}>
            Close message details
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
function Payload({ message }: { message: QueueMessage }) {
  const [view, setView] = useState("preview")
  let buffer: Uint8Array | null = null,
    text: string | null = null,
    json: string | null = null
  try {
    buffer = kvBytes(message.bodyBase64, "base64")
    text = kvText(buffer)
    if (message.contentType === "json")
      json = JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    /* Invalid text remains inspectable as bytes. */
  }
  return (
    <section
      aria-label="Message payload"
      className="flex min-w-0 flex-col gap-3"
    >
      <div className="flex items-center justify-between gap-3">
        <h3>Payload</h3>
        <Button
          variant="outline"
          size="sm"
          disabled={!buffer}
          onClick={() => {
            if (buffer)
              download(
                "cellar-queue-message.bin",
                buffer,
                "application/octet-stream"
              )
          }}
        >
          <Download data-icon="inline-start" />
          Download bytes
        </Button>
      </div>
      <Tabs value={view} onValueChange={setView}>
        <TabsList aria-label="Payload views">
          <TabsTrigger value="preview">
            {json ? "JSON" : text === null ? "Binary" : "Text"}
          </TabsTrigger>
          <TabsTrigger value="base64">Base64</TabsTrigger>
        </TabsList>
        <TabsContent value="preview">
          <pre className="inspection-payload">
            {json ?? text ?? message.bodyBase64}
          </pre>
          {text === null ? (
            <p className="workspace-caption">
              {message.contentType === "structured"
                ? "Structured clone payload shown as base64; no browser decoder is installed."
                : "Binary payload shown as base64."}
            </p>
          ) : null}
        </TabsContent>
        <TabsContent value="base64">
          <pre className="inspection-payload">{message.bodyBase64}</pre>
        </TabsContent>
      </Tabs>
    </section>
  )
}
