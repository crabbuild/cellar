// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useMemo, useRef, useState } from "react"
import { useMutation } from "@tanstack/react-query"
import {
  Braces,
  Download,
  Edit3,
  FileJson,
  Plus,
  RefreshCw,
  Save,
  Search,
  Trash2,
} from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
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
import { CodeEditor } from "@/components/code-editor"
import { DateTimePicker } from "@/components/date-time-picker"
import { CopyValue, Feedback } from "@/components/console-ui"
import { ApiError, bytes, date } from "@/lib/api"
import {
  download,
  resourceRequest,
  useResourceAction,
  type ResourceTarget,
} from "@/lib/resource"
import {
  base64,
  expiryDescription,
  kvBytes,
  kvDirty,
  kvEncoding,
  kvMetadata,
  kvSnapshot,
  kvText,
  kvValue,
  makeKvDraft,
  metadataEditor,
  validateKvDraft,
  type KvDraft,
  type KvEncoding,
  type KvSnapshot,
} from "@/lib/kv-values"
import { useSession } from "@/lib/session"

type KeyPage = {
  keys: { name: string; metadata: string | null; expiration: number | null }[]
  complete: boolean
}
type Review =
  { kind: "save"; draft: KvDraft } | { kind: "delete"; snapshot: KvSnapshot }

export default function KvWorkspace({
  target: currentTarget,
  canRead,
  canWrite,
}: {
  target: ResourceTarget
  canRead: boolean
  canWrite: boolean
}) {
  // Keep drafts tied to the namespace that was selected when this workspace opened.
  const [target] = useState(currentTarget)
  const targetChanged = currentTarget.identity !== target.identity
  const { token } = useSession()
  const [prefix, setPrefix] = useState("")
  const [listedPrefix, setListedPrefix] = useState("")
  const [keys, setKeys] = useState<KeyPage | null>(null)
  const [key, setKey] = useState("")
  const [snapshot, setSnapshot] = useState<KvSnapshot | null>(null)
  const [view, setView] = useState<KvEncoding>("text")
  const [draft, setDraft] = useState<KvDraft | null>(null)
  const [review, setReview] = useState<Review | null>(null)
  const [discard, setDiscard] = useState<(() => void) | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [conflict, setConflict] = useState(false)
  const [latest, setLatest] = useState<KvSnapshot | null>(null)
  const readSequence = useRef(0)
  const mutationBusy = useRef(false)
  const action = useResourceAction(target)
  const list = useMutation({
    mutationFn: async ({
      prefix,
      after,
    }: {
      prefix: string
      after: string
    }) => {
      const reply = await resourceRequest(token, target, {
        action: "kv_list",
        prefix,
        after,
      })
      const result = reply.result as KeyPage
      if (!Array.isArray(result?.keys) || typeof result.complete !== "boolean")
        throw new Error("The namespace returned an unreadable key list.")
      return { prefix, result }
    },
    onSuccess: ({ prefix, result }) => {
      setListedPrefix(prefix)
      setKeys(result)
    },
    retry: false,
  })
  const read = useMutation({
    mutationFn: async (key: string) =>
      kvSnapshot(
        key,
        (await resourceRequest(token, target, { action: "kv_get", key })).result
      ),
    retry: false,
  })
  const dirty = draft ? kvDirty(draft) : false
  const validated = useMemo(
    () => (draft ? validateKvDraft(draft) : null),
    [draft]
  )
  const changeContext = (next: () => void) => {
    if (mutationBusy.current) return
    if (dirty) setDiscard(() => next)
    else {
      setDraft(null)
      next()
    }
  }
  const load = async (key: string, compare = false) => {
    if (!key || !canRead) return
    const sequence = ++readSequence.current
    setError(null)
    try {
      const loaded = await read.mutateAsync(key)
      if (sequence !== readSequence.current) return
      if (compare) {
        setLatest(loaded)
        return
      }
      setKey(key)
      setSnapshot(loaded)
      setView(kvEncoding(loaded))
      setDraft(null)
      setLatest(null)
      setConflict(false)
    } catch (error) {
      if (sequence === readSequence.current)
        setError(
          error instanceof Error ? error.message : "Could not read this key."
        )
    }
  }
  const create = () => {
    ++readSequence.current
    const empty: KvSnapshot = {
      key: "",
      found: false,
      revision: null,
      conditional: true,
      tag: "text",
      bytes: new Uint8Array(),
      metadata: null,
      expiration: null,
    }
    setSnapshot(null)
    setKey("")
    setDraft(makeKvDraft(empty))
    setError(null)
    setConflict(false)
    setLatest(null)
  }
  const selectEncoding = (encoding: KvEncoding) => {
    if (!draft) {
      if (snapshot) {
        try {
          kvValue(snapshot, encoding)
          setView(encoding)
        } catch {
          setError(
            "This binary value is not valid UTF-8. Use the binary view to preserve its bytes."
          )
        }
      }
      return
    }
    try {
      const buffer = kvBytes(
        draft.value,
        draft.encoding === "json" ? "text" : draft.encoding
      )
      const value = encoding === "base64" ? base64(buffer) : kvText(buffer)
      setDraft({ ...draft, encoding, value })
      setError(null)
    } catch {
      setError(
        "This value cannot be converted without changing its bytes. Correct the base64 or use the binary view."
      )
    }
  }
  const save = async (review: Review) => {
    if (mutationBusy.current || !canRead || !canWrite) return
    const checked =
      review.kind === "save" ? validateKvDraft(review.draft) : null
    if (checked?.error) {
      setError(checked.error)
      return
    }
    const key = review.kind === "save" ? review.draft.key : review.snapshot.key
    const command =
      review.kind === "save" && checked?.value
        ? {
            action: "kv_edit",
            key,
            edit: {
              value_base64: base64(checked.value),
              tag: review.draft.original.found
                ? review.draft.original.tag
                : review.draft.encoding === "base64"
                  ? "bytes"
                  : review.draft.encoding,
              metadata: checked.metadata,
              expiration: checked.expiration,
              condition: { revision: review.draft.original.revision },
            },
          }
        : {
            action: "kv_delete_if",
            key,
            condition: {
              revision:
                review.kind === "delete" ? review.snapshot.revision : null,
            },
          }
    mutationBusy.current = true
    setError(null)
    try {
      const reply = await action.mutateAsync(command)
      if (reply.replayed) {
        setError(
          "This operation was already recorded. Inspect Operations to reconcile its outcome."
        )
        return
      }
      setDraft(null)
      setConflict(false)
      setLatest(null)
      setSnapshot(null)
      toast.success(review.kind === "save" ? "Key saved" : "Key deleted")
      await load(key)
      if (keys) list.mutate({ prefix: listedPrefix, after: "" })
    } catch (error) {
      setError(
        error instanceof Error
          ? error.message
          : "The operation did not return success."
      )
      setConflict(error instanceof ApiError && error.status === 409)
    } finally {
      mutationBusy.current = false
    }
  }
  const rebase = () => {
    if (
      !draft ||
      !latest ||
      latest.valueOmitted ||
      !latest.conditional ||
      !["text", "json", "bytes"].includes(latest.tag)
    )
      return
    const valueChanged = (() => {
      try {
        return (
          base64(kvBytes(draft.value, draft.encoding)) !==
          base64(draft.original.bytes)
        )
      } catch {
        return true
      }
    })()
    const metadataChanged = (() => {
      try {
        return (
          kvMetadata(draft.metadata) !==
          kvMetadata(draft.original.metadata ?? "")
        )
      } catch {
        return true
      }
    })()
    const baseline = makeKvDraft(latest)
    setSnapshot(latest)
    setDraft({
      ...draft,
      original: latest,
      value: valueChanged ? draft.value : baseline.value,
      encoding: valueChanged ? draft.encoding : baseline.encoding,
      metadata: metadataChanged ? draft.metadata : baseline.metadata,
    })
    setLatest(null)
    setConflict(false)
    setError(null)
  }
  const valueEncoding = draft?.encoding ?? view
  const visibleSnapshot = latest ?? snapshot
  let value = draft?.value ?? ""
  if (!draft && snapshot?.found) {
    try {
      value = kvValue(snapshot, view)
    } catch {
      value = base64(snapshot.bytes)
    }
  }
  const editingAllowed =
    canRead &&
    canWrite &&
    !!snapshot?.conditional &&
    !snapshot.valueOmitted &&
    ["text", "json", "bytes"].includes(snapshot.tag) &&
    snapshot.bytes.length <= 1024 * 1024
  return (
    <section
      className="data-workbench kv-workbench"
      aria-label="KV explorer and editor"
    >
      <aside className="workbench-sidebar" aria-label="Key explorer">
        <div className="workbench-sidebar-heading">
          <h2>
            <Braces />
            Key explorer
          </h2>
          <Button
            size="icon-sm"
            variant="ghost"
            aria-label="New key"
            disabled={!canRead || !canWrite || action.isPending}
            onClick={() => changeContext(create)}
          >
            <Plus />
          </Button>
        </div>
        <FieldGroup>
          <Field>
            <FieldLabel htmlFor="key-prefix">Key prefix</FieldLabel>
            <Input
              id="key-prefix"
              value={prefix}
              onChange={(event) => setPrefix(event.target.value)}
              placeholder="sessions/"
            />
          </Field>
          <Button
            variant="outline"
            disabled={!canRead || list.isPending}
            onClick={() => list.mutate({ prefix, after: "" })}
          >
            <Search data-icon="inline-start" />
            List keys
          </Button>
        </FieldGroup>
        <Feedback loading={list.isPending && !keys} error={list.error}>
          {keys ? (
            <>
              <p className="workspace-caption">
                {keys.keys.length} keys on this page · prefix{" "}
                {listedPrefix || "(all keys)"}
              </p>
              <ul className="kv-key-list">
                {keys.keys.map((item) => (
                  <li key={item.name}>
                    <button
                      type="button"
                      aria-label={item.name}
                      className={
                        snapshot?.key === item.name
                          ? "kv-key-selected"
                          : undefined
                      }
                      aria-current={
                        snapshot?.key === item.name ? "true" : undefined
                      }
                      disabled={action.isPending}
                      onClick={() => changeContext(() => void load(item.name))}
                    >
                      <code>{item.name}</code>
                      <small>
                        {item.expiration == null
                          ? "No expiration"
                          : date(item.expiration)}
                      </small>
                    </button>
                  </li>
                ))}
              </ul>
              {!keys.keys.length ? (
                <p className="workbench-empty">No keys match this prefix.</p>
              ) : null}
              {!keys.complete && keys.keys.length ? (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={list.isPending}
                  onClick={() =>
                    list.mutate({
                      prefix: listedPrefix,
                      after: keys.keys.at(-1)!.name,
                    })
                  }
                >
                  Next 100 keys
                </Button>
              ) : (
                <p className="workspace-caption">End of listing</p>
              )}
            </>
          ) : (
            <p className="workspace-caption">
              List a prefix to browse keys, or inspect an exact key.
            </p>
          )}
        </Feedback>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            changeContext(() => void load(key))
          }}
        >
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="exact-key">Exact key</FieldLabel>
              <Input
                id="exact-key"
                value={key}
                onChange={(event) => setKey(event.target.value)}
                placeholder="sessions/customer-123"
              />
            </Field>
            <Button
              variant="outline"
              disabled={!key || !canRead || read.isPending || action.isPending}
            >
              Read key
            </Button>
          </FieldGroup>
        </form>
      </aside>
      <div className="workbench-main kv-main">
        <header className="kv-detail-heading">
          <div>
            <h2>
              {draft
                ? draft.original.found
                  ? "Edit key"
                  : "Create key"
                : snapshot?.key || "Select a key"}
            </h2>
            <p>
              {draft
                ? "Review your draft before applying changes to this namespace."
                : "Inspect the value, metadata and expiration together."}
            </p>
          </div>
          {dirty ? (
            <Badge variant="warning">Unsaved draft</Badge>
          ) : snapshot?.found ? (
            <Badge variant="secondary">{snapshot.tag}</Badge>
          ) : null}
        </header>
        {targetChanged ? (
          <Alert>
            <AlertTitle>Namespace binding changed</AlertTitle>
            <AlertDescription>
              Copy your draft before reopening this workspace. It remains tied
              to {target.identity}.
            </AlertDescription>
          </Alert>
        ) : null}
        {error ? (
          <Alert variant="destructive">
            <AlertTitle>
              {conflict
                ? "Key changed while you edited"
                : "Could not complete this action"}
            </AlertTitle>
            <AlertDescription>
              {error}
              {action.isError && !conflict
                ? " Check Operations before retrying a write; its outcome may be unknown."
                : ""}
            </AlertDescription>
          </Alert>
        ) : null}
        {conflict && draft ? (
          <section className="kv-conflict">
            <p>
              Your draft is retained. Compare the current value before reviewing
              another save.
            </p>
            <Button
              variant="outline"
              disabled={read.isPending}
              onClick={() => void load(draft.key, true)}
            >
              <RefreshCw data-icon="inline-start" />
              Compare latest
            </Button>
            {latest ? (
              <>
                <KvComparison original={latest} draft={draft} />
                <Button
                  variant="outline"
                  onClick={rebase}
                  disabled={
                    latest.valueOmitted ||
                    !latest.conditional ||
                    !["text", "json", "bytes"].includes(latest.tag)
                  }
                >
                  Use latest as baseline
                </Button>
                {latest.valueOmitted ? (
                  <p>
                    The latest value exceeds the editor limit. Use the SDK or
                    CLI to reconcile it.
                  </p>
                ) : null}
              </>
            ) : null}
          </section>
        ) : null}
        {read.isPending ? (
          <p className="workspace-caption" role="status">
            Reading key…
          </p>
        ) : null}
        {snapshot?.valueOmitted ? (
          <section className="kv-metadata">
            <h3>Value exceeds the inspection limit</h3>
            <p>
              {bytes(snapshot.size)} · Revision {snapshot.revision} · Expires:{" "}
              {expiryDescription(snapshot.expiration)}
            </p>
            <p>
              The console inspects and edits values up to 1 MiB. Use the
              application SDK or CLI to read this value.
            </p>
            <h3>Metadata</h3>
            <pre>{metadataEditor(snapshot.metadata) || "No metadata"}</pre>
          </section>
        ) : draft || snapshot?.found ? (
          <>
            {draft ? (
              <Field>
                <FieldLabel htmlFor="draft-key">Key</FieldLabel>
                <Input
                  id="draft-key"
                  value={draft.key}
                  readOnly={draft.original.found}
                  onChange={(event) =>
                    setDraft({ ...draft, key: event.target.value })
                  }
                />
                <FieldDescription>
                  {draft.original.found
                    ? `Revision ${draft.original.revision}. Save rejects any intervening change.`
                    : "Creates this key only if it is absent."}
                </FieldDescription>
              </Field>
            ) : (
              <div className="kv-inspection-summary">
                <span>
                  {bytes(snapshot?.bytes.length)} · Revision{" "}
                  {snapshot?.revision ?? "unavailable"}
                </span>
                <span>
                  Expires: {expiryDescription(snapshot?.expiration ?? null)}
                </span>
                <CopyValue value={snapshot!.key} />
              </div>
            )}
            <div className="kv-value-toolbar">
              <Select
                value={valueEncoding}
                onValueChange={(value) => selectEncoding(value as KvEncoding)}
              >
                <SelectTrigger aria-label="Value view">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="text">Text</SelectItem>
                    <SelectItem value="json">JSON</SelectItem>
                    <SelectItem value="base64">Binary · base64</SelectItem>
                  </SelectGroup>
                </SelectContent>
              </Select>
              <span className="workspace-caption">
                {bytes(
                  draft
                    ? (validated?.value?.length ??
                        new TextEncoder().encode(draft.value).length)
                    : snapshot?.bytes.length
                )}
              </span>
              {draft && valueEncoding === "json" ? (
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => {
                    try {
                      setDraft({
                        ...draft,
                        value: JSON.stringify(JSON.parse(draft.value), null, 2),
                      })
                    } catch {
                      setError("Enter valid JSON before formatting.")
                    }
                  }}
                >
                  <FileJson data-icon="inline-start" />
                  Format JSON
                </Button>
              ) : null}
              {!draft && snapshot ? (
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() =>
                    download(
                      "cellar-value.bin",
                      snapshot.bytes,
                      "application/octet-stream"
                    )
                  }
                >
                  <Download data-icon="inline-start" />
                  Download value
                </Button>
              ) : null}
            </div>
            <div className="kv-code-pane">
              <CodeEditor
                key={`${draft ? "draft" : "preview"}:${snapshot?.key ?? "new"}`}
                label={draft ? "Value" : "Value preview"}
                value={value}
                language={valueEncoding === "json" ? "json" : "text"}
                readOnly={!draft}
                onChange={(value) => {
                  if (draft) setDraft({ ...draft, value })
                }}
              />
            </div>
            {draft ? (
              <FieldGroup className="kv-editor-fields">
                <Field>
                  <FieldLabel htmlFor="kv-metadata">Metadata (JSON)</FieldLabel>
                  <Textarea
                    id="kv-metadata"
                    value={draft.metadata}
                    onChange={(event) =>
                      setDraft({ ...draft, metadata: event.target.value })
                    }
                    placeholder="Leave blank for no metadata"
                    className="font-mono"
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="kv-expiry">Expiration</FieldLabel>
                  <Select
                    value={draft.expiry}
                    onValueChange={(expiry) =>
                      setDraft({
                        ...draft,
                        expiry: expiry as KvDraft["expiry"],
                      })
                    }
                  >
                    <SelectTrigger id="kv-expiry">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectGroup>
                        <SelectItem
                          value="preserve"
                          disabled={!draft.original.found}
                        >
                          Preserve current expiry
                        </SelectItem>
                        <SelectItem value="never">Never expire</SelectItem>
                        <SelectItem value="at">Expire at</SelectItem>
                      </SelectGroup>
                    </SelectContent>
                  </Select>
                  <FieldDescription>
                    {draft.expiry === "preserve"
                      ? draft.original.expiration === null
                        ? "This key has no expiration."
                        : `Exact expiry retained: ${expiryDescription(draft.original.expiration)}`
                      : draft.expiry === "never"
                        ? "The saved key will have no expiration."
                        : "Choose when this key should expire."}
                  </FieldDescription>
                </Field>
                {draft.expiry === "at" ? (
                  <DateTimePicker
                    id="kv-expire-at"
                    label="Expiration"
                    value={draft.at}
                    onChange={(at) => setDraft({ ...draft, at })}
                  />
                ) : null}
              </FieldGroup>
            ) : (
              <section className="kv-metadata">
                <h3>Metadata</h3>
                <pre>
                  {metadataEditor(snapshot?.metadata ?? null) || "No metadata"}
                </pre>
              </section>
            )}
            {draft && validated?.error ? (
              <p className="kv-validation" role="alert">
                {validated.error}
              </p>
            ) : null}
            <div className="kv-action-bar">
              {draft ? (
                <>
                  <Button
                    disabled={
                      !dirty ||
                      !!validated?.error ||
                      action.isPending ||
                      conflict ||
                      !canWrite ||
                      !canRead ||
                      targetChanged
                    }
                    onClick={() =>
                      setReview({ kind: "save", draft: { ...draft } })
                    }
                  >
                    <Save data-icon="inline-start" />
                    Review changes
                  </Button>
                  <Button
                    variant="outline"
                    disabled={action.isPending}
                    onClick={() => changeContext(() => setDraft(null))}
                  >
                    Discard draft
                  </Button>
                </>
              ) : (
                <>
                  <Button
                    disabled={
                      !editingAllowed || read.isPending || action.isPending
                    }
                    onClick={() => {
                      if (snapshot) setDraft(makeKvDraft(snapshot))
                    }}
                  >
                    <Edit3 data-icon="inline-start" />
                    Edit value
                  </Button>
                  <Button
                    variant="outline"
                    disabled={read.isPending || action.isPending}
                    onClick={() => {
                      if (snapshot) void load(snapshot.key)
                    }}
                  >
                    <RefreshCw data-icon="inline-start" />
                    Refresh key
                  </Button>
                  <Button
                    variant="destructive"
                    disabled={
                      !editingAllowed ||
                      !snapshot?.found ||
                      read.isPending ||
                      action.isPending
                    }
                    onClick={() => {
                      if (snapshot) setReview({ kind: "delete", snapshot })
                    }}
                  >
                    <Trash2 data-icon="inline-start" />
                    Delete key
                  </Button>
                </>
              )}
            </div>
            {!draft && snapshot && !snapshot.conditional ? (
              <p className="workspace-caption">
                Editing is unavailable because this resource did not return a
                revision.
              </p>
            ) : null}
          </>
        ) : snapshot ? (
          <div className="workbench-empty">
            <p>This key was not found. It may have expired or been deleted.</p>
            <Button
              className="mt-4"
              disabled={!canRead || !canWrite}
              onClick={() => setDraft(makeKvDraft(snapshot))}
            >
              <Plus data-icon="inline-start" />
              Create this key
            </Button>
          </div>
        ) : (
          <div className="workbench-empty">
            <Braces />
            <p>Select a key to inspect it, or create a new key.</p>
          </div>
        )}
        <footer className="workbench-status">
          <span>
            {target.application} / {target.binding}
          </span>
          <span>Values ≤ 1 MiB · metadata ≤ 1 KiB</span>
          <span>
            {visibleSnapshot?.revision
              ? `Observed revision ${visibleSnapshot.revision}`
              : "Owner-checked writes"}
          </span>
        </footer>
      </div>
      <AlertDialog
        open={review !== null}
        onOpenChange={(open) => {
          if (!open) setReview(null)
        }}
      >
        <AlertDialogContent className="sm:max-w-3xl">
          <AlertDialogHeader>
            <AlertDialogTitle>
              {review?.kind === "delete" ? "Delete key" : "Review key changes"}
            </AlertDialogTitle>
            <AlertDialogDescription>
              Fleet: {target.fleet} · {target.application} / {target.binding} ·{" "}
              {review?.kind === "save"
                ? review.draft.key
                : review?.snapshot.key}
              . The owner will reject an intervening change.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {review?.kind === "save" ? (
            <KvComparison
              original={review.draft.original}
              draft={review.draft}
            />
          ) : (
            <p>This removes the inspected key from the namespace.</p>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (review) void save(review)
                setReview(null)
              }}
            >
              {review?.kind === "delete"
                ? "Delete inspected key"
                : "Save changes"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
      <AlertDialog
        open={discard !== null}
        onOpenChange={(open) => {
          if (!open) setDiscard(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Discard unsaved draft?</AlertDialogTitle>
            <AlertDialogDescription>
              Your changes have not been saved. Discard them before leaving this
              key.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Keep editing</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const next = discard
                setDraft(null)
                setDiscard(null)
                next?.()
              }}
            >
              Discard draft
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  )
}

function KvComparison({
  original,
  draft,
}: {
  original: KvSnapshot
  draft: KvDraft
}) {
  const checked = validateKvDraft(draft)
  const before = (() => {
    try {
      return kvValue(original, draft.encoding)
    } catch {
      return base64(original.bytes)
    }
  })()
  const expiration =
    draft.expiry === "preserve"
      ? original.expiration
      : draft.expiry === "never"
        ? null
        : new Date(draft.at).getTime()
  return (
    <div className="kv-comparison">
      <section>
        <h3>
          Before · {original.found ? `revision ${original.revision}` : "absent"}
        </h3>
        <pre>
          {original.valueOmitted
            ? `Value omitted · ${bytes(original.size)}`
            : original.found
              ? before
              : "Key does not exist"}
        </pre>
        <h4>Metadata</h4>
        <pre>{metadataEditor(original.metadata) || "No metadata"}</pre>
        <p>Expiry: {expiryDescription(original.expiration)}</p>
      </section>
      <section>
        <h3>After · {bytes(checked.value?.length)}</h3>
        <pre>{draft.value}</pre>
        <h4>Metadata</h4>
        <pre>{draft.metadata || "No metadata"}</pre>
        <p>
          Expiry:{" "}
          {expiration === null
            ? "No expiration"
            : Number.isFinite(expiration)
              ? expiryDescription(expiration)
              : "Invalid date"}
        </p>
      </section>
    </div>
  )
}
