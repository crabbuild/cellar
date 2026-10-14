export type KvSnapshot = {
  key: string
  found: boolean
  revision: string | null
  conditional: boolean
  tag: string
  bytes: Uint8Array
  metadata: string | null
  expiration: number | null
  valueOmitted?: boolean
  size?: number
}
export type KvEncoding = "text" | "json" | "base64"
export type KvDraft = {
  key: string
  value: string
  encoding: KvEncoding
  metadata: string
  expiry: "preserve" | "never" | "at"
  at: string
  original: KvSnapshot
}

export function base64(bytes: Uint8Array) {
  let value = ""
  for (const byte of bytes) value += String.fromCharCode(byte)
  return btoa(value)
}
export function kvBytes(value: string, encoding: KvEncoding) {
  if (encoding === "base64")
    return Uint8Array.from(atob(value.replaceAll(/\s/g, "")), (character) =>
      character.charCodeAt(0)
    )
  if (encoding === "json") JSON.parse(value)
  return new TextEncoder().encode(value)
}
export function kvText(bytes: Uint8Array) {
  return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
    bytes
  )
}
export function kvEncoding(snapshot: KvSnapshot): KvEncoding {
  try {
    const value = kvText(snapshot.bytes)
    if (snapshot.tag === "json") {
      try {
        JSON.parse(value)
        return "json"
      } catch {
        return "text"
      }
    }
    return "text"
  } catch {
    return "base64"
  }
}
export function kvValue(snapshot: KvSnapshot, encoding: KvEncoding) {
  return encoding === "base64" ? base64(snapshot.bytes) : kvText(snapshot.bytes)
}
export function kvSnapshot(key: string, value: unknown): KvSnapshot {
  const result =
    value && typeof value === "object"
      ? (value as Record<string, unknown>)
      : null
  if (!result || typeof result.found !== "boolean")
    throw new Error("The namespace returned an unreadable key.")
  const snapshot: KvSnapshot = {
    key,
    found: result.found,
    revision: typeof result.revision === "string" ? result.revision : null,
    conditional: !result.found || typeof result.revision === "string",
    tag: typeof result.tag === "string" ? result.tag : "bytes",
    bytes: new Uint8Array(),
    metadata:
      result.metadata == null
        ? null
        : typeof result.metadata === "string"
          ? result.metadata
          : JSON.stringify(result.metadata),
    expiration:
      typeof result.expiration === "number" ? result.expiration : null,
    valueOmitted: result.valueOmitted === true,
    size: typeof result.size === "number" ? result.size : undefined,
  }
  if (result.found && !snapshot.valueOmitted) {
    if (result.valueEncoding === "base64" && typeof result.value === "string")
      snapshot.bytes = kvBytes(result.value, "base64")
    else if (
      Array.isArray(result.value) &&
      result.value.every(
        (byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255
      )
    )
      snapshot.bytes = Uint8Array.from(result.value)
    else
      throw new Error("The value cannot be decoded without changing its bytes.")
  }
  return snapshot
}
export function kvMetadata(value: string) {
  return value.trim() ? JSON.stringify(JSON.parse(value)) : null
}
export function metadataEditor(value: string | null) {
  if (value === null) return ""
  try {
    return JSON.stringify(JSON.parse(value), null, 2)
  } catch {
    return value
  }
}
export function localDate(timestamp: number | null) {
  if (timestamp === null) return ""
  const date = new Date(timestamp)
  if (!Number.isFinite(date.getTime())) return ""
  return new Date(timestamp - date.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 19)
}
export function expiryDescription(timestamp: number | null) {
  if (timestamp === null) return "No expiration"
  const date = new Date(timestamp)
  return Number.isFinite(date.getTime())
    ? date.toISOString()
    : `${timestamp} milliseconds since Unix epoch`
}
export function makeKvDraft(snapshot: KvSnapshot): KvDraft {
  const encoding = kvEncoding(snapshot)
  return {
    key: snapshot.key,
    value: kvValue(snapshot, encoding),
    encoding,
    metadata: metadataEditor(snapshot.metadata),
    expiry: snapshot.found ? "preserve" : "never",
    at: localDate(snapshot.expiration),
    original: snapshot,
  }
}
export function validateKvDraft(draft: KvDraft) {
  try {
    if (!draft.key || new TextEncoder().encode(draft.key).length > 512)
      throw new Error("Key must contain 1–512 bytes.")
    const value = kvBytes(draft.value, draft.encoding)
    if (value.length > 1024 * 1024)
      throw new Error("Values are limited to 1 MiB in this editor.")
    const metadata = kvMetadata(draft.metadata)
    if (metadata !== null && new TextEncoder().encode(metadata).length > 1024)
      throw new Error("Metadata must be at most 1 KiB of JSON.")
    const timestamp =
      draft.expiry === "at" ? new Date(draft.at).getTime() : null
    if (
      timestamp !== null &&
      (!Number.isSafeInteger(timestamp) || timestamp <= Date.now())
    )
      throw new Error("Choose an expiration date in the future.")
    return {
      value,
      metadata,
      expiration:
        draft.expiry === "at"
          ? { mode: "at", timestamp_ms: timestamp }
          : { mode: draft.expiry },
      error: null,
    }
  } catch (error) {
    return {
      value: null,
      metadata: null,
      expiration: null,
      error:
        error instanceof Error ? error.message : "Enter valid JSON or base64.",
    }
  }
}
export function kvDirty(draft: KvDraft) {
  try {
    return (
      !draft.original.found ||
      base64(kvBytes(draft.value, draft.encoding)) !==
        base64(draft.original.bytes) ||
      kvMetadata(draft.metadata) !==
        kvMetadata(draft.original.metadata ?? "") ||
      draft.expiry !== "preserve"
    )
  } catch {
    return true
  }
}
