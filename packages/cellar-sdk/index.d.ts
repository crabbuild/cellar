// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/**
 * Values accepted by the Cellar bridge.
 *
 * Numbers must be finite; integers must satisfy Number.isSafeInteger.
 * Use strings for larger integers. The bridge also enforces nesting and size
 * limits at runtime; TypeScript's number type cannot express those constraints.
 */
export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };

/** Ordered header pairs, including repeated header names. */
export type Headers = [string, string][];

/** Outgoing request wire format. Use request() to encode a text or byte body. */
export interface RequestData {
  url: string;
  method: string;
  headers: Headers;
  /** Base64-encoded bytes, not plain text. */
  body: string;
}

/** Handler return value. Use response() or jsonResponse() to construct it. */
export interface ResponseData {
  status: number;
  headers: Headers;
  /** Base64-encoded bytes, not plain text. */
  body: string;
}

/** Buffered request delivered to a handler; not the browser Request class. */
export interface Request extends Omit<RequestData, "body"> {
  body: Uint8Array;
  /** Decode UTF-8 synchronously; throws for invalid UTF-8. */
  text(): string;
  /** Decode and parse JSON synchronously; invalid input throws. */
  json(): Json;
}

/** Decoded response returned by a nested cell fetch. */
export interface Response extends Omit<ResponseData, "body"> {
  body: Uint8Array;
  /** Decode UTF-8 synchronously; throws for invalid UTF-8. */
  text(): string;
  /** Decode and parse JSON synchronously; invalid input throws. */
  json(): Json;
}

/** A missing key has present=false; a stored JSON null has present=true. */
export interface Snapshot {
  present: boolean;
  value: Json;
}

/**
 * One atomic cell-storage transaction. Checks run before any changes are made.
 * A failed check rejects with Conflict / not_committed. Await storage.sync()
 * after applying changes when the response must carry a durability proof.
 */
export interface Batch {
  checks?: Record<string, Snapshot>;
  puts?: Record<string, Json>;
  deletes?: string[];
  /** Absolute Unix time in milliseconds. Mutually exclusive with delete_alarm. */
  alarm_at_ms?: number;
  delete_alarm?: boolean;
}

/** Cell-local SQL output; each row follows the order in columns. */
export interface SqlResult {
  columns: string[];
  rows: Json[][];
  written: number;
}

/** Identity of the current cell activation. Treat epoch as an opaque string. */
export interface Identity {
  name: string | null;
  class: string;
  scope: string;
  epoch: string;
}

/**
 * Invocation-scoped authority supplied by the Rust host.
 *
 * Never retain a context for a later invocation. Await every operation before
 * returning, and issue mutations sequentially rather than with Promise.all().
 * Binding names refer to resources configured in the native deployment.
 */
export interface Context {
  /** Null for application-level handlers outside a cell. */
  readonly identity: Readonly<Identity> | null;

  /** Read the host clock as Unix milliseconds. */
  now(): Promise<number>;

  /** Read a configured application variable, or null when absent. */
  variable(name: string): Promise<string | null>;

  /** Cell-owned SQLite storage; available in cell invocations. */
  readonly storage: {
    read(key: string): Promise<Snapshot>;
    apply(batch: Batch): Promise<null>;
    /** Await the host's configured durability proof for cell writes. */
    sync(): Promise<null>;
    /** Execute cell-local SQL with positional parameters. */
    sql(sql: string, params?: Json[]): Promise<SqlResult>;
    /** Read the cell's alarm time in Unix milliseconds, or null. */
    getAlarm(): Promise<number | null>;
  };

  /** Address a named cell through a configured namespace binding. */
  cell(binding: string, name: string): {
    fetch(request: RequestData): Promise<Response>;
  };

  /** JSON operations on a configured shared KV binding. */
  kv(binding: string): {
    get(key: string): Promise<Json>;
    put(
      key: string,
      value: Json,
      options?: { ttl_seconds?: number; metadata?: Json },
    ): Promise<null>;
    delete(key: string): Promise<null>;
    list(options?: {
      prefix?: string;
      limit?: number;
      cursor?: string;
    }): Promise<Json>;
  };

  /** Query a shared SQL binding, distinct from cell-local storage.sql(). */
  sql(binding: string): {
    query(sql: string, params?: Json[]): Promise<Json>;
  };

  /** Enqueue JSON through a configured queue binding. */
  queue(binding: string): {
    send(body: Json, options?: { delay_seconds?: number }): Promise<null>;
  };

  /**
   * Acknowledge or retry a queue message. Omitting id settles the whole batch.
   * Only valid during a queue invocation; delay_seconds applies to retries.
   */
  settle(options: {
    retry: boolean;
    id?: string;
    delay_seconds?: number;
  }): Promise<null>;

  /** Disable retries for the current scheduled invocation. */
  noRetry(): Promise<null>;
}

export interface Alarm {
  scheduled_ms: number;
}

export interface Scheduled extends Alarm {
  cron: string;
}

export interface QueueBatch {
  queue: string;
  messages: {
    id: string;
    attempts: number;
    timestamp_ms: number;
    body: Json;
  }[];
}

/**
 * A cell object lives for one activation. Fields are caches, not durable state.
 * The host must explicitly register each implemented handler.
 */
export interface Cell {
  fetch?(
    request: Request,
    context: Context,
  ): ResponseData | Promise<ResponseData>;
  alarm?(alarm: Alarm, context: Context): void | Promise<void>;
}

/** Application exports loaded by the selected JavaScript backend. */
export interface Application {
  /** Constructors receive no Context; each handler receives its own. */
  cells?: Record<string, new () => Cell>;
  fetch?(
    request: Request,
    context: Context,
  ): ResponseData | Promise<ResponseData>;
  queue?(batch: QueueBatch, context: Context): void | Promise<void>;
  scheduled?(event: Scheduled, context: Context): void | Promise<void>;
}

/**
 * A structured SDK-operation failure. This is a type, not a runtime class.
 * An unknown outcome may follow a committed mutation; do not blindly retry it.
 */
export interface CellarError extends Error {
  code: string;
  outcome: "not_committed" | "unknown";
}

/** Encode raw bytes using standard padded base64. */
export function bytesToBase64(bytes: Uint8Array): string;

/** Encode UTF-8, replacing unpaired UTF-16 surrogates with U+FFFD. */
export function utf8(text: string): Uint8Array;

/** Build an outgoing buffered response; defaults to status 200 and no headers. */
export function response(
  body?: string | Uint8Array,
  options?: { status?: number; headers?: Headers },
): ResponseData;

/** Build a response with JSON serialization and an application/json header. */
export function jsonResponse(
  value: Json,
  options?: { status?: number; headers?: Headers },
): ResponseData;

/** Build an outgoing cell request; defaults to GET with an empty body. */
export function request(
  url: string,
  options?: { method?: string; headers?: Headers; body?: string | Uint8Array },
): RequestData;
