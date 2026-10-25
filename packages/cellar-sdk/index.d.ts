// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/**
 * Values accepted by the Cellar bridge.
 *
 * Numbers must be finite; integers must satisfy Number.isSafeInteger.
 * Use strings for larger integers. The bridge also enforces nesting and size
 * limits at runtime; TypeScript's number type cannot express those constraints.
 */
export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };

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

/** Provider configuration metadata; querying it does not start a sandbox. */
export interface ContainerCapabilities {
  readonly format: 1;
  readonly provider: string;
  readonly environment: "unspecified" | "linux" | "worker_isolate";
  readonly operations: readonly string[];
  readonly max_body_bytes: number;
  readonly max_resources: number | null;
}

export interface ContainerStartOptions {
  entrypoint?: string[];
  env?: [string, string][];
  enable_internet?: boolean;
  labels?: [string, string][];
}

export interface ContainerExecOptions {
  cmd: string[];
  env?: [string, string][];
  cwd?: string;
  user?: string;
  combined?: boolean;
}

export interface ContainerExecOutput {
  exit_code: number;
  stdout: Uint8Array;
  stderr: Uint8Array;
}

/** Invocation-owned process streams. Drain stdout and stderr concurrently. */
export interface ContainerExec {
  readStdout(): Promise<Uint8Array>;
  readStderr(): Promise<Uint8Array>;
  write(bytes: Uint8Array): Promise<null>;
  closeStdin(): Promise<null>;
  wait(): Promise<number>;
  kill(signal?: number): Promise<null>;
  /** Release streams without killing the process. */
  release(): Promise<null>;
}

export type ContainerWebSocketEvent =
  | { type: "open"; protocol: string }
  | { type: "text"; text: string }
  | { type: "binary"; data: Uint8Array }
  | { type: "close"; code: number; reason: string; clean: boolean };

/** Event-owned outbound WebSocket, closed when the invocation ends. */
export interface ContainerWebSocket {
  receive(): Promise<ContainerWebSocketEvent>;
  sendText(text: string): Promise<null>;
  sendBinary(bytes: Uint8Array): Promise<null>;
  close(code?: number, reason?: string): Promise<null>;
  release(): Promise<null>;
}

export interface ContainerSocket {
  /** An empty chunk marks EOF. */
  read(): Promise<Uint8Array>;
  write(bytes: Uint8Array): Promise<null>;
  closeWrite(): Promise<null>;
  close(): Promise<null>;
  release(): Promise<null>;
}

/** Root-cell provider control. Native execution requires a declared image. */
export interface Container {
  capabilities(): Promise<Readonly<ContainerCapabilities>>;
  running(): Promise<boolean>;
  /** Opaque run identity, represented as a string to avoid integer rounding. */
  start(options?: ContainerStartOptions): Promise<string>;
  monitor(): Promise<number>;
  destroy(): Promise<null>;
  signal(signal: number): Promise<null>;
  /** Integral milliseconds, at most one day. */
  setInactivityTimeout(timeout_ms: number): Promise<null>;
  exec(options: ContainerExecOptions): Promise<ContainerExec>;
  /** Combined stdout/stderr limit cannot exceed the host body limit. */
  execOutput(options: ContainerExecOptions, max_bytes?: number): Promise<ContainerExecOutput>;
  port(port: number): {
    waitReady(timeout_ms?: number): Promise<null>;
    /** Forward buffered HTTP; redirects are returned without following them. */
    fetch(request: RequestData): Promise<Response>;
    connect(): Promise<ContainerSocket>;
    connectWebSocket(url: string, headers?: Headers): Promise<
      | { upgraded: true; protocol: string; socket: ContainerWebSocket }
      | { upgraded: false; response: Response }
    >;
  };
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

  /** Invocation-scoped control of this cell’s declared container. */
  readonly container: Container;
  /** Fetch an outbound response without buffering its complete body. */
  fetchStream(request: RequestData): Promise<LiveResponse>;

  /** Consume an upgrade response capability and obtain its client socket. */
  acceptWebSocket(response: LiveResponse): Promise<WebSocket>;

  /** Create a server endpoint; return its paired upgrade with webSocketResponse(). */
  websocketPair(): Promise<WebSocket>;

  /** Resolve an opaque socket ID within the current invocation. */
  websocket(id: string): WebSocket;

  /** Connect an outbound client endpoint; consume events with next(). */
  connectWebSocket(url: string, protocols?: string[]): Promise<WebSocket>;

  /** List accepted cell sockets, optionally filtered by tag. */
  websockets(tag?: string | null): Promise<WebSocket[]>;

  /** Read the cell's automatic text request/response pair, or null. */
  getWebSocketAutoResponse(): Promise<[string, string] | null>;

  /** Configure an automatic text reply without waking the cell; null disables it. */
  setWebSocketAutoResponse(pair: [string, string] | null): Promise<null>;

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
  cell(
    binding: string,
    name: string,
  ): {
    fetch(request: RequestData): Promise<Response>;

    /** Fetch a live cell response that can be forwarded or consumed in chunks. */
    fetchStream(request: RequestData): Promise<LiveResponse>;
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
  ): FetchOutput | Promise<FetchOutput>;
  alarm?(alarm: Alarm, context: Context): void | Promise<void>;

  /** Native open notification for a hibernating cell socket. */
  websocket_open?(event: WebSocketOpen, context: Context): void | Promise<void>;

  /** Handle a message using this callback's invocation-scoped socket authority. */
  websocket_message?(
    event: WebSocketMessage,
    context: Context,
  ): void | Promise<void>;

  /** Native close notification, including the close code and reason. */
  websocket_close?(
    event: WebSocketClose,
    context: Context,
  ): void | Promise<void>;
}

/** Application exports loaded by the selected JavaScript backend. */
export interface Application {
  /** Constructors receive no Context; each handler receives its own. */
  cells?: Record<string, new () => Cell>;
  fetch?(
    request: Request,
    context: Context,
  ): FetchOutput | Promise<FetchOutput>;
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

/** Text or binary chunks produced synchronously or asynchronously. */
export type Chunks =
  AsyncIterable<string | Uint8Array> | Iterable<string | Uint8Array>;

/** A producer whose chunks are written with native backpressure. */
export interface StreamingResponseData extends ResponseData {
  chunks: Chunks;
}

/** An upgrade returned by webSocketResponse() for an accepted server socket. */
export interface WebSocketResponseData {
  /** Opaque socket capability; keep it as a string to preserve all ID bits. */
  websocket: string;
  headers: Headers;

  /** Worker socket consumer; returning ends its invocation authority. */
  run?: (socket: WebSocket, context: Context) => void | Promise<void>;
}

/**
 * Native response capability scoped to the current invocation.
 * Return it directly to forward the response, or consume it with chunks().
 * A forwarded response or accepted WebSocket upgrade consumes the capability.
 */
export interface LiveResponse {
  /** Opaque native capability; do not convert it to a number. */
  readonly forward: string;
  readonly status: number;
  readonly headers: Headers;

  /** Buffered prefix, also emitted first by chunks() when nonempty. */
  readonly body: Uint8Array;
  readonly stream: boolean;
  readonly websocket: boolean;

  /** Consume once; stopping iteration releases the native response consumer. */
  chunks(): AsyncIterable<Uint8Array>;

  /** Release the response consumer, including a pending read. */
  cancel(): Promise<null>;
}

/** Supported buffered, streaming, upgrade, and forwarded fetch results. */
export type FetchOutput =
  ResponseData | StreamingResponseData | WebSocketResponseData | LiveResponse;

/** Native cell callback identifying a newly opened socket. */
export interface WebSocketOpen {
  id: string;
  protocol: string;
}

/** Native cell message callback; data decodes the text or base64 wire field. */
export interface WebSocketMessage {
  id: string;
  text?: string;

  /** Base64-encoded binary frame, when present. Prefer the decoded data field. */
  body?: string;
  data: string | Uint8Array;
}

/** Native cell callback identifying a closed socket. */
export interface WebSocketClose {
  id: string;
  code: number;
  reason: string;
  was_clean: boolean;
}

/** Events pulled from a worker server or outbound client socket. */
export type SocketEvent =
  | { type: "open"; protocol: string }
  | { type: "message"; text?: string; body?: string; data: string | Uint8Array }
  | { type: "close"; code: number; reason: string; was_clean: boolean };

/** Invocation-scoped facade over the native WebSocket endpoint. */
export interface WebSocket {
  /** Opaque native ID; never convert it to a JavaScript number. */
  readonly id: string;

  /** Accept a server socket; hibernating cell sockets use registered callbacks. */
  accept(options?: { hibernating?: boolean; tags?: string[] }): Promise<null>;

  /** Send one text or binary frame within the native frame-size limit. */
  send(data: string | Uint8Array): Promise<null>;

  /** Start the close handshake; defaults to code 1000 and an empty reason. */
  close(code?: number, reason?: string): Promise<null>;

  /** Pull an event from a worker server or accepted outbound client endpoint. */
  next(): Promise<SocketEvent>;

  /** Store JSON on the native socket for later cell callbacks. */
  setAttachment(value: Json): Promise<null>;

  /** Read the attachment, or null when absent. */
  getAttachment(): Promise<Json>;

  /** Read the tags supplied when accepting a hibernating socket. */
  tags(): Promise<string[]>;

  /** Last automatic response time in Unix milliseconds, or null. */
  autoResponseTimestamp(): Promise<number | null>;

  /** Read the negotiated protocol, outbound URL, and native connection state. */
  info(): Promise<{
    protocol: string;
    url: string | null;
    state: "Connecting" | "Open" | "Closing" | "Closed";
  }>;
}

/** Stream bounded text or byte chunks under the original invocation. */
export function streamResponse(
  chunks: Chunks,
  options?: { status?: number; headers?: Headers },
): StreamingResponseData;

/** Format one SSE event, preserving multiline data and validating field values. */
export function sseEvent(
  data: string,
  options?: { event?: string; id?: string; retry?: number },
): string;

/** Pass already formatted SSE events, for example from sseEvent(). */
export function sseResponse(
  events: Chunks,
  options?: { status?: number; headers?: Headers },
): StreamingResponseData;

/** Build an upgrade; workers supply run, while hibernating cells use callbacks. */
export function webSocketResponse(
  socket: WebSocket,
  options?: { headers?: Headers; run?: WebSocketResponseData["run"] },
): WebSocketResponseData;
