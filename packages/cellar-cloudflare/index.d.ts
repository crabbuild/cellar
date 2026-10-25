// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/** The implemented partial profile. This is not the full Cloudflare API. */
declare const durableObjectId: unique symbol;
export interface DurableObjectId {
  readonly [durableObjectId]: never;
  readonly name: string | undefined;
  toString(): string;
  equals(other: DurableObjectId): boolean;
}
export type SqlValue = null | number | string | ArrayBuffer;
export interface SqlIterator<T> extends IterableIterator<T> {
  toArray(): T[];
}
export interface SqlCursor<T> extends SqlIterator<T> {
  readonly columnNames: readonly string[];
  readonly rowsRead: number;
  readonly rowsWritten: number;
  raw(): SqlIterator<SqlValue[]>;
  one(): T;
}
export interface SqlStorage {
  readonly databaseSize: number;
  exec<T extends Record<string, SqlValue> = Record<string, SqlValue>>(
    query: string,
    ...parameters: (SqlValue | ArrayBufferView)[]
  ): SqlCursor<T>;
}
export interface ListOptions {
  start?: string;
  end?: string;
  startAfter?: string;
  prefix?: string;
  limit?: number;
  reverse?: boolean;
}
export interface ReadOptions {
  allowConcurrency?: boolean;
  noCache?: boolean;
}
export interface WriteOptions extends ReadOptions {
  allowUnconfirmed?: boolean;
}
export interface DurableObjectKV {
  get<T = unknown>(key: string): T | undefined;
  get<T = unknown>(keys: string[]): Map<string, T>;
  put<T>(key: string, value: T): void;
  put<T>(entries: Record<string, T>): void;
  delete(key: string): boolean;
  delete(keys: string[]): number;
  list<T = unknown>(options?: ListOptions): Map<string, T>;
}
export interface DurableObjectStorage {
  readonly kv: DurableObjectKV;
  readonly sql: SqlStorage;
  get<T = unknown>(key: string, options?: ReadOptions): Promise<T | undefined>;
  get<T = unknown>(
    keys: string[],
    options?: ReadOptions,
  ): Promise<Map<string, T>>;
  put<T>(key: string, value: T, options?: WriteOptions): Promise<void>;
  put<T>(entries: Record<string, T>, options?: WriteOptions): Promise<void>;
  delete(key: string, options?: WriteOptions): Promise<boolean>;
  delete(keys: string[], options?: WriteOptions): Promise<number>;
  list<T = unknown>(
    options?: ListOptions & ReadOptions,
  ): Promise<Map<string, T>>;
  getAlarm(
    options?: Pick<ReadOptions, "allowConcurrency">,
  ): Promise<number | null>;
  setAlarm(
    time: number | Date,
    options?: Omit<WriteOptions, "noCache">,
  ): Promise<void>;
  deleteAlarm(options?: Omit<WriteOptions, "noCache">): Promise<void>;
  deleteAll(options?: WriteOptions): Promise<void>;
  sync(): Promise<null>;
  transaction<T>(
    callback: (transaction: DurableObjectTransaction) => T | Promise<T>,
  ): Promise<T>;
  /** Native synchronous transaction; callbacks returning Promises are refused. */
  transactionSync<T>(callback: () => T extends Promise<unknown> ? never : T): T;
}
export interface DurableObjectTransaction extends DurableObjectStorage {
  rollback(): void;
}
export interface DurableObjectState {
  readonly id: DurableObjectId;
  readonly storage: DurableObjectStorage;
  readonly container: unknown;
  readonly exports: never;
  blockConcurrencyWhile<T>(callback: () => T | Promise<T>): Promise<T>;
  /** Requires Source.workers_lifetimes() in the native host. */
  waitUntil(promise: Promise<unknown>): void;
  abort(): never;
}
type Reserved =
  | "constructor"
  | "dup"
  | "fetch"
  | "connect"
  | "alarm"
  | "webSocketMessage"
  | "webSocketClose"
  | "webSocketError";
/** Runtime additionally enforces prototype-only visibility and clone limits. */
export type DurableObjectStub<T extends object> = {
  readonly id: DurableObjectId;
  readonly name: string | undefined;
} & {
  [
    K in keyof T as K extends Reserved
      ? never
      : T[K] extends (...args: never[]) => unknown
        ? K
        : never
  ]: T[K] extends (...args: infer A) => infer R
    ? (...args: A) => Promise<Awaited<R>>
    : never;
};
export interface DurableObjectNamespace<T extends object> {
  idFromName(name: string): DurableObjectId;
  idFromString(id: string): DurableObjectId;
  newUniqueId(): DurableObjectId;
  get(id: DurableObjectId): DurableObjectStub<T>;
  getByName(name: string): DurableObjectStub<T>;
  jurisdiction(): never;
}
/** Supply the real Cellar handler context; it is an opaque native capability. */
export function durableObjectNamespace<
  T extends object = Record<string, (...args: unknown[]) => unknown>,
>(nativeContext: object, binding: string): DurableObjectNamespace<T>;
/** The accessor must return the current native context for the same activation. */
export function durableStorage(current: () => object): DurableObjectStorage;
export interface Activation<T> {
  invoke<R>(callback: (target: T) => R | Promise<R>): Promise<R>;
  rpc(method: string, args: unknown[]): Promise<unknown>;
}
export function activateDurableObject<T, E extends object>(
  Target: new (state: DurableObjectState, env: E) => T,
  nativeContext: object,
  options?: { env?: E; container?: unknown },
): Activation<T>;
