/** Finite JSON numbers only; integer values must fit Number.isSafeInteger. */
export type Json = null | boolean | number | string | Json[] | {[key: string]: Json};
export type Headers = [string, string][];
export interface RequestData {url: string; method: string; headers: Headers; body: string}
export interface ResponseData {status: number; headers: Headers; body: string}
export interface Request extends Omit<RequestData, "body"> {body: Uint8Array; text(): string; json(): Json}
export interface Response extends Omit<ResponseData, "body"> {body: Uint8Array; text(): string; json(): Json}
export interface Snapshot {present: boolean; value: Json}
export interface Batch {checks?: Record<string, Snapshot>; puts?: Record<string, Json>; deletes?: string[]; alarm_at_ms?: number; delete_alarm?: boolean}
export interface SqlResult {columns: string[]; rows: Json[][]; written: number}
export interface Identity {name: string | null; class: string; scope: string; epoch: string}
export interface Context {
  readonly identity: Readonly<Identity> | null;
  now(): Promise<number>;
  variable(name: string): Promise<string | null>;
  readonly storage: {
    read(key: string): Promise<Snapshot>;
    apply(batch: Batch): Promise<null>;
    sync(): Promise<null>;
    sql(sql: string, params?: Json[]): Promise<SqlResult>;
    getAlarm(): Promise<number | null>;
  };
  cell(binding: string, name: string): {fetch(request: RequestData): Promise<Response>};
  kv(binding: string): {
    get(key: string): Promise<Json>;
    put(key: string, value: Json, options?: {ttl_seconds?: number; metadata?: Json}): Promise<null>;
    delete(key: string): Promise<null>;
    list(options?: {prefix?: string; limit?: number; cursor?: string}): Promise<Json>;
  };
  sql(binding: string): {query(sql: string, params?: Json[]): Promise<Json>};
  queue(binding: string): {send(body: Json, options?: {delay_seconds?: number}): Promise<null>};
  settle(options: {retry: boolean; id?: string; delay_seconds?: number}): Promise<null>;
  noRetry(): Promise<null>;
}
export interface Alarm {scheduled_ms: number}
export interface Scheduled extends Alarm {cron: string}
export interface QueueBatch {queue: string; messages: {id: string; attempts: number; timestamp_ms: number; body: Json}[]}
export interface Cell {
  fetch?(request: Request, context: Context): ResponseData | Promise<ResponseData>;
  alarm?(alarm: Alarm, context: Context): void | Promise<void>;
}
export interface Application {
  cells?: Record<string, new () => Cell>;
  fetch?(request: Request, context: Context): ResponseData | Promise<ResponseData>;
  queue?(batch: QueueBatch, context: Context): void | Promise<void>;
  scheduled?(event: Scheduled, context: Context): void | Promise<void>;
}
export interface CellarError extends Error {code: string; outcome: "not_committed" | "unknown"}
export function bytesToBase64(bytes: Uint8Array): string;
export function utf8(text: string): Uint8Array;
export function response(body?: string | Uint8Array, options?: {status?: number; headers?: Headers}): ResponseData;
export function jsonResponse(value: Json, options?: {status?: number; headers?: Headers}): ResponseData;
export function request(url: string, options?: {method?: string; headers?: Headers; body?: string | Uint8Array}): RequestData;
