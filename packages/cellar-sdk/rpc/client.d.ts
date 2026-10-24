import type { Context } from "../index.js";
import type { InputLimits } from "./input.js";
import type { createCodec } from "./codec.js";

/** The common AbortSignal surface, usable without DOM types in embedded V8. */
export interface RpcAbortSignal {
  readonly aborted: boolean;
  addEventListener(type: "abort", listener: () => void, options?: {once?: boolean}): void;
  removeEventListener(type: "abort", listener: () => void): void;
}

/** Call controls. The generated client never automatically retries. */
export interface RpcCallOptions {
  timeoutMs?: number;
  /** Native Node signals and bundled AbortController implementations are accepted.
   * Cancellation waits for native cleanup and never promises rollback. */
  signal?: RpcAbortSignal;
}

/** Codec-neutral transport metadata; generated APIs supply concrete types. */
export interface RpcMethod {
  readonly method: string;
  readonly digest: string;
  readonly handler: string;
  readonly input: string;
  readonly output: string;
  /** Internal startup/client check against the native artifact inventory. */
  verify(contract: unknown): void;
  encodeRequest(request: unknown, limits?: InputLimits): Uint8Array;
  decodeResponse(bytes: Uint8Array, limits?: InputLimits): unknown;
  decode(bytes: Uint8Array, limits?: InputLimits): unknown;
  encode(response: unknown, limits?: InputLimits): Uint8Array;
}

export interface RpcRegistration {
  readonly methods: Readonly<Record<string, RpcMethod>>;
}

export function createMethod(
  metadata: {
    method: string;
    digest: string;
    handler: string;
    inputSchema: { readonly typeName: string };
    outputSchema: { readonly typeName: string };
  },
  codec: ReturnType<typeof createCodec>,
): RpcMethod;

export function createRegistration(methods: Iterable<RpcMethod>): RpcRegistration;

/** Internal byte transport. Generated subclasses retain request/response types. */
export class RpcClient {
  constructor(context: Context, binding: string, name: string);
  call(method: RpcMethod, request: unknown, options?: RpcCallOptions): Promise<unknown>;
}
