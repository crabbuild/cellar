// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { Batch, Context, Json, Snapshot, FilesystemPath, FilesystemInteger, DirectoryCursor } from "./index.js";

export interface FilesystemToolLimits {
  prefix: string;
  maxReceipts: number;
  maxReadBytes: number;
  maxWriteBytes: number;
  maxEntries: number;
  maxSearchFiles: number;
  maxSearchDirectories: number;
  maxSearchBytes: number;
  maxSearchDepth: number;
  maxRequestBytes: number;
  maxResultBytes: number;
}
export type FilesystemToolRequest =
  | { operation: "read"; path: FilesystemPath; offset?: FilesystemInteger; max_bytes?: number }
  | { operation: "write" | "replace"; path: FilesystemPath; content: string }
  | { operation: "edit"; path: FilesystemPath; inode: FilesystemInteger; version: FilesystemInteger; offset: FilesystemInteger; delete_bytes: FilesystemInteger; insert: string }
  | { operation: "stat" | "unlink"; path: FilesystemPath }
  | { operation: "list"; path: FilesystemPath; limit?: number; cursor?: DirectoryCursor | null }
  | { operation: "rename"; path: FilesystemPath; destination: FilesystemPath }
  | { operation: "search"; path: FilesystemPath; pattern: string };

export interface FilesystemTools {
  readonly limits: Readonly<FilesystemToolLimits>;
  /** Capture a bounded result and checked effects for an existing agent journal.
   * This commits nothing. Commit all returned checks/effects with the stable
   * request fingerprint and result receipt in ONE storage.apply, then sync.
   * Replay the receipt before preparing; never prepare again after an ambiguous
   * commit. A failed file check requires a recorded refusal or a new decision. */
  prepare(request: FilesystemToolRequest): Promise<PreparedFilesystemTool>;
  /** IDs come from a saved model decision. A lost reply is reconciled by replaying
   * the same ID and request in a fresh invocation; effects and receipt are atomic.
   * Failed/ambiguous operations are never retried internally.
   * Receipts must fit the native storage.read envelope, including escaped
   * request text. A request too large for even a refusal receipt rejects before
   * file mutation; configured tool ceilings are additional upper bounds. */
  execute(id: string, request: FilesystemToolRequest, checkpoint?: {
    checks?: Record<string, Snapshot>;
    puts?: Record<string, Json>;
  }): Promise<{ result: Json; replayed: boolean }>;
}

export interface PreparedFilesystemTool {
  result: Json;
  filesystem_checks: NonNullable<Batch["filesystem_checks"]>;
  filesystem_mutations: NonNullable<Batch["filesystem_mutations"]>;
}

/** Return a bounded receipt value only when the SDK proves no commit. */
export function filesystemToolRefusal(error: unknown): Json | null;

/** Check the complete storage.apply JSON request and every put's future read
 * envelope before file effects. journalKeys reserve room for a later checked
 * terminal update (default 1024 bytes, which must cover the caller's bounded
 * terminal changes and batch overhead). Use journalKeys for pending monolithic
 * journals; terminal writes need no further reserve. This commits nothing and
 * throws a ValidationError with outcome not_committed on budget refusal. */
export function validateFilesystemCheckpoint(batch: Batch, transport: { readonly max_value_bytes: number }, options?: {
  journalKeys?: string[];
  reserveBytes?: number;
}): void;

/** Read/search results are saved within bounded receipts. Search covers bounded
 * file prefixes and reports truncation. Bind this helper to a session-owned cell
 * and serialize mutating turns; it supplies no process execution capability. */
export function createFilesystemTools(ctx: Context, options?: Partial<FilesystemToolLimits>): FilesystemTools;
