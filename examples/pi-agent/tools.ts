// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {createHash} from 'node:crypto';
import type {AgentTool} from '@earendil-works/pi-agent-core';
import {Type} from 'typebox';
import type {Batch, Context, Json} from '@cellar/sdk';
import {createFilesystemTools, filesystemToolRefusal, type FilesystemToolRequest} from '@cellar/sdk/filesystem-tools';

const path = Type.String({minLength: 1, maxLength: 4096});
const integer = Type.String({maxLength: 19, pattern: '^(0|[1-9][0-9]*)$'});
const object = {additionalProperties: false};
export const schemas = {
  increment: Type.Object({amount: Type.Integer({minimum: 1, maximum: 100})}, object),
  readFile: Type.Object({path, offset: Type.Optional(integer), max_bytes: Type.Optional(Type.Integer({minimum: 1, maximum: 32768}))}, object),
  listFiles: Type.Object({path, limit: Type.Optional(Type.Integer({minimum: 1, maximum: 128})), cursor: Type.Optional(Type.Union([
    Type.Null(), Type.Object({scope: Type.String({maxLength: 4096}), inode: integer, position: integer}, object),
  ]))}, object),
  writeFile: Type.Object({path, content: Type.String({maxLength: 8192})}, object),
  editFile: Type.Object({path, inode: integer, version: integer, offset: integer, delete_bytes: integer, insert: Type.String({maxLength: 8192})}, object),
  renameFile: Type.Object({path, destination: path}, object),
  deleteFile: Type.Object({path}, object),
  searchFiles: Type.Object({path, pattern: Type.String({minLength: 1, maxLength: 128})}, object),
};

export interface ToolResult {content: {type: 'text'; text: string}[]; details: Json; isError?: boolean}
export interface ToolEntry {request: string; result: ToolResult}
export type Effects = Pick<Batch, 'checks' | 'puts' | 'filesystem_checks' | 'filesystem_mutations'>;

export function fingerprint(value: unknown): string {
  const ordered = (item: unknown): unknown => Array.isArray(item) ? item.map(ordered)
    : item && typeof item === 'object'
      ? Object.fromEntries(Object.keys(item).sort().map(key => [key, ordered((item as Record<string, unknown>)[key])])) : item;
  return createHash('sha256').update(JSON.stringify(ordered(value))).digest('hex');
}

function result(name: string, details: Json): ToolResult {
  const count = details && typeof details === 'object' && !Array.isArray(details) ? details.count : undefined;
  const refused = details && typeof details === 'object' && !Array.isArray(details) && 'error' in details;
  return {content: [{type: 'text', text: name === 'increment' ? String(count) : JSON.stringify(details)}],
    details, ...(refused ? {isError: true} : {})};
}

export function agentTools(ctx: Context, journal: {tools: Record<string, ToolEntry>},
  save: (effects?: Effects) => Promise<void>, options: {
    check: () => void; failed: (error: unknown) => void;
    emit: (result: ToolResult, replayed: boolean) => void; committed: () => Promise<void>;
  }): AgentTool[] {
  const files = createFilesystemTools(ctx);
  async function receipt(name: string, args: unknown, id: string,
    prepare: () => Promise<{result: Json} & Effects>): Promise<ToolResult> {
    try {
      options.check();
      const key = fingerprint({id}); // Raw model IDs never become object properties.
      const request = fingerprint({name, args});
      const cached = journal.tools[key];
      if (cached) {
        if (cached.request !== request) throw new Error('tool replay diverged');
        options.emit(cached.result, true);
        return structuredClone(cached.result);
      }
      if (Object.keys(journal.tools).length >= 16) throw new Error('tool limit exceeded');
      const prepared = await prepare();
      const {result: value, ...effects} = prepared;
      let output = result(name, value);
      journal.tools[key] = {request, result: output};
      try {
        await save(effects);
      } catch (error) {
        const refusal = effects.filesystem_checks || effects.filesystem_mutations ? filesystemToolRefusal(error) : null;
        if (refusal === null) throw error;
        // The rejected batch proved no commit. Save only the refusal under the
        // original run guards; never resubmit its filesystem effects.
        output = result(name, refusal);
        journal.tools[key].result = output;
        await save({checks: effects.checks, puts: effects.puts});
      }
      options.emit(output, false);
      await options.committed();
      return structuredClone(output);
    } catch (error) {
      options.failed(error);
      throw error;
    }
  }
  const fileTools = [
    ['readFile', 'read', 'Read bounded bytes from this turn’s cell files; replay returns saved bytes and identity/version.'],
    ['listFiles', 'list', 'List a bounded directory page; pass the opaque continuation cursor for another page.'],
    ['writeFile', 'write', 'Create or overwrite bounded text; file effects and this tool receipt commit atomically.'],
    ['editFile', 'edit', 'Edit a byte range against the exact inode/version from a read; inspect again after a conflict.'],
    ['renameFile', 'rename', 'Rename a cell file with an atomic receipt.'],
    ['deleteFile', 'unlink', 'Unlink a cell file with an atomic receipt.'],
    ['searchFiles', 'search', 'Search bounded file prefixes for literal text; results report traversal and truncation limits.'],
  ] as const;
  return [{
    name: 'increment', label: 'Increment task counter', description: 'Add an integer to the durable task counter', parameters: schemas.increment,
    execute: (id, args) => receipt('increment', args, id, async () => {
      if (!args || typeof args !== 'object' || !('amount' in args)
        || typeof args.amount !== 'number' || !Number.isInteger(args.amount)
        || args.amount < 1 || args.amount > 100) throw new Error('invalid increment arguments');
      const before = await ctx.storage.read('count');
      const count = (before.present ? Number(before.value) : 0) + args.amount;
      if (!Number.isSafeInteger(count)) throw new Error('invalid counter');
      return {result: {count}, checks: {count: before}, puts: {count}};
    }),
  }, ...fileTools.map(([name, operation, description]): AgentTool => ({
    name, label: name, description, parameters: schemas[name],
    execute: (id, args) => receipt(name, args, id, () => {
      if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('invalid file tool arguments');
      return files.prepare({...args, operation} as FilesystemToolRequest);
    }),
  }))];
}
