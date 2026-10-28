// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { bytesToBase64, utf8 } from "./index.js";

function failure(code, message) {
  const error = new Error(message);
  error.code = code;
  error.outcome = "not_committed";
  return error;
}
function require(value, message) {
  if (!value) throw failure("ValidationError", message);
}
function decimal(value, fallback = 0n) {
  if (value === undefined) return fallback;
  require(typeof value === "bigint" || typeof value === "string" || (typeof value === "number" && Number.isSafeInteger(value)), "Expected an exact filesystem integer");
  const text = String(value);
  require(/^[0-9]+$/.test(text), "Expected a nonnegative decimal integer");
  const number = BigInt(text);
  require(number <= 9223372036854775807n, "Filesystem integer exceeds i64");
  return number;
}
function canonical(value, depth = 0, integersOnly = true) {
  require(depth <= 16, "Tool input exceeds nesting limit");
  if (typeof value === "bigint") return String(value);
  if (value instanceof Uint8Array) return { bytes_base64: bytesToBase64(value) };
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    require(Number.isFinite(value) && (!integersOnly || Number.isSafeInteger(value)), "Tool numbers must be finite and integer arguments must be safe");
    return value;
  }
  require(typeof value === "object", "Tool input must contain JSON values");
  if (Array.isArray(value)) {
    require(value.length <= 128, "Tool input array exceeds limit");
    return value.map(child => canonical(child, depth + 1, integersOnly));
  }
  require([Object.prototype, null].includes(Object.getPrototypeOf(value)), "Tool input must contain plain objects");
  const keys = Object.keys(value).sort();
  require(keys.length <= 64, "Tool input object exceeds limit");
  return Object.fromEntries(keys.map(key => {
    const property = Object.getOwnPropertyDescriptor(value, key);
    require(property && "value" in property, "Tool input cannot contain getters");
    return [key, canonical(property.value, depth + 1, integersOnly)];
  }));
}
function decodeBytes(value) {
  require(typeof value === "string" && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value), "Invalid base64 byte path");
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
  const bytes = new Uint8Array(value.length / 4 * 3 - padding);
  for (let input = 0, output = 0; input < value.length; input += 4) {
    const number = (alphabet.indexOf(value[input]) << 18) | (alphabet.indexOf(value[input + 1]) << 12) | ((value[input + 2] === "=" ? 0 : alphabet.indexOf(value[input + 2])) << 6) | (value[input + 3] === "=" ? 0 : alphabet.indexOf(value[input + 3]));
    if (output < bytes.length) bytes[output++] = number >>> 16;
    if (output < bytes.length) bytes[output++] = number >>> 8;
    if (output < bytes.length) bytes[output++] = number;
  }
  return bytes;
}
function path(value) {
  if (value && typeof value === "object" && Object.keys(value).length === 1 && typeof value.bytes_base64 === "string") return decodeBytes(value.bytes_base64);
  require(typeof value === "string" || value instanceof Uint8Array, "Expected a text or byte path");
  return value;
}
function wirePath(value) {
  return typeof value === "string" ? value : { bytes_base64: bytesToBase64(value) };
}
function text(bytes) {
  try { return decodeURIComponent(Array.from(bytes, byte => `%${byte.toString(16).padStart(2, "0")}`).join("")); }
  catch { return null; }
}
function displayPath(value) {
  const bytes = typeof value === "string" ? utf8(value) : value;
  const decoded = text(bytes);
  return { path_base64: bytesToBase64(bytes), ...(decoded === null ? {} : { path: decoded }) };
}
function childPath(parent, name) {
  const bytes = typeof parent === "string" ? utf8(parent) : parent;
  const slash = bytes.at(-1) === 47 ? 0 : 1;
  const result = new Uint8Array(bytes.length + slash + name.length);
  result.set(bytes);
  if (slash) result[bytes.length] = 47;
  result.set(name, bytes.length + slash);
  return result;
}
function snapshot(path, stat) {
  return stat === null ? { path: wirePath(path), present: false } : {
    path: wirePath(path), present: true, inode: String(stat.inode), version: String(stat.version),
  };
}
function statResult(stat) {
  return { inode: String(stat.inode), version: String(stat.version), kind: stat.kind,
    size: String(stat.size), mode: stat.mode, mtime_ns: String(stat.mtime_ns) };
}
function dirEntry(entry) {
  const name = text(entry.name);
  return { name_base64: bytesToBase64(entry.name), ...(name === null ? {} : { name }),
    inode: String(entry.inode), kind: entry.kind };
}

/** A proven refusal can be journaled without repeating its rejected effects.
 * Unknown/local commits must propagate for reconciliation in a fresh turn. */
export function filesystemToolRefusal(error) {
  if (error?.outcome !== "not_committed" || !["Filesystem", "Conflict", "ValidationError"].includes(error.code)) return null;
  return { error: { code: error.code, errno: error.errno ?? null, outcome: "not_committed" } };
}

/** Admit a file-effect checkpoint before submitting its atomic batch. Journal
 * keys additionally reserve room for a later checked terminal-state update. */
export function validateFilesystemCheckpoint(batch, transport, { journalKeys = [], reserveBytes = 1024 } = {}) {
  const maximum = transport?.max_value_bytes;
  require(Number.isSafeInteger(maximum) && maximum >= 4096, "Invalid filesystem checkpoint transport budget");
  require(Number.isSafeInteger(reserveBytes) && reserveBytes >= 0, "Invalid filesystem checkpoint reserve");
  const size = value => utf8(JSON.stringify(value)).length;
  // storage.apply carries JSON metadata. max_body_bytes bounds binary bodies,
  // not this payload; the bridge charges metadata against max_value_bytes.
  require(size(batch) <= maximum, "Filesystem checkpoint exceeds the transport budget");
  for (const value of Object.values(batch.puts ?? {})) {
    require(size({ ok: true, value: { present: true, value } }) <= maximum,
      "Filesystem checkpoint exceeds the storage read budget");
  }
  const puts = {}, checks = {};
  for (const key of journalKeys) {
    require(Object.hasOwn(batch.puts ?? {}, key), "Filesystem journal key must be written by the checkpoint");
    // Define properties explicitly so application keys cannot alter prototypes.
    Object.defineProperty(puts, key, { value: batch.puts[key], enumerable: true });
    Object.defineProperty(checks, key, { value: { present: true, value: batch.puts[key] }, enumerable: true });
  }
  if (journalKeys.length) require(size({ checks, puts }) + reserveBytes <= maximum,
    "Filesystem journal leaves no room for a checked recovery checkpoint");
}

/**
 * Bounded file tools for a durable, session-owned agent cell. A caller supplies
 * stable operation IDs from its saved model decision. File effects, local state
 * and receipts commit in one storage.apply batch. Replays return saved results;
 * they do not reread files or reapply a mutation. No mutation is retried here.
 */
export function createFilesystemTools(ctx, options = {}) {
  const limits = Object.freeze({
    prefix: "filesystem-tool:", maxReceipts: 256, maxReadBytes: 32768,
    maxWriteBytes: 32768, maxEntries: 128, maxSearchFiles: 16,
    maxSearchDirectories: 16, maxSearchBytes: 131072, maxSearchDepth: 8,
    maxRequestBytes: 131072, maxResultBytes: 131072, ...options,
  });
  require(typeof limits.prefix === "string" && limits.prefix.length <= 128 && limits.prefix.length > 0 && !limits.prefix.startsWith("__sys_"), "Invalid tool receipt prefix");
  for (const [key, value] of Object.entries(limits)) {
    if (key !== "prefix") require(Number.isSafeInteger(value) && value > 0, `Invalid tool limit: ${key}`);
  }
  const countKey = `${limits.prefix}count`;

  async function statOrMissing(path) {
    try { return await ctx.fs.stat(path); }
    catch (error) { if (error.errno === "ENOENT") return null; throw error; }
  }
  async function captureRead(path, count, offset = 0n) {
    const file = await ctx.fs.open(path);
    try {
      const stat = await file.fstat();
      const data = await file.pread(count, offset);
      return { stat, data, check: snapshot(path, stat) };
    } finally { await file.close(); }
  }
  async function prepare(request) {
    const target = path(request.path ?? "/");
    const checks = [], mutations = [];
    let result;
    switch (request.operation) {
      case "read": {
        const count = request.max_bytes ?? limits.maxReadBytes;
        require(Number.isSafeInteger(count) && count > 0 && count <= limits.maxReadBytes, "Read tool exceeds byte limit");
        const offset = decimal(request.offset);
        const read = await captureRead(target, count, offset);
        checks.push(read.check);
        const decoded = text(read.data);
        result = { ...statResult(read.stat), offset: String(offset), data_base64: bytesToBase64(read.data),
          ...(decoded === null ? {} : { text: decoded }), truncated: offset + BigInt(read.data.length) < read.stat.size };
        break;
      }
      case "write": case "replace": {
        require(typeof request.content === "string", "Write tool requires text content");
        const data = utf8(request.content);
        require(data.length <= limits.maxWriteBytes, "Write tool exceeds byte limit");
        checks.push(snapshot(target, await statOrMissing(target)));
        mutations.push({ op: request.operation === "write" ? "write_file" : "replace_file", path: wirePath(target), data_base64: bytesToBase64(data) });
        result = { written_bytes: data.length };
        break;
      }
      case "edit": {
        require(typeof request.insert === "string", "Edit tool requires text insertion");
        const insert = utf8(request.insert);
        require(insert.length <= limits.maxWriteBytes, "Edit tool exceeds byte limit");
        const inode = decimal(request.inode), version = decimal(request.version);
        mutations.push({ op: "edit_file", path: wirePath(target), inode: String(inode), version: String(version),
          offset: String(decimal(request.offset)), delete_bytes: String(decimal(request.delete_bytes)), data_base64: bytesToBase64(insert) });
        result = { expected_inode: String(inode), expected_version: String(version), inserted_bytes: insert.length };
        break;
      }
      case "stat": {
        const stat = await ctx.fs.stat(target);
        checks.push(snapshot(target, stat));
        result = statResult(stat);
        break;
      }
      case "list": {
        const count = request.limit ?? limits.maxEntries;
        require(Number.isSafeInteger(count) && count > 0 && count <= limits.maxEntries, "List tool exceeds entry limit");
        const stat = await ctx.fs.stat(target);
        const page = await ctx.fs.readDir(target, count, request.cursor ?? null);
        checks.push(snapshot(target, stat));
        result = { ...statResult(stat), entries: page.entries.map(dirEntry), next: page.next };
        break;
      }
      case "rename": {
        const destination = path(request.destination);
        checks.push(snapshot(target, await ctx.fs.stat(target)), snapshot(destination, await statOrMissing(destination)));
        mutations.push({ op: "rename", old: wirePath(target), new: wirePath(destination) });
        result = { renamed: true };
        break;
      }
      case "unlink": {
        checks.push(snapshot(target, await ctx.fs.stat(target)));
        mutations.push({ op: "unlink", path: wirePath(target) });
        result = { removed: true };
        break;
      }
      case "search": {
        require(typeof request.pattern === "string" && request.pattern.length > 0 && utf8(request.pattern).length <= 512, "Search requires a literal pattern of 1–512 bytes");
        const queue = [{ path: target, depth: 0 }], matches = [];
        let visitedFiles = 0, visitedDirectories = 0, visitedBytes = 0, visitedEntries = 0, truncated = false;
        while (queue.length && visitedDirectories < limits.maxSearchDirectories && visitedFiles < limits.maxSearchFiles && visitedBytes < limits.maxSearchBytes && visitedEntries < limits.maxEntries) {
          const directory = queue.shift();
          const stat = await ctx.fs.stat(directory.path);
          checks.push(snapshot(directory.path, stat));
          visitedDirectories++;
          const page = await ctx.fs.readDir(directory.path, limits.maxEntries - visitedEntries);
          truncated ||= page.next !== null;
          visitedEntries += page.entries.length;
          for (const entry of page.entries) {
            const location = childPath(directory.path, entry.name);
            if (entry.kind === "Directory") {
              if (directory.depth < limits.maxSearchDepth && queue.length < limits.maxSearchDirectories) queue.push({ path: location, depth: directory.depth + 1 });
              else truncated = true;
              continue;
            }
            if (visitedFiles >= limits.maxSearchFiles || visitedBytes >= limits.maxSearchBytes) { truncated = true; break; }
            const read = await captureRead(location, Math.min(limits.maxReadBytes, limits.maxSearchBytes - visitedBytes));
            checks.push(read.check);
            visitedFiles++; visitedBytes += read.data.length;
            truncated ||= BigInt(read.data.length) < read.stat.size;
            const decoded = text(read.data);
            if (decoded === null) continue;
            const index = decoded.indexOf(request.pattern);
            if (index >= 0) matches.push({ ...displayPath(location), ...statResult(read.stat), excerpt: decoded.slice(Math.max(0, index - 64), index + request.pattern.length + 128) });
          }
        }
        result = { matches, visited_files: visitedFiles, visited_directories: visitedDirectories, visited_bytes: visitedBytes, truncated: truncated || queue.length > 0 };
        break;
      }
      default: throw failure("ValidationError", "Unknown filesystem tool");
    }
    require(utf8(JSON.stringify(result)).length <= limits.maxResultBytes, "Tool result exceeds receipt limit");
    return { result, checks, mutations };
  }

  return Object.freeze({
    limits,
    async prepare(request) {
      const normalized = canonical(request);
      require(utf8(JSON.stringify(normalized)).length <= limits.maxRequestBytes, "Tool request exceeds receipt limit");
      await ctx.fs.capabilities();
      try {
        const { result, checks, mutations } = await prepare(normalized);
        return { result, filesystem_checks: checks, filesystem_mutations: mutations };
      } catch (error) {
        const result = filesystemToolRefusal(error);
        if (result === null) throw error;
        return { result, filesystem_checks: [], filesystem_mutations: [] };
      }
    },
    async execute(id, request, checkpoint = {}) {
      require(typeof id === "string" && /^[A-Za-z0-9_.:-]{1,128}$/.test(id) && id !== "count", "Invalid stable tool operation ID");
      const normalized = canonical(request);
      const savedCheckpoint = canonical(checkpoint, 0, false);
      const identity = JSON.stringify(normalized);
      require(utf8(identity).length <= limits.maxRequestBytes, "Tool request exceeds receipt limit");
      const capabilities = await ctx.fs.capabilities(); // Refuse a missing/retired capability before journaling.
      const maximum = capabilities.transport?.max_value_bytes;
      require(Number.isSafeInteger(maximum) && maximum >= 4096, "Missing filesystem receipt transport budget");
      const key = `${limits.prefix}${id}`;
      const prior = await ctx.storage.read(key);
      if (prior.present) {
        require(prior.value.format === 1 && prior.value.request === identity, "Tool operation ID was reused with different input");
        await ctx.fs.sync();
        return { result: JSON.parse(JSON.stringify(prior.value.result)), replayed: true };
      }
      const count = await ctx.storage.read(countKey);
      const used = count.present ? count.value : 0;
      require(Number.isSafeInteger(used) && used >= 0 && used < limits.maxReceipts, "Tool receipt capacity reached");
      require(!Object.keys(savedCheckpoint.checks ?? {}).concat(Object.keys(savedCheckpoint.puts ?? {})).some(key => key.startsWith(limits.prefix)), "Agent checkpoint collides with tool receipt keys");
      let prepared;
      try { prepared = await prepare(normalized); }
      catch (error) {
        const result = filesystemToolRefusal(error);
        if (result === null) throw error;
        prepared = { result, checks: [], mutations: [] };
      }
      const batch = result => {
        const receipt = { format: 1, request: identity, result };
        // Check the future storage.read success envelope, including the
        // canonical request's second layer of JSON escaping, before effects.
        require(utf8(JSON.stringify({ ok: true, value: { present: true, value: receipt } })).length <= maximum,
          "Tool receipt exceeds the storage read transport budget");
        const value = {
          checks: { ...savedCheckpoint.checks, [key]: prior, [countKey]: count },
          puts: { ...savedCheckpoint.puts, [key]: receipt, [countKey]: used + 1 },
        };
        validateFilesystemCheckpoint(value, { max_value_bytes: maximum });
        return value;
      };
      try {
        const mutation = { ...batch(prepared.result), filesystem_checks: prepared.checks, filesystem_mutations: prepared.mutations };
        validateFilesystemCheckpoint(mutation, { max_value_bytes: maximum });
        await ctx.storage.apply(mutation);
      } catch (error) {
        const result = filesystemToolRefusal(error);
        if (result === null) throw error;
        // The failed batch proved no commit. Record that result without
        // repeating its file mutations; original agent/checkpoint checks still
        // apply, so a cancelled or superseded turn cannot publish this receipt.
        prepared.result = result;
        await ctx.storage.apply(batch(prepared.result));
      }
      await ctx.fs.sync();
      return { result: prepared.result, replayed: false };
    },
  });
}
