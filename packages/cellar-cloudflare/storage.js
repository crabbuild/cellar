// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { sqlStorage } from "./sql.js";

function options(value, allowed) {
  if (value === undefined) return {};
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some((key) => !allowed.includes(key))
  ) {
    throw new TypeError("Unsupported storage options");
  }
  // These are hints, not permission to bypass native durability/output gates.
  for (const key of ["allowConcurrency", "allowUnconfirmed", "noCache"]) {
    if (value[key] !== undefined && typeof value[key] !== "boolean")
      throw new TypeError("Invalid storage option");
  }
  return value;
}
function key(value) {
  if (typeof value !== "string" || !value || !value.isWellFormed())
    throw new TypeError("Storage keys must be nonempty Unicode strings");
  return value;
}

export function durableStorage(current, transaction = false) {
  const kv = Object.freeze({
    get(keys) {
      const invocation = current();
      const many = Array.isArray(keys);
      const requested = many ? keys.map(key) : [key(keys)];
      const rows = invocation._sync("workers.kv.get", { keys: requested });
      const result = new Map(
        rows.map(([name, value]) => [name, invocation._deserialize(value)]),
      );
      return many ? result : result.get(keys);
    },
    put(name, value) {
      const invocation = current();
      const entries =
        typeof name === "string"
          ? [[key(name), value]]
          : Object.entries(name).map(([name, value]) => [key(name), value]);
      const encoded = entries.map(([name, value]) => [
        name,
        invocation._serialize(value),
      ]);
      invocation._sync("workers.kv.put", { entries: encoded });
    },
    delete(keys) {
      const many = Array.isArray(keys);
      const count = current()._sync("workers.kv.delete", {
        keys: many ? keys.map(key) : [key(keys)],
      });
      return many ? count : count === 1;
    },
    list(value) {
      value = options(value, [
        "start",
        "end",
        "startAfter",
        "prefix",
        "limit",
        "reverse",
      ]);
      const wire = {};
      for (const name of ["start", "end", "prefix", "limit", "reverse"])
        if (value[name] !== undefined) wire[name] = value[name];
      if (value.startAfter !== undefined) wire.start_after = value.startAfter;
      const invocation = current();
      return new Map(
        invocation
          ._sync("workers.kv.list", wire)
          .map(([key, value]) => [key, invocation._deserialize(value)]),
      );
    },
  });
  const readOptions = ["allowConcurrency", "noCache"];
  const writeOptions = ["allowConcurrency", "allowUnconfirmed", "noCache"];
  return Object.freeze({
    ...(transaction
      ? {
          rollback() {
            current()._sync("workers.callback.rollback", {});
          },
        }
      : {}),
    kv,
    sql: sqlStorage(current),
    async get(key, value) {
      options(value, readOptions);
      return kv.get(key);
    },
    async put(name, value, config) {
      if (typeof name !== "string") {
        config = value;
        value = undefined;
      }
      options(config, writeOptions);
      kv.put(name, value);
    },
    async delete(key, value) {
      options(value, writeOptions);
      return kv.delete(key);
    },
    async list(value) {
      value = options(value, [
        ...readOptions,
        "start",
        "end",
        "startAfter",
        "prefix",
        "limit",
        "reverse",
      ]);
      const { allowConcurrency, noCache, ...query } = value;
      return kv.list(query);
    },
    async getAlarm(value) {
      options(value, ["allowConcurrency"]);
      return current()._call("alarm.get");
    },
    async setAlarm(time, value) {
      options(value, ["allowConcurrency", "allowUnconfirmed"]);
      if (time instanceof Date) time = time.getTime();
      if (!Number.isSafeInteger(time) || time < 0)
        throw new TypeError(
          "Alarm time must be nonnegative integral milliseconds",
        );
      await current()._call("storage.apply", { alarm_at_ms: time });
    },
    async deleteAlarm(value) {
      options(value, ["allowConcurrency", "allowUnconfirmed"]);
      await current()._call("storage.apply", { delete_alarm: true });
    },
    async deleteAll(value) {
      options(value, writeOptions);
      current()._sync("workers.kv.delete_all");
    },
    sync: () => current().storage.sync(),
    transaction(callback) {
      if (typeof callback !== "function")
        throw new TypeError("transaction requires a callback");
      return current()._callback("transaction", (scope) =>
        callback(durableStorage(scope, true)),
      );
    },
    transactionSync(callback) {
      return current()._callbackSync(callback);
    },
  });
}
