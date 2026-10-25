// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Cursors retain only an invocation-owned token. Actual stepping, statements,
// schema protection and resource cleanup remain in native cell SQLite.
export function sqlStorage(current) {
  function parameter(value) {
    if (value === null) return { type: "null" };
    if (typeof value === "string") return { type: "text", value };
    if (typeof value === "number")
      return { type: "number", value: String(value) };
    if (value instanceof ArrayBuffer) value = new Uint8Array(value);
    if (ArrayBuffer.isView(value)) {
      return {
        type: "blob",
        value: current()._encode(
          new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        ),
      };
    }
    throw new TypeError(
      "SQL parameters must be null, numbers, strings or binary buffers",
    );
  }
  function value(value) {
    switch (value.type) {
      case "null":
        return null;
      case "number":
        return Number(value.value);
      case "text":
        return value.value;
      case "blob":
        return current()._decode(value.value).buffer;
      default:
        throw new Error("Invalid SQL value");
    }
  }
  return Object.freeze({
    get databaseSize() {
      return current()._sync("workers.sql.size");
    },
    exec(query, ...parameters) {
      const invocation = current();
      if (typeof query !== "string")
        throw new TypeError("SQL query must be a string");
      const initial = invocation._sync("workers.sql.exec", {
        sql: query,
        params: parameters.map(parameter),
      });
      let done = false;
      let read = initial.rows_read;
      let written = initial.rows_written;
      const columns = Object.freeze(initial.columns);
      function next(raw) {
        // Reusing the DO storage facade is allowed in a later event; reusing
        // a cursor must never borrow the later invocation's resource table.
        if (done) return { done: true };
        const row = invocation._sync("workers.sql.next", { id: initial.id });
        done = row.done;
        read = row.rows_read;
        written = row.rows_written;
        if (done) return { done: true };
        const values = row.row.map(value);
        return {
          done: false,
          value: raw
            ? values
            : Object.fromEntries(columns.map((name, i) => [name, values[i]])),
        };
      }
      function close() {
        if (!done) {
          invocation._sync("workers.sql.close", { id: initial.id });
          done = true;
        }
        return { done: true };
      }
      function iterator(raw) {
        return {
          [Symbol.iterator]() {
            return this;
          },
          next: () => next(raw),
          return: close,
          toArray() {
            return [...this];
          },
        };
      }
      const cursor = {
        ...iterator(false),
        get columnNames() {
          return columns;
        },
        get rowsRead() {
          return read;
        },
        get rowsWritten() {
          return written;
        },
        raw: () => iterator(true),
        one() {
          const first = next(false);
          const second = next(false);
          if (first.done || !second.done) {
            close();
            throw new Error("Expected exactly one SQL row");
          }
          return first.value;
        },
      };
      return cursor;
    },
  });
}
