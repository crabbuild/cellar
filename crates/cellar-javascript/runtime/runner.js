(host) => {
  "use strict";
  const parse = JSON.parse, stringify = JSON.stringify, freeze = Object.freeze;
  const entries = Object.entries, isArray = Array.isArray;
  const MapClass = Map, ErrorClass = Error;
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  function json(value, depth = 0) {
    if (depth > 64) throw new ErrorClass("ValueDepth");
    if (value === null || typeof value === "string" || typeof value === "boolean") return value;
    if (typeof value === "number" && Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value))) return value;
    if (isArray(value)) return value.map(v => json(v, depth + 1));
    if (typeof value === "object" && value !== null && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)) {
      const result = Object.create(null);
      for (const [key, v] of entries(value)) result[key] = json(v, depth + 1);
      return result;
    }
    throw new ErrorClass("ValueType: use finite JSON values and safe integers");
  }
  function decode(text) {
    const bytes = [];
    let bits = 0, count = 0;
    for (const c of text) {
      if (c === "=") break;
      const n = alphabet.indexOf(c);
      if (n < 0) throw new ErrorClass("InvalidBase64");
      bits = (bits << 6) | n; count += 6;
      if (count >= 8) { count -= 8; bytes.push((bits >> count) & 255); }
    }
    return new Uint8Array(bytes);
  }
  function request(value) {
    const body = decode(value.body);
    const text = () => decodeURIComponent(Array.from(body, b => "%" + b.toString(16).padStart(2, "0")).join(""));
    return freeze({...value, body, text, json: () => json(parse(text()))});
  }
  function context(id, identity) {
    const call = async (name, value = {}) => {
      let encoded, pending;
      try { encoded = stringify(json(value)); }
      catch { const error = new ErrorClass("ValidationError"); error.code = "ValidationError"; error.outcome = "not_committed"; throw error; }
      try { pending = host(id, name, encoded); }
      catch (error) { error.code = error.message; error.outcome = "not_committed"; throw error; }
      const result = parse(await pending);
      if (!result.ok) {
        const error = new ErrorClass(result.code);
        error.code = result.code; error.outcome = result.outcome;
        throw error;
      }
      // Unsafe native integers are refused rather than silently rounded.
      try { return json(result.value); }
      catch {
        const read = ["clock.now", "storage.read", "alarm.get", "var", "kv.get", "kv.list"].includes(name);
        const error = new ErrorClass(read ? "HostError" : "Ambiguous");
        error.code = error.message; error.outcome = "unknown"; throw error;
      }
    };
    return freeze({
      identity: identity && freeze(identity),
      now: () => call("clock.now"),
      variable: name => call("var", {name}),
      storage: freeze({
        read: key => call("storage.read", {key}),
        apply: batch => call("storage.apply", batch),
        sync: () => call("storage.sync"),
        sql: (sql, params = []) => call("storage.sql", {sql, params}),
        getAlarm: () => call("alarm.get"),
      }),
      cell: (binding, name) => freeze({fetch: async value => request(await call("cell.fetch", {binding, name, request: value}))}),
      kv: binding => freeze({
        get: key => call("kv.get", {binding, key}),
        put: (key, value, options = {}) => call("kv.put", {binding, key, value, ...options}),
        delete: key => call("kv.delete", {binding, key}),
        list: (options = {}) => call("kv.list", {binding, prefix: "", ...options}),
      }),
      sql: binding => freeze({query: (sql, params = []) => call("sql.query", {binding, sql, params})}),
      queue: binding => freeze({send: (body, options = {}) => call("queue.send", {binding, body, ...options})}),
      settle: options => call("queue.settle", options),
      noRetry: () => call("cron.no_retry"),
    });
  }
  return (app, descriptor, maximum) => {
    const declared = parse(descriptor), objects = new MapClass();
    for (const name of declared.events) if (typeof app[name] !== "function") throw new ErrorClass("Missing application handler");
    for (const [name, events] of entries(declared.cells)) {
      if (typeof app.cells?.[name] !== "function") throw new ErrorClass("Missing cell class");
      for (const event of events) if (typeof app.cells[name].prototype[event] !== "function") throw new ErrorClass("Missing cell handler");
    }
    return freeze({
      async dispatch(id, payload) {
        const invocation = parse(payload);
        const ctx = context(id, invocation.identity);
        let target = app;
        if (invocation.object !== null) {
          target = objects.get(invocation.object);
          if (!target) {
            if (objects.size >= maximum) throw new ErrorClass("ObjectLimit");
            target = new app.cells[invocation.class]();
            objects.set(invocation.object, target);
          }
        }
        const event = invocation.kind === "fetch" ? request(invocation.event) : json(invocation.event);
        const result = await target[invocation.kind](event, ctx);
        return stringify(json(invocation.kind === "fetch" ? result : null));
      },
      retire(id) { objects.delete(id); },
    });
  };
}
