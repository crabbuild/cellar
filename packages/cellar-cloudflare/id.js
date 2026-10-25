// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

class DurableObjectId {
  #value;
  #name;
  constructor(value, name) {
    this.#value = value;
    this.#name = name === null ? undefined : name.map(unit => String.fromCharCode(unit)).join("");
    Object.defineProperty(this, "__cellar_runtime_capability__", { value: () => {}, enumerable: true });
    Object.freeze(this);
  }
  get name() { return this.#name; }
  toString() { return this.#value; }
  equals(other) {
    if (!(other instanceof DurableObjectId)) throw new TypeError("Expected a DurableObjectId");
    return this.#value === other.#value;
  }
}
export function nativeId({ id, name }) {
  if (!/^[0-9a-f]{64}$/.test(id)) throw new TypeError("Invalid native Durable Object ID");
  return new DurableObjectId(id, name);
}
