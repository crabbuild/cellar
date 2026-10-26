// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

const descriptors = new WeakMap();
class DurableObjectId {
  #value;
  #name;
  constructor(value, name) {
    this.#value = value;
    this.#name =
      name === null
        ? undefined
        : name.map((unit) => String.fromCharCode(unit)).join("");
    descriptors.set(
      this,
      Object.freeze({
        id: value,
        name: name === null ? null : Object.freeze([...name]),
      }),
    );
    Object.defineProperty(this, "__cellar_runtime_capability__", {
      value: () => {},
      enumerable: true,
    });
    Object.freeze(this);
  }
  get name() {
    return this.#name;
  }
  toString() {
    return this.#value;
  }
  equals(other) {
    if (!(other instanceof DurableObjectId))
      throw new TypeError("Expected a DurableObjectId");
    return this.#value === other.#value;
  }
}
export function nativeId({ id, name }) {
  if (
    typeof id !== "string" ||
    !/^[0-9a-f]{64}$/.test(id) ||
    (name !== null &&
      (!Array.isArray(name) ||
        !name.every(
          (unit) => Number.isInteger(unit) && unit >= 0 && unit <= 65535,
        )))
  )
    throw new TypeError("Invalid native Durable Object ID");
  return new DurableObjectId(id, name);
}
export function idDescriptor(id) {
  const descriptor = descriptors.get(id);
  if (!descriptor) throw new TypeError("Expected a DurableObjectId");
  return descriptor;
}
