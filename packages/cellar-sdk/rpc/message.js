import { Scalar, scalar, mapKey, refuse, varintSize } from "./scalar.js";
import { inputLimits } from "./input.js";

const plans = new WeakMap();
const wrappers = new Set([
  "google.protobuf.DoubleValue", "google.protobuf.FloatValue", "google.protobuf.Int64Value",
  "google.protobuf.UInt64Value", "google.protobuf.Int32Value", "google.protobuf.UInt32Value",
  "google.protobuf.BoolValue", "google.protobuf.StringValue", "google.protobuf.BytesValue",
]);

function plan(schema) {
  let result = plans.get(schema);
  if (result) return result;
  if (
    schema?.kind !== "message" || schema.file?.edition !== 999 ||
    typeof schema.typeName !== "string" || !Array.isArray(schema.members) ||
    !Array.isArray(schema.fields) || schema.fields.length > 65_536
  ) {
    refuse();
  }
  const members = new Map();
  for (const member of schema.members) {
    if (members.has(member.localName)) refuse();
    members.set(member.localName, member);
  }
  result = {members};
  plans.set(schema, result);
  return result;
}

/**
 * Validate a Protobuf-ES initializer and calculate its exact encoded size before
 * create()/toBinary() can allocate a recursive message or binary writer buffer.
 * The returned plain initializer owns container properties and invokes no input
 * getters. Bytes remain fixed private views until the native submission copies
 * them. Wire elements/depth use the same occurrence rules as native preflight.
 */
export function prepareMessage(schema, input, limits = {}) {
  const {maxDepth, maxElements, maxMessageBytes, maxDecodedSlots} = inputLimits(limits);
  const ancestors = new Set();
  let remaining = maxElements;
  let slots = maxDecodedSlots;

  function layout(count) {
    slots -= count;
    if (slots < 0) refuse("ResourceExhausted");
  }

  function depth(value) {
    if (value > maxDepth) refuse("ResourceExhausted");
  }

  function charge(count = 1) {
    layout(count);
    remaining -= count;
    if (remaining < 0) refuse("ResourceExhausted");
  }

  function add(left, right) {
    const size = left + right;
    if (size > maxMessageBytes) refuse("ResourceExhausted");
    return size;
  }

  const tag = (number, wire) => varintSize(BigInt(number) * 8n + BigInt(wire));
  const delimited = size => add(varintSize(BigInt(size)), size);

  function object(value, array = false) {
    if (value === null || typeof value !== "object" || ancestors.has(value)) refuse();
    const prototype = Object.getPrototypeOf(value);
    const invalid = array
      ? !Array.isArray(value) || prototype !== Array.prototype
      : Array.isArray(value) || prototype !== Object.prototype && prototype !== null;
    if (invalid) refuse();
    ancestors.add(value);
    return value;
  }

  function own(value) {
    const keys = Reflect.ownKeys(value);
    if (keys.length > 65_538 + maxElements) refuse("ResourceExhausted");
    const fields = new Map();
    for (const key of keys) {
      if (typeof key !== "string") refuse();
      const property = Object.getOwnPropertyDescriptor(value, key);
      if (!property || !Object.hasOwn(property, "value")) refuse();
      fields.set(key, property.value);
    }
    return fields;
  }

  function value(field, item, currentDepth, kind = field.fieldKind) {
    if (field.delimitedEncoding || field.longAsString) refuse();
    if (kind === "enum") return scalar(Scalar.INT32, item, maxMessageBytes);
    if (kind === "scalar") return scalar(field.scalar, item, maxMessageBytes);
    if (kind !== "message") refuse();

    let child;
    // These two field representations are part of the pinned codec's API.
    // Root wrappers/Struct and wrappers in lists/maps/oneofs remain messages.
    if (field.message.typeName === "google.protobuf.Struct" && field.parent.typeName !== "google.protobuf.Value") {
      child = jsonStruct(item, currentDepth + 1);
    } else if (field.fieldKind === "message" && !field.oneof && wrappers.has(field.message.typeName)) {
      depth(currentDepth + 1);
      layout(2); // Wrapper object and its declared value field.
      const wrapped = scalar(field.message.fields[0].scalar, item, maxMessageBytes);
      if (!wrapped.zero) charge();
      child = {input:wrapped.value, size:wrapped.zero ? 0 : add(tag(1, wrapped.wire), wrapped.size)};
    } else {
      child = message(field.message, item, currentDepth + 1);
    }
    return {value:child.input, size:delimited(child.size), wire:2, zero:false};
  }

  function fieldValue(field, item, currentDepth, explicit = false) {
    if (field.fieldKind === "list") {
      object(item, true);
      if (item.length > remaining) refuse("ResourceExhausted");
      const entries = own(item);
      if (entries.size !== item.length + 1 || !entries.has("length")) refuse();
      const output = [];
      let size = 0;
      if (item.length > 0 && field.packed) charge();
      for (let index = 0; index < item.length; index++) {
        if (!entries.has(String(index))) refuse();
        charge();
        const child = value(field, entries.get(String(index)), currentDepth, field.listKind);
        size = add(size, field.packed ? child.size : add(tag(field.number, child.wire), child.size));
        output.push(child.value);
      }
      ancestors.delete(item);
      if (item.length && field.packed) size = add(tag(field.number, 2), delimited(size));
      return {value:output, size};
    }

    if (field.fieldKind === "map") {
      object(item);
      const entries = own(item);
      if (entries.size > Math.floor(remaining / 3)) refuse("ResourceExhausted");
      const output = Object.create(null);
      let size = 0;
      for (const [key, entry] of entries) {
        depth(currentDepth + 1); // The synthetic map-entry message.
        layout(3); // Entry object plus declared key/value fields.
        charge(3); // Outer entry occurrence, key field, value field.
        const keyValue = mapKey(field.mapKey, key, maxMessageBytes);
        const child = value(field, entry, currentDepth + 1, field.mapKind);
        const entrySize = add(add(tag(1, keyValue.wire), keyValue.size), add(tag(2, child.wire), child.size));
        size = add(size, add(tag(field.number, 2), delimited(entrySize)));
        output[key] = child.value;
      }
      ancestors.delete(item);
      return {value:output, size};
    }

    const child = value(field, item, currentDepth);
    if (!explicit && field.presence === 2 && child.zero) return {value:child.value, size:0};
    charge();
    return {value:child.value, size:add(tag(field.number, child.wire), child.size)};
  }

  function message(desc, item, currentDepth) {
    depth(currentDepth);
    layout(desc.fields.length + 1);
    object(item);
    const {members} = plan(desc);
    const entries = own(item);
    const output = Object.create(null);
    let size = 0;
    for (const [name, input] of entries) {
      if (name === "$typeName") {
        if (input !== desc.typeName) refuse();
        continue;
      }
      // Unknown wire fields are accepted on decode and deliberately discarded.
      // Never forward caller-built $unknown buffers around the size preflight.
      if (name === "$unknown") continue;

      const member = members.get(name);
      if (!member) refuse();
      if (input === undefined) continue;
      if (member.kind === "oneof") {
        object(input);
        const group = own(input);
        if ([...group.keys()].some(key => key !== "case" && key !== "value")) refuse();
        const selected = group.get("case");
        if (selected === undefined) {
          if (group.get("value") !== undefined) refuse();
          output[name] = {case:undefined};
        } else {
          const field = member.fields.find(field => field.localName === selected);
          if (!field || !group.has("value") || group.get("value") === undefined) refuse();
          const child = fieldValue(field, group.get("value"), currentDepth, true);
          size = add(size, child.size);
          output[name] = {case:selected, value:child.value};
        }
        ancestors.delete(input);
      } else {
        const child = fieldValue(member, input, currentDepth);
        size = add(size, child.size);
        output[name] = child.value;
      }
    }
    ancestors.delete(item);
    return {input:output, size};
  }

  // Protobuf-ES represents Struct fields as JsonObject. Measure that wire shape
  // directly, without allocating the runtime's expanded Struct/Value graph.
  function jsonStruct(item, currentDepth) {
    depth(currentDepth);
    layout(2); // Struct object and its fields map.
    object(item);
    const entries = own(item);
    if (entries.size > Math.floor(remaining / 3)) refuse("ResourceExhausted");
    const output = Object.create(null);
    let size = 0;
    for (const [key, input] of entries) {
      depth(currentDepth + 1);
      layout(3); // Synthetic entry object/key/value fields.
      charge(3);
      const keyValue = scalar(Scalar.STRING, key, maxMessageBytes);
      const child = jsonValue(input, currentDepth + 2);
      const entrySize = add(add(tag(1, 2), keyValue.size), add(tag(2, 2), delimited(child.size)));
      size = add(size, add(tag(1, 2), delimited(entrySize)));
      output[key] = child.input;
    }
    ancestors.delete(item);
    return {input:output, size};
  }

  function jsonValue(item, currentDepth) {
    depth(currentDepth);
    layout(7); // Value object and six declared oneof alternatives.
    charge(); // Value's selected oneof field.
    if (item === null) return {input:null, size:2};
    if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") {
      const type = typeof item === "string" ? Scalar.STRING
        : typeof item === "number" ? Scalar.DOUBLE
        : Scalar.BOOL;
      const child = scalar(type, item, maxMessageBytes);
      return {input:child.value, size:add(1, child.size)};
    }

    let child;
    if (Array.isArray(item)) {
      depth(currentDepth + 1); // ListValue message.
      layout(2);
      object(item, true);
      if (item.length > remaining) refuse("ResourceExhausted");
      const entries = own(item);
      if (entries.size !== item.length + 1 || !entries.has("length")) refuse();
      child = {input:[], size:0};
      for (let index = 0; index < item.length; index++) {
        if (!entries.has(String(index))) refuse();
        charge(); // Repeated Value message occurrence.
        const value = jsonValue(entries.get(String(index)), currentDepth + 2);
        child.size = add(child.size, add(1, delimited(value.size)));
        child.input.push(value.input);
      }
      ancestors.delete(item);
    } else {
      child = jsonStruct(item, currentDepth + 1);
    }
    return {input:child.input, size:add(1, delimited(child.size))};
  }

  return message(schema, input, 0);
}
