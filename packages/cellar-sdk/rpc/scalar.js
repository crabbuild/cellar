// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Standard FieldDescriptorProto type numbers, shared by the pinned codec's
// reflection descriptors. The SDK deliberately has no Protobuf runtime import.
export const Scalar = Object.freeze({
  DOUBLE: 1, FLOAT: 2, INT64: 3, UINT64: 4, INT32: 5, FIXED64: 6,
  FIXED32: 7, BOOL: 8, STRING: 9, BYTES: 12, UINT32: 13,
  SFIXED32: 15, SFIXED64: 16, SINT32: 17, SINT64: 18,
});

const longTypes = new Set([
  Scalar.INT64, Scalar.UINT64, Scalar.SINT64, Scalar.FIXED64, Scalar.SFIXED64,
]);
const unsignedTypes = new Set([
  Scalar.UINT32, Scalar.FIXED32, Scalar.UINT64, Scalar.FIXED64,
]);
const integer32Types = new Set([
  Scalar.INT32, Scalar.UINT32, Scalar.SINT32, Scalar.FIXED32, Scalar.SFIXED32,
]);

export function refuse(code = "InvalidArgument") {
  const error = new Error(code);
  error.code = code;
  error.outcome = "not_committed";
  throw error;
}

export function varintSize(value) {
  let size = 1;
  while (value > 127n) {
    value >>= 7n;
    size++;
  }
  return size;
}

/** Count UTF-8 without allocating an encoded string or replacing surrogates. */
export function utf8Size(value, maximum) {
  if (typeof value !== "string") refuse();
  if (value.length > maximum) refuse("ResourceExhausted");
  let size = 0;
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(++index);
      if (!(low >= 0xdc00 && low <= 0xdfff)) refuse();
      size += 4;
    } else {
      if (unit >= 0xdc00 && unit <= 0xdfff) refuse();
      size += unit < 0x80 ? 1 : unit < 0x800 ? 2 : 3;
    }
    if (size > maximum) refuse("ResourceExhausted");
  }
  return size;
}

// Capture typed-array accessors once. An own byteLength/buffer property on an
// input view must not change the measured bytes or reach the codec's writer.
const typedArray = Object.getPrototypeOf(Uint8Array.prototype);
const viewBuffer = Object.getOwnPropertyDescriptor(typedArray, "buffer").get;
const viewLength = Object.getOwnPropertyDescriptor(typedArray, "byteLength").get;
const viewOffset = Object.getOwnPropertyDescriptor(typedArray, "byteOffset").get;
const resizable = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get;

function bytesView(value, maximum) {
  if (!(value instanceof Uint8Array)) refuse();
  const buffer = viewBuffer.call(value);
  const length = viewLength.call(value);
  if (!(buffer instanceof ArrayBuffer) || resizable?.call(buffer)) refuse();
  if (length > maximum) refuse("ResourceExhausted");
  // Constructing a fresh view also rejects a detached backing store. It owns
  // the view properties without copying payload bytes before sizing succeeds.
  return new Uint8Array(buffer, viewOffset.call(value), length);
}

/** Validate the exact portable scalar type and return its bare wire size. */
export function scalar(type, value, maximum) {
  if (type === Scalar.STRING) {
    const length = utf8Size(value, maximum);
    return {value, size:varintSize(BigInt(length)) + length, wire:2, zero:length === 0};
  }
  if (type === Scalar.BYTES) {
    value = bytesView(value, maximum);
    const length = value.byteLength;
    return {value, size:varintSize(BigInt(length)) + length, wire:2, zero:length === 0};
  }
  if (type === Scalar.BOOL) {
    if (typeof value !== "boolean") refuse();
    return {value, size:1, wire:0, zero:value === false};
  }
  if (type === Scalar.DOUBLE || type === Scalar.FLOAT) {
    if (typeof value !== "number") refuse();
    if (type === Scalar.FLOAT && Number.isFinite(value) && Math.abs(value) > 3.4028234663852886e38) refuse();
    return {value, size:type === Scalar.FLOAT ? 4 : 8, wire:type === Scalar.FLOAT ? 5 : 1, zero:Object.is(value, 0)};
  }

  const long = longTypes.has(type);
  const unsigned = unsignedTypes.has(type);
  const integer32 = integer32Types.has(type);
  if (!long && !integer32) refuse();
  if (long ? typeof value !== "bigint" : typeof value !== "number" || !Number.isInteger(value)) refuse();
  const minimum = unsigned ? 0n : -(1n << BigInt(long ? 63 : 31));
  const maximumValue = unsigned ? (1n << BigInt(long ? 64 : 32)) - 1n : -minimum - 1n;
  const number = BigInt(value);
  if (number < minimum || number > maximumValue) refuse();
  if (type === Scalar.FIXED32 || type === Scalar.SFIXED32) return {value, size:4, wire:5, zero:number === 0n};
  if (type === Scalar.FIXED64 || type === Scalar.SFIXED64) return {value, size:8, wire:1, zero:number === 0n};
  const encoded = type === Scalar.SINT32 || type === Scalar.SINT64
    ? (number << 1n) ^ (number >> BigInt(long ? 63 : 31))
    : BigInt.asUintN(64, number);
  return {value, size:varintSize(encoded), wire:0, zero:number === 0n};
}

/** Map keys are object property strings; only canonical scalar spellings pass. */
export function mapKey(type, key, maximum) {
  if (type === Scalar.STRING) return scalar(type, key, maximum);
  if (type === Scalar.BOOL) {
    if (key !== "true" && key !== "false") refuse();
    return scalar(type, key === "true", maximum);
  }
  const long = longTypes.has(type);
  // Bound even the spelling check before BigInt can allocate a large integer.
  if (key.length > (long ? 20 : 11)) refuse();
  if (!/^-?(0|[1-9][0-9]*)$/.test(key)) refuse();
  const value = long ? BigInt(key) : Number(key);
  if (String(value) !== key) refuse();
  return scalar(type, value, maximum);
}
