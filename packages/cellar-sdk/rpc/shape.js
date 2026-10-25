// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Codec identity is a structural projection of live reflection descriptors. It
// is not the artifact digest: runtime descriptors need not serialize identically
// to protoc's image. The Rust host supplies the expected projection privately.
const verified = new WeakMap();

function mismatch() {
  throw new TypeError("Cellar RPC codec differs from its contract");
}

/** Check both reachable codec graphs and freeze records used by the codec. */
export function verifyCodecs(input, output, expected) {
  if (!expected || typeof expected !== "object") mismatch();
  let cached = verified.get(expected);
  if (!cached) {
    cached = new WeakSet();
    verified.set(expected, cached);
  }
  const pending = [input, output];
  const seen = new Set();
  const lock = [];
  let fieldsLeft = 65_536;

  function reference(field, kind) {
    if (kind !== "message" && kind !== "enum") return "";
    const child = kind === "message" ? field.message : field.enum;
    if (!child || child.kind !== kind) mismatch();
    pending.push(child);
    return child.typeName;
  }

  while (pending.length) {
    const desc = pending.pop();
    if (!desc || typeof desc !== "object") mismatch();
    if (cached.has(desc) || seen.has(desc)) continue;
    seen.add(desc);
    if (seen.size > 4096 || desc.file?.edition !== 999) mismatch();
    const wanted = expected[desc.typeName];
    if (!Array.isArray(wanted) || wanted[0] !== desc.kind) mismatch();
    let rows;

    if (desc.kind === "enum") {
      if (desc.open !== true || !Array.isArray(desc.values) || desc.values.length !== wanted[1].length) mismatch();
      rows = desc.values.map(value => [value.name, value.number]);
      lock.push(...desc.values, desc.values, desc.value);
    } else if (desc.kind === "message") {
      if (!Array.isArray(desc.fields) || desc.fields.length !== wanted[1].length) mismatch();
      fieldsLeft -= desc.fields.length;
      if (fieldsLeft < 0) mismatch();
      const members = new Set();
      const fieldSet = new Set(desc.fields);
      const groups = new Map();
      rows = desc.fields.map(field => {
        if (field.parent !== desc || field.longAsString || field.delimitedEncoding ||
            Object.hasOwn(field.proto, "defaultValue") || (field.proto?.options?.jstype ?? 0) !== 0) mismatch();
        const cardinality = field.fieldKind === "map" ? 2 : field.fieldKind === "list" ? 1 : 0;
        const kind = cardinality === 2 ? field.mapKind : cardinality === 1 ? field.listKind : field.fieldKind;
        if (!["scalar", "enum", "message"].includes(kind)) mismatch();
        if ((field.scalar === 9 || field.mapKey === 9) && field.utf8Validation !== true) mismatch();
        const number = kind === "scalar" ? field.scalar : kind === "enum" ? 14 : 11;
        const group = field.oneof;
        members.add(group ?? field);
        if (group) {
          if (!groups.has(group)) {
            if (group.parent !== desc || !Array.isArray(group.fields) || group.fields.length > desc.fields.length) mismatch();
            const entries = new Set(group.fields);
            if (entries.size !== group.fields.length || group.fields.some(field => !fieldSet.has(field) || field.oneof !== group)) mismatch();
            groups.set(group, entries);
          }
          if (!groups.get(group).has(field)) mismatch();
          lock.push(group, group.fields);
        }
        lock.push(field.proto, field.proto?.options, field);
        return [field.number, field.name, number, cardinality,
          cardinality === 0 && field.presence === 1, group?.name ?? "",
          cardinality === 1 && field.packed === true, reference(field, kind),
          cardinality === 2 ? field.mapKey : 0];
      }).sort((left, right) => left[0] - right[0]);
      // create() walks members, while binary codecs walk fields. Both must
      // describe the same records; a matching fields array alone is insufficient.
      if (!Array.isArray(desc.members) || desc.members.length !== members.size ||
          desc.members.some(member => !members.delete(member))) mismatch();
      lock.push(desc.fields, desc.members);
    } else {
      mismatch();
    }
    if (JSON.stringify(rows) !== JSON.stringify(wanted[1])) mismatch();
    lock.push(desc.file, desc);
  }

  // Freeze only reflection records used for message construction/encoding.
  // The codec's compiled readers/writers are held in its own WeakMap caches.
  for (const value of lock) if (value && typeof value === "object") Object.freeze(value);
  for (const desc of seen) cached.add(desc);
}
