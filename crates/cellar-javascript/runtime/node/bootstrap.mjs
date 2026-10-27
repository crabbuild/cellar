// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Keep exec arguments small on every Unix host. Rust sends the length-prefixed
// compiled source first, followed by the private configuration document. Reading
// exactly the source leaves configuration on stdin for the imported worker; its
// authentication secret never becomes part of a module URL, argv or environment.
import { readSync } from "node:fs";

function readExactly(length) {
  const bytes = Buffer.allocUnsafe(length);
  let offset = 0;
  while (offset < length) {
    const count = readSync(0, bytes, offset, length - offset);
    if (count === 0) throw new Error("Cellar Node bootstrap pipe closed");
    offset += count;
  }
  return bytes;
}

// This private pipe belongs to the Rust parent. The source is compiled into
// that executable, rather than supplied by an application or network peer.
const source = readExactly(readExactly(4).readUInt32BE(0));
await import(`data:text/javascript;base64,${source.toString("base64")}`);
