// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Native, bounded text codecs; installed before evaluating application modules.
(encode, createDecoder) => {
  const Bytes = Uint8Array, Buffer = ArrayBuffer, string = String;
  const view = Buffer.isView, define = Object.defineProperty;

  function input(value) {
    if (value === undefined) return new Bytes();
    if (view(value)) return new Bytes(value.buffer, value.byteOffset, value.byteLength);
    if (value instanceof Buffer) return new Bytes(value);
    throw new TypeError("Text input must be a byte buffer");
  }

  class TextEncoder {
    get encoding() { return "utf-8"; }

    encode(source = "") {
      if (typeof source === "symbol") throw new TypeError("Invalid text input");
      return encode(string(source))[0];
    }

    encodeInto(source, destination) {
      if (typeof source === "symbol" || !(destination instanceof Bytes)) {
        throw new TypeError("Invalid text destination");
      }
      const [bytes, read] = encode(string(source), destination.byteLength);
      destination.set(bytes);
      return { read, written: bytes.byteLength };
    }
  }

  class TextDecoder {
    #decode;
    #encoding;
    #fatal;
    #ignoreBOM;

    constructor(label = "utf-8", options = {}) {
      if (typeof label === "symbol") throw new RangeError("Invalid encoding label");
      this.#fatal = Boolean(options?.fatal);
      this.#ignoreBOM = Boolean(options?.ignoreBOM);
      [this.#encoding, this.#decode] = createDecoder(string(label), this.#fatal, this.#ignoreBOM);
    }

    get encoding() { return this.#encoding; }
    get fatal() { return this.#fatal; }
    get ignoreBOM() { return this.#ignoreBOM; }

    decode(value = undefined, options = {}) {
      return this.#decode(input(value), Boolean(options?.stream));
    }
  }

  define(TextEncoder.prototype, Symbol.toStringTag, { value: "TextEncoder", configurable: true });
  define(TextDecoder.prototype, Symbol.toStringTag, { value: "TextDecoder", configurable: true });
  define(globalThis, "TextEncoder", { value: TextEncoder, writable: true, configurable: true });
  define(globalThis, "TextDecoder", { value: TextDecoder, writable: true, configurable: true });
}
