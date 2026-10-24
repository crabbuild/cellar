import { prepareMessage } from "./message.js";
import { inputLimits } from "./input.js";

/**
 * Generated service packages supply their pinned Protobuf runtime. Keeping that
 * runtime explicit also configures the exact codec copy used by the service.
 */
export function createCodec(runtime) {
  const { create, fromBinary, toBinary, configureTextEncoding } = runtime;
  let configured = false;

  function configureEncoding() {
    if (configured) return;

    const encoder = new TextEncoder();
    // A BOM is application string data in Protobuf. The WHATWG default would
    // remove it, so configure the SDK's codec instance to retain it explicitly.
    const decoder = new TextDecoder("utf-8", { ignoreBOM: true });
    const strictDecoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

    configureTextEncoding({
      encodeUtf8: value => encoder.encode(value),
      decodeUtf8: (bytes, strict) => (strict ? strictDecoder : decoder).decode(bytes),
      checkUtf8(value) {
        try {
          encodeURIComponent(value);
          return true;
        } catch {
          return false;
        }
      },
    });
    configured = true;
  }

  /** Encode a caller-owned initializer before submitting the target invocation. */
  function encodeMessage(schema, input, limits = {}) {
    try {
      const prepared = prepareMessage(schema, input, limits);
      configureEncoding();
      const bytes = toBinary(schema, create(schema, prepared.input), {writeUnknownFields:false});
      if (!(bytes instanceof Uint8Array) || bytes.byteLength !== prepared.size) throw new Error("Codec size mismatch");
      return bytes;
    } catch (error) {
      const code = error instanceof Error && error.code === "ResourceExhausted"
        ? "ResourceExhausted"
        : "InvalidArgument";
      throw failure(code, "not_committed");
    }
  }

  /**
   * Decode bytes after the Rust host's descriptor-driven wire preflight. The host
   * checks field occurrences and nesting before a guest codec can allocate them.
   * This internal helper does not replace that native admission boundary.
   */
  function decodeMessage(schema, bytes, limits = {}) {
    try {
      const {maxMessageBytes} = inputLimits(limits);
      if (!(bytes instanceof Uint8Array) || bytes.byteLength > maxMessageBytes) {
        throw new Error("InvalidResponse");
      }
      configureEncoding();
      const message = fromBinary(schema, bytes, {readUnknownFields:false});
      // Incoming resource checks describe the bytes Native already validated.
      // Re-encoding this value can have a different cost: Prost omits default
      // map fields that Protobuf-ES writes explicitly. Applying the encoder's
      // occurrence/depth count here would reject valid cross-language replies.
      if (message.$typeName !== schema.typeName) throw new Error("InvalidResponse");
      return message;
    } catch {
      // Reply decoding happens after remote business code may have committed.
      throw failure("InvalidResponse", "unknown");
    }
  }

  return Object.freeze({ encodeMessage, decodeMessage });
}

function failure(code, outcome) {
  const error = new Error(code);
  error.code = code;
  error.outcome = outcome;
  return error;
}
