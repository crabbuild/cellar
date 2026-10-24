import { verifyCodecs } from "./shape.js";

/** Internal authority key installed by both Cellar JavaScript backends. */
const transportKey = Symbol.for("cellar.rpc");
const methods = new WeakSet();

function failure(code, outcome = "not_committed") {
  const error = new Error(code);
  error.code = code;
  error.outcome = outcome;
  return error;
}

function check(transport) {
  try {
    transport.check();
  } catch {
    throw failure("ContextExpired");
  }
}

function aborted(signal, outcome = "not_committed") {
  if (signal === undefined) return false;
  try {
    if (signal === null || typeof signal !== "object") throw new TypeError();
    const state = signal.aborted;
    if (typeof state !== "boolean" ||
        typeof signal.addEventListener !== "function" ||
        typeof signal.removeEventListener !== "function") {
      throw new TypeError("Invalid cancellation signal");
    }
    return state;
  } catch {
    throw failure("InvalidArgument", outcome);
  }
}

/**
 * Bind schema selection and wire identity once in generated service code.
 * Native code independently checks the descriptor digest and actual wire bytes.
 */
export function createMethod({ method, digest, handler, inputSchema, outputSchema }, codec) {
  if (typeof method !== "string" || method.length > 256 || !method.includes("/") ||
      !/^[a-f0-9]{64}$/.test(digest) || typeof handler !== "string" ||
      ["constructor", "__proto__", "then"].includes(handler) ||
      typeof inputSchema?.typeName !== "string" || typeof outputSchema?.typeName !== "string") {
    throw new TypeError("Invalid Cellar RPC method registration");
  }
  const rule = Object.freeze({
    method, digest, handler,
    input: inputSchema.typeName,
    output: outputSchema.typeName,
    verify(contract) {
      if (contract?.digest !== digest || !contract.methods.some(entry =>
        entry.name === method && entry.input === inputSchema.typeName && entry.output === outputSchema.typeName)) {
        throw failure("ContractMismatch");
      }
      try {
        verifyCodecs(inputSchema, outputSchema, contract.types);
      } catch {
        throw failure("ContractMismatch");
      }
    },
    encodeRequest: (request, limits) => codec.encodeMessage(inputSchema, request, limits),
    decodeResponse: (bytes, limits) => codec.decodeMessage(outputSchema, bytes, limits),
    decode: (bytes, limits) => codec.decodeMessage(inputSchema, bytes, limits),
    encode(response, limits) {
      if (response?.$typeName !== outputSchema.typeName) {
        throw failure("InvalidResponse", "unknown");
      }
      try {
        return codec.encodeMessage(outputSchema, response, limits);
      } catch {
        throw failure("InvalidResponse", "unknown");
      }
    },
  });
  methods.add(rule);
  return rule;
}

/** Explicit server table; business helpers never become remotely callable. */
export function createRegistration(rules) {
  const table = Object.create(null);
  for (const rule of rules) {
    if (!methods.has(rule) || Object.hasOwn(table, rule.method)) {
      throw new TypeError("Invalid or duplicate Cellar RPC registration");
    }
    table[rule.method] = rule;
  }
  if (Object.keys(table).length === 0) throw new TypeError("Empty Cellar RPC registration");
  return Object.freeze({ methods: Object.freeze(table) });
}

/** Transport base used by generated clients, bound to one native invocation. */
export class RpcClient {
  #transport;
  #binding;
  #name;

  constructor(context, binding, name) {
    const transport = context?.[transportKey];
    if (typeof transport?.check !== "function" || typeof transport?.call !== "function" || typeof transport?.verify !== "function" ||
        typeof binding !== "string" || binding.length === 0 || typeof name !== "string") {
      throw failure("InvalidArgument");
    }
    check(transport);
    this.#transport = transport;
    this.#binding = binding;
    this.#name = name;
  }

  async call(method, request, options = {}) {
    check(this.#transport);
    if (!methods.has(method) || options === null || typeof options !== "object") {
      throw failure("InvalidArgument");
    }
    let timeoutMs, signal;
    try {
      if (Object.keys(options).some(key => key !== "timeoutMs" && key !== "signal")) throw new TypeError();
      ({timeoutMs, signal} = options);
      if (timeoutMs !== undefined && (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 0xffffffff)) {
        throw new TypeError();
      }
    } catch {
      throw failure("InvalidArgument");
    }
    if (aborted(signal)) throw failure("Cancelled");
    const limits = this.#transport.verify(method);
    const body = method.encodeRequest(request, limits);
    if (aborted(signal)) throw failure("Cancelled");
    const metadata = {
      binding: this.#binding, name: this.#name,
      method: method.method, contract_digest: method.digest,
    };
    if (timeoutMs !== undefined) metadata.timeout_ms = timeoutMs;
    const reply = await this.#transport.call(metadata, body, signal);
    if (aborted(signal, "unknown")) throw failure("Cancelled", "unknown");
    const result = method.decodeResponse(reply, limits);
    if (aborted(signal, "unknown")) throw failure("Cancelled", "unknown");
    return result;
  }
}
