// Real-host fault probes. Production call budgets never use this wall clock.
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { jsonResponse, type Context } from "@cellar/sdk";
import { createCodec, createMethod, RpcClient, type RpcMethod, type InputLimits } from "@cellar/sdk/rpc";
import { CounterRegistration } from "./fixtures/generated/typescript/counter_cellar.js";
import { IncrementRequestSchema, IncrementResponseSchema } from "./fixtures/generated/typescript/counter_pb.js";

interface Budget { remaining(): number; limit(ms: number): void; release(): void }
interface Transport {
  check(): void;
  budget(): Budget | Promise<Budget>;
  verify(method: RpcMethod): InputLimits;
  call(metadata: object, body: Uint8Array, signal?: undefined, budget?: Budget): Promise<Uint8Array>;
}
const key = Symbol.for("cellar.rpc");
const transport = (ctx: Context) => Reflect.get(ctx, key) as Transport;
const codec = createCodec({create, fromBinary, toBinary, configureTextEncoding});
const registered = CounterRegistration.methods["example.counter.v1.Counter/Increment"];
const now = Date.now;
function pause(ms: number) { const until = now() + ms; while (now() < until) {} }

export async function codecBudget(ctx: Context, binding: string, phase: string, id: string) {
  if (!["verify", "encode", "decode"].includes(phase)) throw Error("Unknown codec probe");
  const real = transport(ctx);
  const wrapper = Object.create(ctx) as Context;
  let entered = 0;
  Object.defineProperty(wrapper, key, {value:{...real, verify(method: RpcMethod) {
    if (phase === "verify") { entered++; pause(40); }
    return real.verify(method);
  }}});
  const method = createMethod({method:registered.method, digest:registered.digest, handler:"increment",
    inputSchema:IncrementRequestSchema, outputSchema:IncrementResponseSchema}, {
    encodeMessage(schema, value, limits) {
      if (phase === "encode") { entered++; pause(40); }
      return codec.encodeMessage(schema, value, limits);
    },
    decodeMessage(schema, value, limits) {
      if (phase === "decode") { entered++; pause(120); }
      return codec.decodeMessage(schema, value, limits);
    },
  });
  try {
    await new RpcClient(wrapper, binding, "codec-budget-" + phase).call(method, {delta:1n, operationId:id}, {timeoutMs:phase === "decode" ? 100 : 20});
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || !("outcome" in error) || entered !== 1) throw error;
    return {code:String(error.code), outcome:String(error.outcome), codec_entered:phase};
  }
  throw Error("Slow codec escaped its original budget");
}

export async function parentBudget(ctx: Context): Promise<bigint> {
  const budget = await transport(ctx).budget();
  try {
    budget.limit(60000);
    const remaining = budget.remaining();
    if (!(remaining > 0 && remaining <= 1000)) throw Error("Guest extended native parent budget");
    return BigInt(remaining);
  } finally { budget.release(); }
}

let held: {budget:Budget; finish:() => void} | undefined;
export async function budgetControl(path: string, ctx: Context) {
  if (path.endsWith("/budget-capacity")) {
    const budget = await transport(ctx).budget();
    try {
      try { await transport(ctx).budget(); throw Error("Missing budget bound"); }
      catch (error) { if (!(error instanceof Error) || error.message !== "ResourceExhausted") throw error; }
      await ctx.variable("DOMAIN_MARKER"); // Budget needs no second SDK slot.
    } finally { budget.release(); }
    const fresh = await transport(ctx).budget();
    fresh.release();
    return jsonResponse({bounded:true, slot_reused:true});
  }
  if (path.endsWith("/budget-held")) return jsonResponse({held:held !== undefined});
  if (path.endsWith("/budget-free")) {
    if (!held) throw Error("No held budget");
    held.budget.release(); // Cleanup is safe from a different continuation.
    held.finish();
    return jsonResponse({released:true});
  }
  if (path.endsWith("/budget-foreign")) {
    if (!held) throw Error("No held budget");
    try { await transport(ctx).call({binding:"RUST", name:"unconstructed-budget", method:registered.method,
      contract_digest:registered.digest}, new Uint8Array(), undefined, held.budget); }
    catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ContextMismatch" && "outcome" in error && error.outcome === "not_committed") {
        return jsonResponse({scope_refused:true});
      }
      throw error;
    }
    throw Error("Foreign budget acquired native authority");
  }
  if (!path.endsWith("/budget-hold") || held) throw Error("Invalid budget control");
  const budget = await transport(ctx).budget();
  let finish!: () => void;
  const finished = new Promise<void>(resolve => { finish = resolve; });
  held = {budget, finish};
  try { await finished; return jsonResponse({joined:true}); }
  finally { budget.release(); held = undefined; }
}
