// Real-host fault probes. Production call budgets never use this wall clock.
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { jsonResponse, type Context, type Json } from "@cellar/sdk";
import { createCodec, createMethod, RpcClient, type RpcMethod, type InputLimits } from "@cellar/sdk/rpc";
import { CounterRegistration, type CounterClient } from "./fixtures/generated/typescript/counter_cellar.js";
import { GetCountResponseSchema, IncrementRequestSchema, IncrementResponseSchema } from "./fixtures/generated/typescript/counter_pb.js";

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

export async function codecBudget(ctx: Context, binding: string, phase: string, id: string, remote = false) {
  if (!["verify", "encode", "decode", "control"].includes(phase)) throw Error("Unknown codec probe");
  const preflightDelay = remote ? 120 : 40;
  const decodeTimeout = remote ? 400 : 100;
  const real = transport(ctx);
  const wrapper = Object.create(ctx) as Context;
  let entered = 0;
  Object.defineProperty(wrapper, key, {value:{...real, verify(method: RpcMethod) {
    if (phase === "control") throw Error("Expired control reached codec verification");
    if (phase === "verify") { entered++; pause(preflightDelay); }
    return real.verify(method);
  }}});
  const method = createMethod({method:registered.method, digest:registered.digest, handler:"increment",
    inputSchema:IncrementRequestSchema, outputSchema:IncrementResponseSchema}, {
    encodeMessage(schema, value, limits) {
      if (phase === "verify" || phase === "control") throw Error("Expired verification reached encoding");
      if (phase === "encode") { entered++; pause(preflightDelay); }
      return codec.encodeMessage(schema, value, limits);
    },
    decodeMessage(schema, value, limits) {
      if (phase === "decode") { entered++; pause(remote ? 600 : 120); }
      return codec.decodeMessage(schema, value, limits);
    },
  });
  try {
    // Node sends budget_start before this getter blocks its event loop. The
    // real acknowledgement remains queued until the getter returns.
    const options = phase === "control" ? {get timeoutMs() { entered++; pause(preflightDelay); return 20; }}
      : {timeoutMs:phase === "decode" ? decodeTimeout : 20};
    await new RpcClient(wrapper, binding, "codec-budget-" + phase).call(method, {delta:1n, operationId:id}, options);
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || !("outcome" in error) || entered !== 1) throw error;
    return {code:String(error.code), outcome:String(error.outcome), codec_entered:phase};
  }
  throw Error("Slow codec escaped its original budget");
}

type ClientFactory = {bind(ctx: Context, binding: string, name: string): Pick<CounterClient, "getCount" | "increment">};
let clients: ClientFactory;
let nested: {armed:boolean; child:string; observation:Record<string, Json>} = {armed:false, child:"", observation:{}};

export function configureDeadlineClient(factory: ClientFactory) { clients = factory; }

export function deadlineControl(action: string, child: string) {
  if (action === "arm") {
    if (nested.armed) throw Error("Nested deadline probe already armed");
    nested = {armed:true, child, observation:{late_done:false}};
  } else if (action === "disarm") {
    if (!nested.observation.late_done) throw Error("Nested deadline probe has not settled");
    nested.armed = false;
  } else if (action !== "status") throw Error("Unknown deadline control");
  return jsonResponse({...nested.observation});
}

async function expired(operation: () => unknown) {
  try { await operation(); }
  catch (error) {
    return error instanceof Error && "code" in error && error.code === "ContextExpired"
      && "outcome" in error && error.outcome === "not_committed";
  }
  return false;
}

/** Invoked as a real typed provider, under the original incoming parent cap. */
export async function nestedDeadline(ctx: Context) {
  if (!nested.armed) {
    const late = await ctx.storage.read("deadline-parent-late");
    return create(GetCountResponseSchema, {count:{value:late.present ? 1n : 0n}});
  }
  const current = nested.observation;
  const budget = await transport(ctx).budget();
  try { current.remaining_ms = budget.remaining(); }
  finally { budget.release(); }
  const real = transport(ctx), wrapper = Object.create(ctx) as Context;
  Object.defineProperty(wrapper, key, {value:{...real, verify(method: RpcMethod) {
    current.verify_entered = true;
    pause(120);
    return real.verify(method);
  }}});
  const client = clients.bind(wrapper, "RUST", nested.child);
  // Cache a client with the original facade too: both retained authority and
  // fresh admission must be refused after the parent invocation expires.
  const cached = clients.bind(ctx, "RUST", "guest-followup");
  try {
    return await client.getCount({}, {timeoutMs:60000});
  } catch (error) {
    current.storage_refused = await expired(() => ctx.storage.apply({puts:{"deadline-parent-late":true}}));
    current.cached_call_refused = await expired(() => cached.getCount({}));
    current.new_call_refused = await expired(() => clients.bind(ctx, "RUST", "guest-followup").getCount({}));
    current.late_done = true;
    throw error;
  }
}

export async function deadlineCall(ctx: Context, binding: string, name: string, timeout?: string) {
  const node = await ctx.variable("DOMAIN_MARKER");
  const client = clients.bind(ctx, binding, name);
  let result: Json;
  try {
    const reply = await client.getCount({}, {timeoutMs:timeout === undefined ? undefined : Number(timeout)});
    result = {writes:Number(reply.count!.value)};
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || !("outcome" in error)) throw error;
    result = {code:String(error.code), outcome:String(error.outcome)};
  }
  // Plain reads seed ownership. Only a deliberately timed call audits release
  // of its mutation permit; warmup must not consume that bounded probe's log.
  const followup = timeout === undefined ? undefined
    : await clients.bind(ctx, "RUST", "guest-followup").getCount({});
  return jsonResponse({node, result, followup_writes:followup ? Number(followup.count!.value) : null});
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
