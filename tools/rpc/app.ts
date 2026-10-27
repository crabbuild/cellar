// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Shared business implementation. V8 consumes TypeScript-generated services;
// the Node entry uses the separately generated JavaScript services.
import { create } from "@bufbuild/protobuf";
import { jsonResponse, utf8, type Context, type Json, type Request, type Snapshot } from "@cellar/sdk";
import { CounterClient, CounterRegistration, type CounterHandler } from "./fixtures/generated/typescript/counter_cellar.js";
import {
  GetCountResponseSchema, IncrementResponseSchema,
  type GetCountRequest, type GetCountResponse, type IncrementRequest, type IncrementResponse,
} from "./fixtures/generated/typescript/counter_pb.js";
import { configureLiveClient, liveControl, liveSnapshot, liveSync } from "./live.js";
import type { RpcAbortSignal } from "@cellar/sdk/rpc";
import { budgetControl, codecBudget, parentBudget, configureDeadlineClient, deadlineCall, deadlineControl, nestedDeadline } from "./deadline.js";
import { configureCycles, cycleAfterCommit, cycleControl } from "./cycles.js";
import { configureProvenance, provenanceAfterCommit, provenanceControl } from "./provenance.js";

let constructed = 0;
const maximum = (1n << 63n) - 1n;

function integer(value: Json): bigint {
  if (typeof value !== "string" || !/^-?(0|[1-9][0-9]*)$/.test(value)) throw Error("Invalid durable count");
  const result = BigInt(value);
  if (result < -(1n << 63n) || result > maximum || result.toString() !== value) throw Error("Invalid durable count");
  return result;
}

function receipt(snapshot: Snapshot): {delta: string; value: bigint} {
  const row = snapshot.value;
  if (!row || typeof row !== "object" || Array.isArray(row) || typeof row.delta !== "string") throw Error("Invalid durable receipt");
  return {delta: row.delta, value:integer(row.value)};
}

const count = (value: bigint) => create(IncrementResponseSchema, {result:{case:"count", value:{value}}});
const reject = (reason: string) => create(IncrementResponseSchema, {result:{case:"outOfRange", value:{reason}}});

export class Counter implements CounterHandler {
  constructor() { constructed++; }

  async getCount(_request: GetCountRequest, ctx: Context): Promise<GetCountResponse> {
    if (ctx.identity?.name === "codec-parent") return create(GetCountResponseSchema, {count:{value:await parentBudget(ctx)}});
    if (ctx.identity?.name === "deadline-nested") return nestedDeadline(ctx);
    const current = await ctx.storage.read("count");
    return create(GetCountResponseSchema, {count:{value:current.present ? integer(current.value) : 0n}});
  }

  async increment(request: IncrementRequest, ctx: Context): Promise<IncrementResponse> {
    if (request.delta === undefined || request.delta <= 0n) return reject("positive delta is required");
    if (!request.operationId || utf8(request.operationId).byteLength > 128) return reject("operation ID is required");
    const key = "receipt:" + request.operationId;
    for (let attempt = 0; attempt < 16; attempt++) {
      const current = await ctx.storage.read("count");
      const saved = await ctx.storage.read(key);
      if (saved.present) {
        const previous = receipt(saved);
        return previous.delta === request.delta.toString() ? count(previous.value)
          : create(IncrementResponseSchema, {result:{case:"operationConflict", value:{}}});
      }
      const value = (current.present ? integer(current.value) : 0n) + request.delta;
      if (value > maximum) return reject("counter overflow");
      try {
        await ctx.storage.apply({checks:{count:current, [key]:saved}, puts:{
          count:value.toString(), [key]:{delta:request.delta.toString(), value:value.toString()},
        }});
      } catch (error) {
        if (error instanceof Error && "code" in error && error.code === "Conflict") continue;
        throw error;
      }
      // Qualification-only commands return from the handler before proof, so
      // the native output gate itself must withhold a successful RPC response.
      await liveSync(ctx,request.operationId);
      if (!request.operationId.startsWith("proof-no-sync-")) await ctx.storage.sync();
      await cycleAfterCommit(ctx, request.operationId);
      await provenanceAfterCommit(ctx, request.operationId);
      if (request.operationId.startsWith("held-reply-quota-")) {
        // Valid output after the receipt commits; only the host reply quota
        // should refuse it. A same-ID retry returns the saved count above.
        return reject("x".repeat(64 * 1024));
      }
      if (request.operationId === "invalid-reply") {
        // Deliberate post-commit bug. The caller must receive unknown effects;
        // retrying this ID after restart must return the durable saved result.
        return create(GetCountResponseSchema, {count:{value}}) as unknown as IncrementResponse;
      }
      return count(value);
    }
    throw Error("Counter contention exceeded its retry budget");
  }

  async privateHelper() { throw Error("Unregistered helper was invoked"); }
}

type Calls = Pick<CounterClient, "increment" | "getCount">;
type Client = {bind(context: Context, binding: string, name: string): Calls};
type Controller = {signal: RpcAbortSignal; abort(): void};

/** HTTP is only the test ingress; entity behavior uses generated named methods. */
export function handler(Client: Client, createController?: () => Controller) {
  configureLiveClient(ctx => Client.bind(ctx,"RUST","alice"));
  configureDeadlineClient(Client);
  configureCycles(Client);
  configureProvenance(Client);
  let retained: Calls | undefined;
  const cancelling = new Map<string, {controller: Controller; context: Context; finish: () => void;
    settled:boolean; result:Json; followup_writes:number}>();
  return async (request: Request, ctx: Context) => {
    const [path, query = ""] = request.url.split("?");
    const params = Object.fromEntries(query.split("&").filter(Boolean).map(pair => {
      const [key, value = ""] = pair.split("=");
      return [decodeURIComponent(key), decodeURIComponent(value)];
    }));
    const binding = params.target === "rust" ? "RUST" : params.target === "node" ? "NODE"
      : params.target === "python" ? "PYTHON" : "V8";
    if (path.endsWith("/metadata-control")) {
      try {
        if (params.mode === "oversize") {
          await Client.bind(ctx, binding, "x".repeat(32 * 1024)).getCount({});
        } else if (params.mode === "unknown") {
          // Probe the actual native decoder with a valid empty request and an
          // undeclared control graph. Business methods must never receive it.
          const rule = CounterRegistration.methods["example.counter.v1.Counter/GetCount"];
          const transport = Reflect.get(ctx, Symbol.for("cellar.rpc"));
          await transport.call({binding, name:"alice", method:rule.method,
            contract_digest:rule.digest, extra:{items:[1, 2]}}, new Uint8Array());
        } else throw Error("Unknown metadata-control probe");
        return jsonResponse({admitted:true});
      } catch (error) {
        const failure = error as {code?:unknown; outcome?:unknown};
        if (typeof failure?.code !== "string" || typeof failure?.outcome !== "string") throw error;
        return jsonResponse({code:failure.code, outcome:failure.outcome});
      }
    }
    if (/\/budget-(capacity|hold|held|free|foreign)$/.test(path)) return budgetControl(path, ctx);
    if (path.endsWith("/cycle-control")) return jsonResponse(cycleControl(params.action));
    if (path.endsWith("/provenance-control")) return jsonResponse(provenanceControl(params.action === "clear"));
    if (path.endsWith("/cancel-release")) {
      const control = cancelling.get(params.name);
      if (!control) throw Error("No pending cancellation probe");
      if (params.phase === "finish") control.finish();
      else control.controller.abort(); // A different invocation fires the signal.
      return jsonResponse({released:true});
    }
    if (path.endsWith("/cancel-state")) {
      const control = cancelling.get(params.name);
      if (!control) throw Error("No pending cancellation probe");
      return jsonResponse({settled:control.settled, result:control.result, followup_writes:control.followup_writes});
    }
    if (path.endsWith("/cancel-attack")) {
      const control = cancelling.get(params.name);
      if (!control) throw Error("No pending cancellation probe");
      try {
        // Attempt raw RPC admission under the retained original facade. Even
        // private byte transport must check scope before creating a control.
        const transport = Reflect.get(control.context, Symbol.for("cellar.rpc"));
        await Reflect.apply(Reflect.get(transport, "call"), transport,
          [{probe:"foreign-cancellation"}, new Uint8Array()]);
      }
      catch (error) {
        if (error instanceof Error && "code" in error && error.code === "ContextMismatch") {
          return jsonResponse({scope_refused:true});
        }
        throw error;
      }
      throw Error("Foreign invocation acquired original SDK authority");
    }
    if (path.endsWith("/cancel") || path.endsWith("/proof-cancel")) {
      const proof = path.endsWith("/proof-cancel");
      const name = proof ? params.id : params.name;
      const deadline = proof && params.trigger === "deadline";
      if (!createController || cancelling.has(name)) throw Error("Invalid cancellation probe");
      const controller = createController();
      let finish!: () => void;
      const finished = new Promise<void>(resolve => { finish = resolve; });
      const control = {controller, context:ctx, finish, settled:false, result:null as Json, followup_writes:0};
      cancelling.set(name, control);
      try {
        const client = Client.bind(ctx, proof ? binding : "RUST", proof ? "alice" : name);
        let result: Json;
        try {
          const options = {timeoutMs:deadline ? 1000 : 5000, signal:controller.signal};
          await (proof ? client.increment({delta:BigInt(params.delta), operationId:params.id}, options)
            : client.getCount({}, options));
          throw Error("RPC ignored cancellation");
        } catch (error) {
          if (!(error instanceof Error) || !("code" in error) || !("outcome" in error) ||
              error.code !== (deadline ? "DeadlineExceeded" : "Cancelled") || error.outcome !== "unknown") throw error;
          result = {code:String(error.code), outcome:error.outcome};
        }
        // Native acknowledgement must release mutation admission before this
        // fresh RPC starts. Keep the parent live through the target's late work.
        const followup = await Client.bind(ctx, "RUST", "cancel-followup").getCount({});
        control.result = result;
        control.followup_writes = Number(followup.count?.value);
        control.settled = true;
        await finished;
        const finalFollowup = proof ? await Client.bind(ctx, "RUST", "cancel-followup").getCount({}) : followup;
        return jsonResponse({node:await ctx.variable("DOMAIN_MARKER"), result,
          followup_writes:Number(followup.count?.value), final_followup_writes:Number(finalFollowup.count?.value)});
      } finally {
        cancelling.delete(name);
      }
    }
    if (path.endsWith("/constructors")) return jsonResponse({constructed});
    if (path.endsWith("/probe")) return jsonResponse(liveSnapshot());
    if (path.endsWith("/probe-release")) return jsonResponse(liveControl("release"));
    if (path.endsWith("/probe-clear")) return jsonResponse(liveControl("clear"));
    if (path.endsWith("/deadline-control")) return deadlineControl(params.action, params.child);
    if (path.endsWith("/deadline")) return deadlineCall(ctx, binding, params.name, params.timeout);
    if (path.endsWith("/codec-budget")) {
      const result = await codecBudget(ctx, binding, params.phase, params.id, params.remote === "true");
      const probe = Client.bind(ctx, binding, "codec-budget-" + params.phase);
      const current = await probe.getCount({});
      let replay: string | null = null;
      if (params.phase === "decode") {
        const saved = await probe.increment({delta:1n, operationId:params.id});
        if (saved.result.case !== "count") throw Error("Missing codec receipt");
        replay = saved.result.value.value.toString();
      }
      return jsonResponse({...result, count:current.count!.value.toString(), replay});
    }
    const client = Client.bind(ctx, binding, "alice");
    if (path.endsWith("/capture")) { retained = client; return jsonResponse({captured:true}); }
    try {
      if (path.endsWith("/expired")) {
        if (!retained) throw Error("No retained client");
        await retained.getCount({});
        throw Error("Expired client remained usable");
      }
      if (path.endsWith("/read")) {
        const response = await client.getCount({});
        if (!response.count) throw Error("Missing count");
        return jsonResponse({value:response.count.value.toString()});
      }
      const response = await client.increment({delta:params.delta === undefined ? undefined : BigInt(params.delta), operationId:params.id ?? ""});
      switch (response.result.case) {
        case "count": return jsonResponse({value:response.result.value.value.toString()});
        case "outOfRange": return jsonResponse({error:"out_of_range"});
        case "operationConflict": return jsonResponse({error:"operation_conflict"});
        default: throw Error("Missing result");
      }
    } catch (error) {
      return jsonResponse({
        code:error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : "Internal",
        outcome:error instanceof Error && "outcome" in error && typeof error.outcome === "string" ? error.outcome : "unknown",
      });
    }
  };
}
