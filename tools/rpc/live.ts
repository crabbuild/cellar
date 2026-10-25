// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Qualification-only continuation retained across cancellation or owner loss.
import type { Context, Json } from "@cellar/sdk";
type LiveClient = {increment(request: {delta:bigint;operationId:string}):Promise<unknown>};
let bindClient: ((context:Context) => LiveClient) | undefined;

export function configureLiveClient(bind:(context:Context) => LiveClient): void {bindClient = bind;}

type Probe = {
  ready: boolean; id: string; node: string; class: string; scope: string; epoch: string;
  count: Json; receipt: Json; active_before: boolean; committed: boolean; waiting: boolean;
  cancelled: boolean; late_done: boolean; storage_refused: boolean; read_refused: boolean;
  call_refused: boolean; release: () => void;
};
let probe: Probe | undefined;

async function expired(operation: () => Promise<unknown>): Promise<boolean> {
  try { await operation(); return false; }
  catch (error) {
    return error instanceof Error && "code" in error && error.code === "ContextExpired" &&
      "outcome" in error && error.outcome === "not_committed";
  }
}

export function liveSnapshot(): Json {
  if (!probe) return {ready:false};
  const {release: _release,...snapshot} = probe;
  return snapshot;
}

export function liveControl(action: "release" | "clear"): Json {
  if (!probe) throw Error("No live fence probe");
  if (action === "release") {
    if (!probe.cancelled) throw Error("Live handler was not cancelled by its fence");
    probe.release();
    return {released:true};
  }
  if (!probe.late_done) throw Error("Live probe cleanup preceded late SDK checks");
  probe = undefined;
  return {cleared:true};
}

export async function liveSync(ctx: Context, id: string): Promise<boolean> {
  if (!id.startsWith("proof-fence-sync-") && !id.startsWith("proof-cancel-sync-")) return false;
  const node = await ctx.variable("DOMAIN_MARKER");
  if (node !== "proof-owner") return false; // Explicit retries use new authority.
  if (probe || !ctx.identity) throw Error("Live probe already exists or has no cell identity");
  const count = await ctx.storage.read("count");
  const receipt = await ctx.storage.read("receipt:" + id);
  if (!count.present || !receipt.present) throw Error("Live probe preceded its atomic batch");
  if (!bindClient) throw Error("Live client factory was not configured");
  const client = bindClient(ctx);
  let release!: () => void;
  const released = new Promise<void>(resolve => { release = resolve; });
  const current: Probe = {ready:true,id,node,class:ctx.identity.class,scope:ctx.identity.scope,
    epoch:ctx.identity.epoch,count:count.value,receipt:receipt.value,active_before:true,
    committed:true,waiting:true,cancelled:false,late_done:false,storage_refused:false,
    read_refused:false,call_refused:false,release};
  probe = current;
  try { await ctx.storage.sync(); }
  catch {
    current.cancelled = true;
    current.waiting = false;
    await released;
    current.storage_refused = await expired(() => ctx.storage.apply({puts:{"late-fenced":true}}));
    current.read_refused = await expired(() => ctx.storage.read("count"));
    current.call_refused = await expired(() => client.increment({delta:1n,operationId:"fenced-late"}));
    current.late_done = true;
    throw Error("Retired live handler completed its late SDK refusal audit");
  }
  throw Error("Sync completed while its real proof paths were withheld");
}
