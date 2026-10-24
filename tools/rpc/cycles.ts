// Qualification-only dependency cycles over the generated business methods.
import type { Context, Json } from "@cellar/sdk";
import type { CounterClient } from "./fixtures/generated/typescript/counter_cellar.js";

type Factory = {bind(ctx: Context, binding: string, name: string): Pick<CounterClient, "increment" | "getCount">};
type Probe = {role: string; waiting: boolean; late_done: boolean; storage_refused: boolean;
  read_refused: boolean; call_refused: boolean; release: () => void};
let clients: Factory;
const probes = new Map<string, Probe>();

export function configureCycles(factory: Factory) { clients = factory; }

export function cycleControl(action: string): Json {
  if (action === "release") for (const probe of probes.values()) probe.release();
  else if (action === "clear") {
    if ([...probes.values()].some(probe => !probe.late_done)) throw Error("Cycle work has not settled");
    probes.clear();
  } else if (action !== "status") throw Error("Unknown cycle control");
  return [...probes.values()].map(({release: _release, ...view}) => view);
}

async function expired(work: () => Promise<unknown>): Promise<boolean> {
  try { await work(); }
  catch (error) {
    return error instanceof Error && "code" in error && error.code === "ContextExpired"
      && "outcome" in error && error.outcome === "not_committed";
  }
  return false;
}

/** Each entered frame has already saved and synced its atomic receipt. */
export async function cycleAfterCommit(ctx: Context, id: string): Promise<void> {
  if (!id.startsWith("t15|")) return;
  const [prefix, a, b, shape, role] = id.split("|");
  if (prefix !== "t15" || !["self", "aba"].includes(shape) || !["outer", "middle", "inner"].includes(role)
    || probes.has(role) || probes.size >= 3) throw Error("Invalid or duplicate cycle frame");
  const ownBinding = role === "middle" ? b : a;
  const ownName = role === "middle" ? "cycle-b" : "cycle-a";
  const cached = clients.bind(ctx, ownBinding, ownName);
  let release!: () => void;
  const released = new Promise<void>(resolve => { release = resolve; });
  const probe: Probe = {role, waiting:true, late_done:false, storage_refused:false,
    read_refused:false, call_refused:false, release};
  probes.set(role, probe);
  try {
    if (role === "inner") await clients.bind(ctx, "RUST", "cycle-gate").getCount({}, {timeoutMs:60000});
    else {
      const next = role === "outer" && shape === "aba" ? "middle" : "inner";
      await clients.bind(ctx, next === "middle" ? b : a, next === "middle" ? "cycle-b" : "cycle-a")
        .increment({delta:1n, operationId:[prefix, a, b, shape, next].join("|")}, {timeoutMs:60000});
    }
  } catch (error) {
    probe.waiting = false;
    // A child's error can precede its parent's retirement. The supervisor
    // releases this original continuation only after every native frame is
    // independently observed revoked, while the source invocation stays live.
    await released;
    probe.storage_refused = await expired(() => ctx.storage.apply({puts:{count:"999"}}));
    probe.read_refused = await expired(() => ctx.storage.read("count"));
    probe.call_refused = await expired(() => cached.increment({delta:100n, operationId:"cycle-late"}));
    probe.late_done = true;
    throw error;
  }
  throw Error("Held cycle unexpectedly completed");
}
