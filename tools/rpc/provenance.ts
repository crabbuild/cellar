// Post-commit failures must describe the parent call, not its refused child.
import type { Context, Json } from "@cellar/sdk";
import type { CounterClient } from "./fixtures/generated/typescript/counter_cellar.js";

type Factory = {bind(ctx: Context, binding: string, name: string): Pick<CounterClient, "getCount">};
let clients: Factory;
let observation: Json | undefined;

export function configureProvenance(factory: Factory) { clients = factory; }

export function provenanceControl(clear: boolean): Json {
  if (clear) observation = undefined;
  return observation ?? {ready:false};
}

export async function provenanceAfterCommit(ctx: Context, id: string): Promise<void> {
  if (!id.startsWith("t06:")) return;
  if (observation) throw Error("Previous provenance observation was not cleared");
  const mode = id.split(":")[1];
  let failure: unknown;
  if (mode === "child") {
    try {
      await clients.bind(ctx, "RUST", "t06-unconstructed").getCount({}, {timeoutMs:0});
    } catch (error) { failure = error; }
    if (!(failure instanceof Error) || !("code" in failure) || failure.code !== "DeadlineExceeded"
      || !("outcome" in failure) || failure.outcome !== "not_committed") throw Error("Child was not refused before submission");
  } else {
    const error = new Error("T06-private-exception-sentinel");
    Object.defineProperty(error, "code", {value:mode === "forge" ? "MethodNotFound" : "T06-private-code-sentinel"});
    // The bridge may preserve a fixed category. It must not consult this
    // claimed effect outcome or arbitrary exception text, even via a getter.
    Object.defineProperty(error, "outcome", {get() { throw Error("T06-outcome-accessed"); }});
    failure = error;
  }
  const stored = await ctx.storage.read("count");
  const receipt = await ctx.storage.read("receipt:" + id);
  if (!stored.present || !receipt.present) throw Error("Parent lost its committed state or authority");
  observation = {ready:true,id,mode,parent_active:true,count:stored.value,receipt:receipt.value,
    child:mode === "child" ? {code:"DeadlineExceeded",outcome:"not_committed"} : null};
  throw failure;
}
