// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject, RpcTarget } from "cloudflare:workers";
import {
  activateDurableObject,
  durableObjectNamespace,
} from "../../../../packages/cellar-cloudflare/index.js";
import { check, refuses } from "./adapter.mjs";

class Base extends DurableObject {
  echo(value) {
    return value;
  }
  identity() {
    return this.ctx.id.toString();
  }
  fail() {
    const cause = new Error("inner");
    const error = new TypeError("remote failure", { cause });
    error.code = "BUSINESS_REFUSAL";
    error.details = { count: 9n, bytes: new Uint8Array([7, 8]) };
    throw error;
  }
  throwUndefined() {
    throw undefined;
  }
  target() {
    return new RpcTarget();
  }
  async lostResult() {
    await this.ctx.storage.put("lost-result-effect", true);
    return new RpcTarget();
  }
  lostResultEffect() {
    return this.ctx.storage.get("lost-result-effect");
  }
  async writeAndThrow() {
    await this.ctx.storage.put("effect", true);
    throw new Error("after write");
  }
  effect() {
    return this.ctx.storage.get("effect");
  }
}
class Probe extends Base {
  secret = "private";
  arrow = () => "private";
  get hidden() {
    throw new Error("getter was executed");
  }
  async tick() {
    const count = ((await this.ctx.storage.get("count")) ?? 0) + 1;
    await this.ctx.storage.put("count", count);
    return count;
  }
}
class Counter {
  async cell_rpc({ method, args }, ctx) {
    this.activation ??= activateDurableObject(Probe, ctx);
    return this.activation.rpc(method, args);
  }
}
let saved;
export const cells = { Counter, Other: Counter };
export async function fetch(req, ctx) {
  try {
    const [name, command] = req.url.split("/").slice(3);
    const namespace = durableObjectNamespace(ctx, "COUNTERS");
    const id = namespace.idFromName(name);
    const stub = namespace.get(id);
    if (saved) await refuses(() => saved.echo("forbidden"), "ContextMismatch");
    saved = stub;
    let result;
    if (command === "tick") result = { count: await stub.tick() };
    else {
      check(
        (await stub.identity()) === id.toString(),
        "native RPC cell identity",
      );
      const restored = namespace.idFromString(id.toString());
      check(
        id.equals(restored) && restored.name === undefined && id.name === name,
        "namespace identity",
      );
      check(
        (await namespace.get(restored).identity()) === id.toString(),
        "routing from ID",
      );
      check(
        (await namespace.getByName(name).identity()) === id.toString(),
        "routing from name",
      );
      check(
        (await stub.native_fixture_json()).fromNative === true,
        "native JSON RPC interoperability",
      );
      const lone = namespace.idFromName("lone-\ud800");
      check(
        lone.name === "lone-\ud800" &&
          lone.equals(namespace.idFromName("lone-\ud800")),
        "UTF-16 ID names",
      );
      check(
        !namespace.newUniqueId().equals(namespace.newUniqueId()),
        "unique IDs",
      );
      await refuses(
        () => namespace.idFromString("0".repeat(64)),
        "ValidationError",
      );
      await refuses(() => namespace.get({ toString: () => id.toString() }));
      await refuses(
        () =>
          namespace.get(durableObjectNamespace(ctx, "OTHERS").idFromName(name)),
        "ValidationError",
      );
      await refuses(() => namespace.newUniqueId({ jurisdiction: "eu" }));
      await refuses(
        () => durableObjectNamespace(ctx, "MISSING").idFromName("x"),
        "ValidationError",
      );
      const graph = {
        bigint: 2n ** 90n,
        empty: undefined,
        lone: "\ud800",
        bytes: new Uint8Array([3, 4]),
        map: new Map(),
        sparse: [],
      };
      graph.self = graph;
      graph.map.set(graph, graph.bytes);
      graph.sparse[3] = graph;
      const copy = await stub.echo(graph);
      check(
        copy.self === copy &&
          copy.bigint === graph.bigint &&
          copy.empty === undefined &&
          copy.lone === graph.lone &&
          copy.bytes[1] === 4 &&
          copy.map.get(copy) === copy.bytes &&
          !(0 in copy.sparse) &&
          copy.sparse[3] === copy,
        "RPC structured graph",
      );
      check((await stub.echo(undefined)) === undefined, "undefined result");
      const parallel = await Promise.all([
        stub.echo("first"),
        stub.echo("second"),
      ]);
      check(
        parallel[0] === "first" && parallel[1] === "second",
        "independently routed RPC calls may overlap",
      );
      const error = await refuses(() => stub.fail());
      check(
        error.name === "TypeError" &&
          error.message === "remote failure" &&
          error.code === "BUSINESS_REFUSAL" &&
          error.cause.message === "inner" &&
          error.details.count === 9n &&
          error.details.bytes[1] === 8,
        "remote error data",
      );
      check(
        error.stack === undefined &&
          error.cause.stack === undefined &&
          error.outcome === undefined,
        "remote error stack/outcome",
      );
      let threw = false;
      try {
        await stub.throwUndefined();
      } catch (value) {
        check(value === undefined, "undefined throw");
        threw = true;
      }
      check(threw, "remote undefined throw was swallowed");
      for (const method of [
        "constructor",
        "dup",
        "alarm",
        "webSocketMessage",
        "toString",
        "hasOwnProperty",
        "secret",
        "arrow",
        "ctx",
        "env",
        "hidden",
        "__proto__",
      ]) {
        const error = await refuses(() => stub[method]());
        check(
          error.name === "TypeError" && error.message !== "getter was executed",
          `RPC visibility: ${method}`,
        );
      }
      check(
        (await refuses(() => stub.target())).outcome === "unknown",
        "lost result must remain unknown",
      );
      check(
        (await refuses(() => stub.lostResult())).outcome === "unknown",
        "post-write lost result must remain unknown",
      );
      check(
        (await stub.lostResultEffect()) === true,
        "lost result incorrectly rolled back a completed write",
      );
      await refuses(() => stub.echo(new RpcTarget()));
      await refuses(() => stub.writeAndThrow());
      check(
        (await stub.effect()) === true,
        "remote error incorrectly rolled back a write",
      );
      await refuses(
        () =>
          ctx._call("workers.rpc", {
            binding: "COUNTERS",
            id: id.toString(),
            name: null,
            method: "tick",
            args: ctx._serialize({}),
          }),
        "ValidationError",
      );
      await refuses(
        () =>
          ctx._call("workers.rpc", {
            binding: "COUNTERS",
            id: id.toString(),
            name: [120],
            method: "tick",
            args: ctx._serialize([]),
          }),
        "ValidationError",
      );
      result = { ok: true };
    }
    return {
      status: 200,
      headers: [],
      body: ctx._encode(JSON.stringify(result)),
    };
  } catch (error) {
    return {
      status: 500,
      headers: [],
      body: ctx._encode(
        JSON.stringify({
          message: error?.message,
          code: error?.code,
          stack: error?.stack,
        }),
      ),
    };
  }
}
