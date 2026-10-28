// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DirectoryBackupGateway, S3Gateway } from "@cloudflare/sandbox";
import { Serializer } from "node:v8";
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { workersApplication } from "../../../packages/cellar-cloudflare/index.js";

let calls = 0,
  unrepresentable = 0,
  nativeContext;
let background, finishBackground, backgroundMarker;
let valueCalls = 0;
class Parent extends WorkerEntrypoint {
  greet(name) {
    calls++;
    return `${this.ctx.props?.prefix ?? "hello"}:${this.env.DOMAIN_MARKER}:${name}`;
  }
}
class Greeter extends Parent {
  count = 0;
  ownSecret = "private";
  increment() {
    return ++this.count;
  }
  get settings() {
    return { prefix: this.ctx.props?.prefix, bytes: Uint8Array.of(0, 255) };
  }
  get tools() {
    return {
      base: 4,
      sum(value) {
        return this.base + value;
      },
    };
  }
  echo(value) {
    valueCalls++;
    return { value, props: this.ctx.props };
  }
  fail() {
    const error = new TypeError("gateway failure");
    error.code = "VisibleCode";
    throw error;
  }
  unrepresentable() {
    unrepresentable++;
    return () => {};
  }
  webValue(kind) {
    unrepresentable++;
    if (kind === "response") return new Response("not clone data");
    if (kind === "stream") return new ReadableStream();
    if (kind === "throw")
      throw new Error("nested", { cause: new Response("not clone data") });
    return new Error("nested", { cause: new Response("not clone data") });
  }
  errorValue() {
    const error = new TypeError("cloned error");
    error.cause = { error };
    return error;
  }
  background() {
    const gate = Promise.withResolvers();
    finishBackground = gate.resolve;
    background = gate.promise.then(() => {
      backgroundMarker = this.env.DOMAIN_MARKER;
    });
    this.ctx.waitUntil(background);
    return "retained";
  }
  fetch(request) {
    return new Response(request.body, {
      headers: {
        "x-prefix": this.ctx.props?.prefix ?? "none",
        "x-input-length": request.headers.get("content-length") ?? "unknown",
      },
    });
  }
}
class Counter extends DurableObject {
  async fetch() {
    const result = await this.ctx.exports
      .Greeter({ props: { prefix: "cell" } })
      .greet("object");
    const id = this.ctx.exports.Counter.idFromName("other");
    return Response.json({ result, named: id.name });
  }
}
let saved;
const app = workersApplication(
  {
    Greeter,
    Counter,
    DirectoryBackupGateway,
    S3Gateway,
    Plain: {
      fetch(_request, _env, ctx) {
        return Response.json({ plain: ctx.props?.plain });
      },
    },
    default: class extends WorkerEntrypoint {
      label() {
        return this.ctx.props?.label;
      }
      async fetch(request) {
        const env = this.env,
          ctx = this.ctx,
          native = nativeContext;
        const route = new URL(request.url).pathname;
        if (route === "/fixed-response" || route === "/fixed-request") {
          const { readable, writable } = new FixedLengthStream(3n);
          ctx.waitUntil(
            (async () => {
              const writer = writable.getWriter();
              try {
                await writer.write(Uint8Array.of(0));
                await writer.write(Uint8Array.of(255, 42));
                await writer.close();
              } finally {
                writer.releaseLock();
              }
            })(),
          );
          if (route === "/fixed-response") return new Response(readable);
          return ctx.exports.Greeter.fetch(
            new Request("http://gateway/", {
              method: "POST",
              body: readable,
              duplex: "half",
            }),
          );
        }
        if (route === "/loopback") {
          const props = { prefix: "captured" };
          const stub = ctx.exports.Greeter({ props });
          props.prefix = "changed";
          const greeting = await stub.greet("worker");
          const settings = await stub.settings;
          const sum = await stub.tools.sum(3);
          const counts = [await stub.increment(), await stub.increment()];
          let error;
          try {
            await stub.fail();
          } catch (failure) {
            error = {
              name: failure.name,
              message: failure.message,
              code: failure.code,
            };
          }
          const refused = [];
          for (const name of ["ownSecret", "ctx", "env"]) {
            try {
              await stub[name];
            } catch {
              refused.push(name);
            }
          }
          return Response.json({
            greeting,
            prefix: settings.prefix,
            bytes: [...settings.bytes],
            sum,
            counts,
            error,
            refused,
          });
        }
        if (route === "/service") {
          let refused = false;
          try {
            env.SELF({ props: { prefix: "forged" } });
          } catch {
            refused = true;
          }
          return Response.json({
            greeting: await env.SELF.greet("service"),
            refused,
          });
        }
        if (route === "/default")
          return Response.json({
            label: await ctx.exports
              .default({ props: { label: "class-default" } })
              .label(),
            rootProps: ctx.props ?? null,
          });
        if (route === "/plain")
          return ctx.exports.Plain({ props: { plain: true } }).fetch(request);
        if (route === "/background-start")
          return new Response(await ctx.exports.Greeter.background());
        if (route === "/background-finish") {
          finishBackground();
          await background;
          return Response.json({ marker: backgroundMarker });
        }
        if (route === "/invalid") {
          const before = calls;
          // Exercise native admission even when an untrusted guest bypasses
          // the facade's earlier clone-size check, using the same wire codec.
          const oversized = new Serializer();
          oversized.writeHeader();
          oversized.writeValue(new Uint8Array(8192));
          const operation = {
            kind: "call",
            path: ["greet"],
            args: native._serialize(["invalid"]),
          };
          let refused = 0;
          const errors = [];
          for (const input of [
            {
              service: { kind: "export", name: "Greeter", script: "foreign" },
              operation,
            },
            {
              service: {
                kind: "binding",
                binding: "SELF",
                props: native._serialize({ prefix: "forged" }),
              },
              operation,
            },
            {
              service: { kind: "export", name: "Greeter", props: "invalid" },
              operation,
            },
            {
              service: {
                kind: "export",
                name: "Greeter",
                props: oversized.releaseBuffer().toString("base64"),
              },
              operation,
            },
            {
              service: { kind: "export", name: "Greeter" },
              operation: { ...operation, args: native._serialize({}) },
            },
            {
              service: { kind: "export", name: "Greeter" },
              operation: { ...operation, path: [] },
            },
            {
              service: { kind: "export", name: "Greeter" },
              operation: { ...operation, path: Array(65).fill("greet") },
            },
          ]) {
            try {
              await native._call("workers.rpc", input);
            } catch (error) {
              errors.push({ code: error.code, outcome: error.outcome });
              if (
                error.code === "ValidationError" &&
                error.outcome === "not_committed"
              )
                refused++;
            }
          }
          return Response.json({ refused, effects: calls - before, errors });
        }
        if (route === "/unrepresentable") {
          const before = unrepresentable;
          let outcome;
          try {
            await ctx.exports.Greeter.unrepresentable();
          } catch (error) {
            outcome = error.outcome;
          }
          return Response.json({ effects: unrepresentable - before, outcome });
        }
        if (route === "/unsupported-values") {
          const stub = ctx.exports.Greeter;
          const before = valueCalls;
          let argumentsRefused = 0,
            propsRefused = 0;
          for (const value of [
            new Request("http://request/"),
            new Response("body"),
            new ReadableStream(),
            new Error("nested", { cause: new Response("body") }),
            () => {},
          ]) {
            try {
              await stub.echo(value);
            } catch {
              argumentsRefused++;
            }
            try {
              stub({ props: value });
            } catch {
              propsRefused++;
            }
          }
          const outcomes = [];
          const effects = unrepresentable;
          for (const kind of ["response", "stream", "cause", "throw"]) {
            try {
              await stub.webValue(kind);
            } catch (error) {
              outcomes.push(error.outcome);
            }
          }
          let error, errorFailure;
          try {
            error = await stub.errorValue();
          } catch (failure) {
            errorFailure = {
              code: failure.code,
              message: failure.message,
              outcome: failure.outcome,
            };
          }
          return Response.json({
            argumentsRefused,
            propsRefused,
            inputEffects: valueCalls - before,
            outputEffects: unrepresentable - effects,
            outcomes,
            error: errorFailure ?? {
              name: error.name,
              message: error.message,
              cycle: error.cause.error === error,
            },
          });
        }
        if (route === "/fetch")
          return ctx.exports
            .Greeter({ props: { prefix: "fetch" } })
            .fetch(request);
        if (route === "/save") {
          saved = ctx.exports.Greeter({ props: { prefix: "old" } });
          return new Response("saved");
        }
        if (route === "/borrow") {
          try {
            await saved.greet("foreign");
            return new Response("borrowed");
          } catch (error) {
            return Response.json({ code: error.code, outcome: error.outcome });
          }
        }
        if (route === "/object")
          return env.COUNTERS.getByName("test").fetch(request);
        if (route === "/gateways") {
          const backup = ctx.exports.DirectoryBackupGateway({
            props: { protocolVersion: 1, mode: "deny" },
          });
          const deniedBackup = await backup.fetch("http://backup/parts/1", {
            method: "PUT",
            body: Uint8Array.of(0, 255),
          });
          let control;
          try {
            await backup.deleteObject();
          } catch (error) {
            control = error.message;
          }
          const mount = ctx.exports.S3Gateway({
            props: { protocolVersion: 1, mode: "deny", routeId: "revoked" },
          });
          const deniedMount = await mount.fetch("http://mount/object");
          return Response.json({
            backup: deniedBackup.status,
            backupText: await deniedBackup.text(),
            mount: deniedMount.status,
            control,
          });
        }
        return new Response("missing", { status: 404 });
      }
    },
  },
  {
    namespaces: { COUNTERS: "Counter" },
    variables: ["DOMAIN_MARKER"],
    services: ["SELF"],
  },
);
export const { cells, worker_rpc } = app;
export function fetch(frame, context) {
  nativeContext = context;
  return app.fetch(frame, context);
}
