// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

export class Counter extends DurableObject {
  outputs = new Map();

  duplex() {
    const pair = new IdentityTransformStream();
    return {
      readable: pair.readable,
      writable: pair.writable,
      alias: pair.writable,
    };
  }

  openSink(token, mode) {
    const gates = {
      write: Promise.withResolvers(),
      close: Promise.withResolvers(),
      background: Promise.withResolvers(),
    };
    const storage = this.ctx.storage;
    let chunks = 0,
      bytes = 0;
    const body = new WritableStream({
      async write(chunk) {
        if (mode === "gated" && chunks === 0) {
          storage.kv.put(`${token}-write-started`, true);
          await gates.write.promise;
        }
        if (mode === "error" && chunks === 1)
          throw new Error("deliberate sink failure after an accepted prefix");
        storage.kv.put(`${token}-chunk-${chunks++}`, chunk);
        bytes += chunk.byteLength;
        storage.kv.put(`${token}-chunks`, chunks);
        storage.kv.put(`${token}-bytes`, bytes);
      },
      async close() {
        storage.kv.put(`${token}-close-started`, true);
        if (mode === "gated") await gates.close.promise;
        storage.kv.put(`${token}-closed`, true);
      },
      abort(reason) {
        storage.kv.put(`${token}-aborted`, true);
        storage.kv.put(
          `${token}-reason`,
          typeof reason === "string" ? reason : null,
        );
      },
    });
    this.outputs.set(token, { body, gates });
    if (mode === "gated") {
      this.ctx.waitUntil(
        gates.background.promise.then(() => {
          storage.kv.put(`${token}-background`, true);
        }),
      );
    }
    return body;
  }

  release(token, gate) {
    this.outputs.get(token).gates[gate].resolve();
  }

  state(token) {
    const storage = this.ctx.storage.kv;
    let offset = 0,
      valid = true;
    for (let i = 0; i < (storage.get(`${token}-chunks`) ?? 0); i++) {
      for (const byte of storage.get(`${token}-chunk-${i}`)) {
        if (byte !== offset % 256) valid = false;
        offset++;
      }
    }
    return {
      valid,
      bytes: offset,
      locked: this.outputs.get(token)?.body.locked ?? false,
      writing: storage.get(`${token}-write-started`) ?? false,
      closing: storage.get(`${token}-close-started`) ?? false,
      closed: storage.get(`${token}-closed`) ?? false,
      aborted: storage.get(`${token}-aborted`) ?? false,
      reason: storage.get(`${token}-reason`),
      background: storage.get(`${token}-background`) ?? false,
    };
  }
}

let stale;
const app = workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        const url = new URL(request.url);
        const token = url.searchParams.get("token");
        const stub = env.COUNTERS.getByName("writer-owner");
        if (url.pathname === "/state")
          return Response.json(await stub.state(token));
        if (url.pathname === "/release") {
          await stub.release(token, url.searchParams.get("gate"));
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/duplex") {
          const pair = await stub.duplex();
          if (pair.writable !== pair.alias)
            throw new Error("writable alias changed");
          try {
            const bytes = new Uint8Array(4096).map((_, i) => i % 256);
            const [output] = await Promise.all([
              new Response(pair.readable).arrayBuffer(),
              new Blob([bytes]).stream().pipeTo(pair.writable),
            ]);
            const read = new Uint8Array(output);
            return Response.json({
              bytes: read.byteLength,
              valid: read.every((byte, i) => byte === i % 256),
            });
          } finally {
            pair[Symbol.dispose]();
          }
        }
        if (url.pathname === "/stale") {
          try {
            await stale.getWriter().write(Uint8Array.of(1));
          } catch (error) {
            return Response.json({ code: error.code });
          }
          throw new Error("stale writer acquired a later event");
        }
        const mode = url.pathname.slice(1);
        const stream = await stub.openSink(token, mode);
        if (mode === "abandon") {
          stale = stream;
          return new Response(null, { status: 204 });
        }
        const writer = stream.getWriter();
        if (mode === "abort" || mode === "bad-reason") {
          let refused = false;
          try {
            await writer.abort(mode === "abort" ? "caller left" : () => {});
          } catch {
            refused = true;
          }
          if (refused !== (mode === "bad-reason"))
            throw new Error("abort reason contract");
        } else if (mode === "error") {
          let failed = false;
          try {
            await writer.write(new Uint8Array(2048).map((_, i) => i % 256));
          } catch {
            failed = true;
          }
          if (!failed) throw new Error("sink failure became success");
        } else {
          // pipeTo exercises native backpressure across the returned writer.
          writer.releaseLock();
          let sent = false;
          await new ReadableStream({
            type: "bytes",
            pull(controller) {
              if (sent) {
                const pending = controller.byobRequest;
                controller.close();
                pending?.respond(0);
              } else {
                sent = true;
                controller.enqueue(new Uint8Array(4096).map((_, i) => i % 256));
              }
            },
          }).pipeTo(stream);
        }
        return Response.json(await stub.state(token));
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" } },
);
export const { cells, fetch } = app;
