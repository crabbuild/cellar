// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

export class Counter extends DurableObject {
  async consume(token, stream, alias) {
    if (stream !== alias) throw new Error("input alias changed");
    const reader = stream.getReader({ mode: "byob" });
    let total = 0,
      valid = true;
    try {
      for (;;) {
        const chunk = await reader.read(new Uint8Array(113));
        if (chunk.done) break;
        for (const byte of chunk.value)
          if (byte !== total++ % 256) valid = false;
        this.ctx.storage.kv.put(token, total);
      }
      return { total, valid };
    } catch (error) {
      this.ctx.storage.kv.put("input-error-stack", error.stack ?? null);
      throw error;
    } finally {
      reader.releaseLock();
    }
  }
  async produce(token, stream) {
    const writer = stream.getWriter();
    this.ctx.storage.kv.put(token, "writing");
    await writer.write(new Uint8Array(4096).map((_, i) => i % 256));
    this.ctx.storage.kv.put(token, "closing");
    await writer.close();
    this.ctx.storage.kv.put(token, "closed");
    return true;
  }
  echo(stream) {
    return stream;
  }
  receiver() {
    return (stream) => this.consume("delegate", stream, stream);
  }
  defer(stream) {
    return async () => {
      const bytes = new Uint8Array(await new Response(stream).arrayBuffer());
      return {
        total: bytes.length,
        valid: bytes.every((byte, i) => byte === i % 256),
      };
    };
  }
  forward(token, stream) {
    return this.env.COUNTERS.getByName("input-peer").consume(
      token,
      stream,
      stream,
    );
  }
  unused() {
    const count = this.ctx.storage.kv.get("unused-calls") ?? 0;
    this.ctx.storage.kv.put("unused-calls", count + 1);
    return true;
  }
  async cancel(stream) {
    const reader = stream.getReader();
    const pending = reader.read();
    await reader.cancel("remote cancelled");
    return (await pending).done;
  }
  state(token) {
    return this.ctx.storage.kv.get(token);
  }
}

const sources = new Map();
const app = workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        try {
          const url = new URL(request.url),
            token = url.searchParams.get("token");
          const stub = env.COUNTERS.getByName("input-owner");
          if (url.pathname === "/state")
            return Response.json({
              remote: await stub.state(token),
              local: sources.get(token)?.state,
            });
          if (url.pathname === "/release") {
            sources.get(token).gate.resolve();
            return new Response(null, { status: 204 });
          }
          const gate = Promise.withResolvers(),
            state = { cancelled: false, bytes: 0, closed: false };
          sources.set(token, { gate, state });
          if (url.pathname === "/write") {
            const stream = new WritableStream({
              async write(chunk) {
                if (state.bytes === 0) await gate.promise;
                for (const byte of chunk)
                  if (byte !== state.bytes++ % 256)
                    throw new Error("corrupted input write");
              },
              close() {
                state.closed = true;
              },
              abort(reason) {
                state.cancelled = reason ?? true;
              },
            });
            await stub.produce(token, stream);
            return Response.json({ ...state, locked: stream.locked });
          }
          let sent = false;
          const stream = new ReadableStream({
            type: "bytes",
            async pull(controller) {
              if (url.pathname === "/cancel") return;
              if (sent) {
                if (url.pathname === "/source-error")
                  throw new Error("deliberate upload failure");
                const pending = controller.byobRequest;
                controller.close();
                pending?.respond(0);
                return;
              }
              sent = true;
              controller.enqueue(new Uint8Array(4096).map((_, i) => i % 256));
              // Hold a source callback while its first bytes reach the callee.
              if (url.pathname === "/read") await gate.promise;
            },
            cancel(reason) {
              state.cancelled = reason ?? true;
            },
          });
          if (url.pathname === "/unused") {
            await stub.unused(stream);
            return new Response(null, { status: 204 });
          }
          if (url.pathname === "/cancel")
            return Response.json({
              done: await stub.cancel(stream),
              reason: state.cancelled,
            });
          if (url.pathname === "/echo")
            return new Response(await stub.echo(stream));
          if (url.pathname === "/delegate") {
            const consume = await stub.receiver();
            try {
              return Response.json(await consume(stream));
            } finally {
              consume[Symbol.dispose]();
            }
          }
          if (url.pathname === "/defer") {
            const callback = await stub.defer(stream);
            try {
              return Response.json(await callback());
            } finally {
              callback[Symbol.dispose]();
            }
          }
          if (url.pathname === "/forward")
            return Response.json(await stub.forward(token, stream));
          if (url.pathname === "/quota" || url.pathname === "/callback") {
            const streams = [stream];
            if (url.pathname === "/quota")
              for (let i = 0; i < 6; i++)
                streams.push(new ReadableStream({ type: "bytes" }));
            try {
              await stub.unused(
                streams,
                url.pathname === "/callback" ? () => {} : null,
              );
            } catch (error) {
              return Response.json({
                refused: true,
                locked: streams.some((stream) => stream.locked),
                code: error.code,
              });
            }
            throw new Error("invalid input accepted");
          }
          if (url.pathname === "/source-error") {
            try {
              await stub.consume(token, stream, stream);
            } catch (error) {
              return Response.json({
                message: error.message,
                prefix: await stub.state(token),
              });
            }
            throw new Error("source error became successful EOF");
          }
          return Response.json({
            ...(await stub.consume(token, stream, stream)),
            locked: stream.locked,
          });
        } catch (error) {
          return Response.json(
            { error: error.message, code: error.code, stack: error.stack },
            { status: 500 },
          );
        }
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" } },
);
export default app;
