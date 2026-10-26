// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

const deferred = () => Promise.withResolvers();
export class Counter extends DurableObject {
  streams = new Map();

  release(token, gate) {
    this.streams.get(token)?.[gate]?.resolve();
  }

  state(token) {
    return {
      phase: this.ctx.storage.kv.get(`${token}-phase`) ?? 0,
      cancelled: this.ctx.storage.kv.get(`${token}-cancelled`) ?? 0,
      background: this.ctx.storage.kv.get(`${token}-background`) ?? false,
    };
  }

  async rpcStream(mode, token) {
    return (
      await this.fetch(new Request(`http://object/${mode}?token=${token}`))
    ).body;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const mode = url.pathname.slice(1);
    const token = url.searchParams.get("token");
    if (mode === "empty") return new Response(null, { status: 204 });
    if (mode === "large") {
      let n = 0;
      return new Response(
        new ReadableStream(
          {
            type: "bytes",
            pull(controller) {
              if (n === 6) {
                const pending = controller.byobRequest;
                controller.close();
                pending?.respond(0);
              } else controller.enqueue(new Uint8Array(128).fill(n++));
            },
          },
          { highWaterMark: 0 },
        ),
      );
    }
    const state = {
      first: deferred(),
      second: deferred(),
      background: deferred(),
    };
    this.streams.set(token, state);
    const storage = this.ctx.storage;
    if (["cancel", "error", "finish"].includes(mode)) {
      this.ctx.waitUntil(
        state.background.promise.then(() => {
          storage.kv.put(`${token}-background`, true);
        }),
      );
    }
    let n = 0;
    const body = new ReadableStream(
      {
        type: "bytes",
        async pull(controller) {
          if (mode === "oversized") {
            storage.kv.put(`${token}-phase`, 1);
            controller.enqueue(new Uint8Array(257));
            return;
          }
          if (n++ === 0) {
            if (mode === "stream") await state.first.promise;
            if (state.cancelled) return;
            storage.kv.put(`${token}-phase`, 1);
            controller.enqueue(Uint8Array.of(0, 255, 42));
            return;
          }
          await state.second.promise;
          if (state.cancelled) return;
          if (mode === "error")
            throw new Error("deliberate web producer failure");
          storage.kv.put(`${token}-phase`, 2);
          controller.enqueue(Uint8Array.of(43, 0, 255));
          controller.close();
        },
        cancel() {
          state.cancelled = true;
          state.first.resolve();
          state.second.resolve();
          storage.kv.put(
            `${token}-cancelled`,
            (storage.kv.get(`${token}-cancelled`) ?? 0) + 1,
          );
        },
      },
      { highWaterMark: 0 },
    );
    return new Response(body, {
      headers: [
        ["x-stream", "before-first-byte"],
        ["set-cookie", "a=1"],
        ["set-cookie", "b=2"],
      ],
    });
  }
}

let staleStream;
const worker = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const stub = env.COUNTERS.getByName("stream-owner");
    const token = url.searchParams.get("token");
    if (url.pathname === "/state")
      return Response.json(await stub.state(token));
    if (url.pathname === "/release") {
      await stub.release(token, url.searchParams.get("gate"));
      return new Response(null, { status: 204 });
    }
    if (url.pathname.startsWith("/rpc/")) {
      const mode = url.pathname.slice("/rpc/".length);
      if (mode === "stale") {
        try {
          await staleStream.getReader().read();
          throw new Error("stale RPC stream accepted a read");
        } catch (error) {
          return Response.json({ code: error.code, message: error.message });
        }
      }
      const body = await stub.rpcStream(
        ["abandon", "bad-reason"].includes(mode) ? "cancel" : mode,
        token,
      );
      if (mode === "bad-reason") {
        const reader = body.getReader();
        await reader.read();
        const pending = reader.read();
        let refused = false;
        try {
          await reader.cancel(() => {});
        } catch {
          refused = true;
        }
        if (!refused || !(await pending).done)
          throw new Error("untransferable reason did not join cancellation");
        return new Response(null, { status: 204 });
      }
      if (mode === "abandon") {
        staleStream = body;
        return new Response(null, { status: 204 });
      }
      return new Response(body, {
        headers: { "x-stream": "before-first-byte" },
      });
    }
    return stub.fetch(request);
  },
};
const app = workersApplication(
  { default: worker, Counter },
  {
    namespaces: { COUNTERS: "Counter" },
  },
);
export const { cells, fetch } = app;
